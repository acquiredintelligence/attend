import { DatabaseSync } from 'node:sqlite';
import { afterAll, describe, expect, test } from 'vitest';
import { createAttend, type AttendConfig } from '../src/attend.js';
import { memoryStore } from '../src/memory.js';
import { sqliteStore } from '../src/sqlite.js';
import { drain, fakeClock, recordingSink, scriptedJudge } from '../src/testing.js';
import type { Event, Source, StateStore, Trace } from '../src/types.js';

const databases: DatabaseSync[] = [];
afterAll(() => {
  for (const db of databases) db.close();
});

const stores: [string, () => StateStore][] = [
  ['memoryStore', memoryStore],
  [
    'SQLite',
    () => {
      const db = new DatabaseSync(':memory:');
      databases.push(db);
      return sqliteStore(db);
    },
  ],
];
const event = (id: string): Event => ({ id, at: 0, name: 'close', data: {} });
const entries = [
  { ...event('a'), kind: 'closing', seenAt: 1000 },
  { ...event('b'), kind: 'closing', seenAt: 1000 },
  { ...event('c'), kind: 'closing', seenAt: 1000 },
];

function setup(store: StateStore, config: Partial<AttendConfig> = {}) {
  const clock = fakeClock(1000);
  const traces: Trace[] = [];
  const attend = createAttend({
    store,
    clock,
    kind: () => 'closing',
    judge: scriptedJudge([]),
    sink: recordingSink(),
    deadLetter: () => undefined,
    settle: { retryBackoffMs: [10] },
    observe: (trace) => traces.push(trace),
    ...config,
  });
  return { attend, clock, traces };
}

function gate() {
  let release: () => void = () => undefined;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, release: () => release() };
}

for (const [name, makeStore] of stores) describe(`ingest through ${name}`, () => {
  test.each(['throwing kind', 'invalid kind', 'BigInt', 'BigInt at'] as const)('%s rejects only the middle push behind a slow write', async (failure) => {
    const inner = makeStore();
    const held = gate();
    let puts = 0;
    const store: StateStore = { ...inner, async put(record, version) {
      puts += 1;
      if (puts === 1) await held.promise;
      return inner.put(record, version);
    } };
    const error = new Error('bad kind');
    const classified: string[] = [];
    const { attend } = setup(store, {
      // @ts-expect-error Exercise an invalid classifier from a JavaScript caller.
      kind: (value) => {
        classified.push(value.id);
        if (value.id === 'b' && failure === 'throwing kind') throw error;
        if (value.id === 'b' && failure === 'invalid kind') return 'invalid';
        return 'closing';
      },
    });
    const writing = attend.push('s', []);
    await drain();
    const bad = failure === 'BigInt' ? { ...event('b'), data: { value: 1n } } : failure === 'BigInt at' ? { ...event('b'), at: 1n } : event('b');
    const first = attend.push('s', [event('a')]);
    // @ts-expect-error Exercise non-JSON data from a JavaScript caller.
    const middle = attend.push('s', [bad]);
    const last = attend.push('s', [event('c')]);
    const results = Promise.allSettled([first, middle, last]);
    expect(classified).toEqual(failure.startsWith('BigInt') ? ['a', 'c'] : ['a', 'b', 'c']);
    held.release();
    await writing;
    const settled = await results;
    expect(settled[0]).toEqual({ status: 'fulfilled', value: undefined });
    expect(settled[2]).toEqual({ status: 'fulfilled', value: undefined });
    if (failure === 'throwing kind') expect(settled[1]).toEqual({ status: 'rejected', reason: error });
    else expect(settled[1]).toEqual({ status: 'rejected', reason: expect.any(TypeError) });
    expect((await attend.inspect('s'))?.buffer.entries).toEqual([entries[0], entries[2]]);
    expect(puts).toBe(2);
  });

  test.each(['kind', 'put', 'committed put'] as const)('a pipelined source resumes without loss or duplicates after a failed %s', async (failure) => {
    const inner = makeStore();
    const held = gate();
    const error = new Error(`failed ${failure}`);
    let failed = false;
    let puts = 0;
    const stored: { cursor: string | null | undefined; ids: string[] }[] = [];
    const reads: { cursor: string | null | undefined; ids: string[] }[] = [];
    const store: StateStore = { ...inner, async get(scope) {
      const record = await inner.get(scope);
      reads.push({ cursor: record?.cursors.src, ids: record?.buffer.entries.map((value) => value.id) ?? [] });
      return record;
    }, async put(record, version) {
      puts += 1;
      if (puts === 1) await held.promise;
      const reject = record.buffer.entries.length > 0 && failure !== 'kind' && !failed;
      if (reject) failed = true;
      if (reject && failure === 'put') throw error;
      const result = await inner.put(record, version);
      stored.push({ cursor: record.cursors.src, ids: record.buffer.entries.map((value) => value.id) });
      if (reject) throw error;
      return result;
    } };
    const { attend, clock, traces } = setup(store, { kind: (value) => {
      if (failure === 'kind' && value.id === 'b' && !failed) { failed = true; throw error; }
      return 'closing';
    } });
    const writing = attend.push('s', []);
    await drain();
    const cursors: (string | null)[] = [];
    const signals: AbortSignal[] = [];
    const source: Source = { key: 'src', async subscribe(cursor, emit, signal) {
      cursors.push(cursor);
      signals.push(signal);
      const batches = ['a', 'b', 'c'].slice(Number(cursor ?? 0));
      await Promise.allSettled(batches.map((id) => emit({ events: [event(id)], cursor: String(['a', 'b', 'c'].indexOf(id) + 1) })));
    } };
    const attached = attend.attach('s', source);
    await drain();
    held.release();
    await writing;
    await drain();
    expect(signals[0]?.aborted).toBe(true);
    expect(traces.filter((trace) => trace.kind === 'retrying')).toEqual([
      { kind: 'retrying', scope: 's', stage: 'source', id: 'src', attempt: 1, at: 1010, message: `failed ${failure}` },
    ]);
    await clock.advance(10);
    await attached;
    expect(cursors).toEqual(failure === 'committed put' ? [null, '3'] : [null, null]);
    expect(signals[0]).not.toBe(signals[1]);
    expect(stored).toEqual([{ cursor: undefined, ids: [] }, { cursor: '3', ids: ['a', 'b', 'c'] }]);
    expect(reads.filter((read) => read.cursor === '2' || read.cursor === '3')).toEqual(
      failure === 'committed put' ? [{ cursor: '3', ids: ['a', 'b', 'c'] }] : [],
    );
    const record = await attend.inspect('s');
    expect(record?.cursors).toEqual({ src: '3' });
    expect(record?.buffer.entries).toEqual(failure === 'committed put' ? entries : [
      { ...event('a'), kind: 'closing', seenAt: 1010 },
      { ...event('b'), kind: 'closing', seenAt: 1010 },
      { ...event('c'), kind: 'closing', seenAt: 1010 },
    ]);
  });

  test('an emit queued behind a failed write rejects before writing', async () => {
    const inner = makeStore();
    const held = gate();
    const error = new Error('offline');
    let puts = 0;
    const store: StateStore = { ...inner, async put(record, version) {
      puts += 1;
      if (puts === 1) { await held.promise; throw error; }
      return inner.put(record, version);
    } };
    const { attend, clock } = setup(store);
    const results: PromiseSettledResult<void>[][] = [];
    const cursors: (string | null)[] = [];
    const attached = attend.attach('s', { key: 'src', async subscribe(cursor, emit) {
      cursors.push(cursor);
      const first = emit({ events: [event('a')], cursor: '1' });
      const second = emit({ events: [event('b')], cursor: '2' });
      await drain();
      const third = emit({ events: [event('c')], cursor: '3' });
      const pending = Promise.allSettled([first, second, third]);
      held.release();
      results.push(await pending);
    } });
    await drain();
    await drain();
    expect(results[0]).toEqual([
      { status: 'rejected', reason: error }, { status: 'rejected', reason: error }, { status: 'rejected', reason: error },
    ]);
    expect(puts).toBe(1);
    expect(await inner.get('s')).toBeNull();
    await clock.advance(10);
    await attached;
    expect(cursors).toEqual([null, null]);
    expect((await attend.inspect('s'))?.buffer.entries).toEqual([
      { ...event('a'), kind: 'closing', seenAt: 1010 },
      { ...event('b'), kind: 'closing', seenAt: 1010 },
      { ...event('c'), kind: 'closing', seenAt: 1010 },
    ]);
    expect((await attend.inspect('s'))?.cursors).toEqual({ src: '3' });
  });
});

test('30 merged pushes reject after one failed put attempt', async () => {
  const inner = memoryStore();
  const error = new Error('offline');
  let puts = 0;
  const { attend } = setup({ ...inner, async put() { puts += 1; throw error; } });
  const results = await Promise.allSettled(Array.from({ length: 30 }, (_, i) => attend.push('s', [event(String(i))])));
  expect(results).toHaveLength(30);
  for (const result of results) expect(result).toEqual({ status: 'rejected', reason: error });
  expect(puts).toBe(1);
  expect(await attend.inspect('s')).toBeNull();
});

test('a source returning before its emits settle still retries a failed write', async () => {
  const inner = memoryStore();
  const held = gate();
  let puts = 0;
  const { attend, clock } = setup({ ...inner, async put(record, version) {
    puts += 1;
    if (puts === 1) { await held.promise; throw new Error('offline'); }
    return inner.put(record, version);
  } });
  const cursors: (string | null)[] = [];
  const attached = attend.attach('s', { key: 'src', async subscribe(cursor, emit) {
    cursors.push(cursor);
    for (const [i, id] of ['a', 'b', 'c'].entries()) void emit({ events: [event(id)], cursor: String(i + 1) }).catch(() => undefined);
  } });
  await drain();
  held.release();
  await drain();
  expect(cursors).toEqual([null]);
  expect(puts).toBe(1);
  await clock.advance(10);
  await attached;
  expect(cursors).toEqual([null, null]);
  expect(puts).toBe(2);
  expect((await attend.inspect('s'))?.cursors).toEqual({ src: '3' });
  expect((await attend.inspect('s'))?.buffer.entries).toEqual([
    { ...event('a'), kind: 'closing', seenAt: 1010 },
    { ...event('b'), kind: 'closing', seenAt: 1010 },
    { ...event('c'), kind: 'closing', seenAt: 1010 },
  ]);
});

test('a source that drops its emit promises leaves no unhandled rejection and still retries', async () => {
  const unhandled: unknown[] = [];
  const onUnhandled = (reason: unknown) => unhandled.push(reason);
  process.on('unhandledRejection', onUnhandled);
  try {
    const inner = memoryStore();
    let puts = 0;
    const { attend, clock } = setup({ ...inner, async put(record, version) {
      puts += 1;
      if (puts === 1) throw new Error('offline');
      return inner.put(record, version);
    } });
    const attached = attend.attach('s', { key: 'src', async subscribe(_cursor, emit) {
      for (const [i, id] of ['a', 'b', 'c'].entries()) void emit({ events: [event(id)], cursor: String(i + 1) });
    } });
    await drain();
    await new Promise((resolve) => setImmediate(resolve));
    await clock.advance(10);
    await attached;
    await new Promise((resolve) => setImmediate(resolve));
    expect(unhandled).toEqual([]);
    expect((await attend.inspect('s'))?.cursors).toEqual({ src: '3' });
    expect((await attend.inspect('s'))?.buffer.entries.map((entry) => entry.id)).toEqual(['a', 'b', 'c']);
  } finally {
    process.off('unhandledRejection', onUnhandled);
  }
});

test('a compare-and-swap retry does not classify events again', async () => {
  const inner = memoryStore();
  let puts = 0;
  let classified = 0;
  const { attend } = setup({ ...inner, async put(record, version) {
    puts += 1;
    return puts === 1 ? 'conflict' : inner.put(record, version);
  } }, { kind: () => { classified += 1; return 'closing'; } });
  await attend.push('s', [event('a')]);
  expect(classified).toBe(1);
  expect(puts).toBe(2);
  expect((await attend.inspect('s'))?.buffer.entries).toEqual([entries[0]]);
});

test.each([
  ['Date', { when: new Date('2026-10-15T12:00:00.000Z') }, { when: '2026-10-15T12:00:00.000Z' }],
  ['invalid Date', { when: new Date(NaN) }, { when: null }],
  ['data toJSON', { toJSON: () => ({ value: 42 }) }, { value: 42 }],
  ['omitted toJSON', { value: { toJSON: () => undefined } }, {}],
  ['function and symbol', { value: () => 1, symbol: Symbol('value') }, {}],
  ['nonfinite numbers', { value: NaN, positive: Infinity, negative: -Infinity }, { value: null, positive: null, negative: null }],
  ['array undefined', { values: [undefined, () => 1, Symbol('value')] }, { values: [null, null, null] }],
])('JSON data has identical values on memoryStore and SQLite: %s', async (_name, data, expected) => {
  expect(JSON.parse(JSON.stringify(data))).toEqual(expected);
  for (const [, makeStore] of stores) {
    const { attend } = setup(makeStore());
    // @ts-expect-error Exercise non-JSON input from a JavaScript caller.
    await attend.push('s', [{ ...event('a'), data }]);
    expect((await attend.inspect('s'))?.buffer.entries[0]?.data).toEqual(expected);
  }
});

const cycle: Record<string, unknown> = {};
cycle.self = cycle;
test.each([
  ['missing data', [{ id: 'a', at: 0, name: 'close' }]],
  ['null event', [null]],
  ['sparse events', Array(1)],
  ['non-array events', {}],
  ['throwing getter', [{ ...event('a'), get data() { throw new Error('getter'); } }]],
  ['throwing toJSON', [{ ...event('a'), data: { toJSON() { throw new Error('toJSON'); } } }]],
  ['cycle', [{ ...event('a'), data: cycle }]],
  ['non-object data', [{ ...event('a'), data: { toJSON: () => [] } }]],
])('push and emit reject without throwing synchronously: %s', async (_name, events) => {
  const { attend } = setup(memoryStore());
  let pushed: Promise<void> | undefined;
  expect(() => {
    // @ts-expect-error Exercise malformed input from a JavaScript caller.
    pushed = attend.push('s', events);
  }).not.toThrow();
  await expect(pushed).rejects.toBeInstanceOf(Error);
  const stopped = new AbortController();
  let emitted: Promise<void> | undefined;
  let threw = false;
  let broken = false;
  await attend.attach('s', { key: 'src', async subscribe(_cursor, emit, signal) {
    try {
      // @ts-expect-error Exercise malformed input from a JavaScript caller.
      emitted = emit({ events });
    } catch {
      threw = true;
    }
    await emitted?.catch(() => undefined);
    broken = signal.aborted;
    stopped.abort();
  } }, { signal: stopped.signal });
  expect(threw).toBe(false);
  await expect(emitted).rejects.toBeInstanceOf(Error);
  expect(broken).toBe(true);
  expect(await attend.inspect('s')).toBeNull();
});
