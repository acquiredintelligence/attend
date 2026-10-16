import { getEventListeners } from 'node:events';
import { setFlagsFromString } from 'node:v8';
import { runInNewContext } from 'node:vm';
import { describe, expect, test } from 'vitest';
import { DEFAULT_POLICY } from '../../src/core/types.js';
import type { DecisionAppend, LoggedInput, ScopeRecord } from '../../src/core/types.js';
import { freezingStore, memoryStore } from '../../src/store/memory.js';
import type { ScopeStore, ScopeWrite, Snapshot } from '../../src/store/types.js';
import { decisionLogConformance, scopeStoreConformance } from '../../src/testing/conformance.js';

function record(scope: string, version: number): ScopeRecord {
  return {
    scope, version, cursors: {}, seen: [], buffer: { entries: [], dropped: 0, gap: false, forcedAt: null },
    batch: null, outbox: { decisions: [], deadLetters: [] }, recent: [], dueAt: null,
  };
}

function snapshot(scope: string, version: number): Snapshot {
  return { version, record: record(scope, version), policy: DEFAULT_POLICY, reducer: 1 };
}

function logged(id: string): LoggedInput {
  return { input: { kind: 'events', events: [{ id, at: 1000, name: 'changed', data: { id }, kind: 'closing' }] }, now: 1000 };
}

function append(id: string, scope = 's', at = 1000): DecisionAppend {
  return {
    kind: 'decision', id, scope, route: 'interrupt', at,
    body: {
      id, scope, route: 'interrupt', at, confidence: null, predicates: {},
      probabilities: { interrupt: 1, queue: 0, note: 0, ask: 0, skip: 0, wait: 0 },
      window: { id, events: [], reason: 'closing', waits: 0, dropped: 0, gap: false, settledAt: at },
    },
  };
}

function write(scope = 's', expectedVersion = 0, options: Partial<Pick<ScopeWrite, 'inputs' | 'snapshot' | 'appends' | 'due'>> = {}): ScopeWrite {
  return { scope, expectedVersion, inputs: [logged(`${scope}:${expectedVersion}`)], snapshot: null, appends: [], ...options };
}

/** Hands out the scope it holds and keeps writing to it, as a store that keeps rows by reference without copying at load would. */
function byReference(inner: ScopeStore): ScopeStore {
  const held = new Map<string, { version: number; snapshot: Snapshot | null; tail: LoggedInput[] }>();
  return {
    durable: inner.durable,
    log: inner.log,
    async load(scope) {
      const stored = await inner.load(scope);
      if (stored === null) return null;
      let row = held.get(scope);
      if (!row) {
        row = { version: stored.version, snapshot: stored.snapshot, tail: [...stored.tail] };
        held.set(scope, row);
      }
      return row;
    },
    async commit(writes) {
      const outcomes = await inner.commit(writes);
      writes.forEach((write, i) => {
        const row = held.get(write.scope);
        if (outcomes[i] !== 'ok' || !row) return;
        row.version += write.inputs.length;
        if (write.snapshot) {
          row.snapshot = write.snapshot;
          row.tail.length = 0;
        } else row.tail.push(...write.inputs);
      });
      return outcomes;
    },
    due: (now, limit, after) => inner.due(now, limit, after),
    async remove(scope) {
      held.delete(scope);
      await inner.remove(scope);
    },
  };
}

describe('memoryStore conformance', () => {
  for (const c of [...scopeStoreConformance(() => memoryStore()), ...decisionLogConformance(() => memoryStore())]) test(c.name, c.run);
});

describe('freezingStore conformance', () => {
  for (const c of [...scopeStoreConformance(() => freezingStore(memoryStore())), ...decisionLogConformance(() => freezingStore(memoryStore()))]) test(c.name, c.run);
});

test('the unchanged-read case rejects a port that overwrites returned containers', async () => {
  const cases = scopeStoreConformance(() => byReference(memoryStore()));
  for (const c of cases) {
    if (c.name === 'what a read returned is unchanged by later commits, trims and removes') {
      await expect(c.run()).rejects.toThrow('returned values unchanged');
    } else {
      await expect(c.run()).resolves.toBeUndefined();
    }
  }
  expect(cases.at(-1)?.name).toBe('what a read returned is unchanged by later commits, trims and removes');
});

test('freezingStore freezes committed snapshots, input events and append bodies', async () => {
  const inner = memoryStore();
  const store = freezingStore(inner);
  const saved = snapshot('s', 1);
  const input = logged('event');
  const row = append('decision');
  Object.freeze(saved.record.buffer);
  expect(await store.commit([write('s', 0, { snapshot: saved, appends: [row] })])).toEqual(['ok']);
  expect(await store.commit([write('s', 1, { inputs: [input] })])).toEqual(['ok']);
  expect(() => { saved.record.scope = 'changed'; }).toThrow(TypeError);
  expect(() => { saved.record.buffer.entries.push({ id: 'x', at: 1, name: 'x', data: {}, kind: 'closing', seenAt: 1 }); }).toThrow(TypeError);
  if (input.input.kind !== 'events') throw new Error('expected events');
  const event = input.input.events[0]!;
  expect(() => { event.name = 'changed'; }).toThrow(TypeError);
  expect(() => { row.body.scope = 'changed'; }).toThrow(TypeError);
  expect(await inner.load('s')).toEqual({ version: 2, snapshot: snapshot('s', 1), tail: [logged('event')] });
  expect(await inner.log.find('decision', 'decision')).toEqual({ ...append('decision'), position: '1' });
  expect(store.durable).toBe(false);
  expect(store.log).toBe(inner.log);
  expect(store.close).toBeUndefined();
});

test('freezingStore delegates load, due, remove and an optional close', async () => {
  const inner = memoryStore();
  let closed = 0;
  const store = freezingStore({ ...inner, durable: true, async close() { closed += 1; } });
  expect(store.durable).toBe(true);
  expect(await store.commit([write('s', 0, { due: 1234 })])).toEqual(['ok']);
  expect(await store.load('s')).toEqual({ version: 1, snapshot: null, tail: [logged('s:0')] });
  expect(await store.due(1234, 1)).toEqual([{ due: 1234, scope: 's' }]);
  await store.remove('s');
  expect(await store.load('s')).toBeNull();
  expect(await store.due(1234, 1)).toEqual([]);
  await store.close?.();
  expect(closed).toBe(1);
});

test('committed contents stay by reference while load containers are fresh', async () => {
  const store = memoryStore();
  const saved = snapshot('s', 1);
  const input = logged('tail');
  const row = append('row');
  expect(await store.commit([write('s', 0, { snapshot: saved, appends: [row] })])).toEqual(['ok']);
  expect(await store.commit([write('s', 1, { inputs: [input] })])).toEqual(['ok']);
  const loaded = await store.load('s');
  const again = await store.load('s');
  expect(loaded).toEqual({ version: 2, snapshot: saved, tail: [input] });
  expect(again).not.toBe(loaded);
  expect(again?.tail).not.toBe(loaded?.tail);
  expect(loaded?.snapshot).toBe(saved);
  expect(loaded?.tail[0]).toBe(input);
  expect((await store.log.find('decision', 'row'))?.body).toBe(row.body);
});

test('the decision ring and its indexes are bounded by rows', async () => {
  const store = memoryStore({ decisions: { maxRows: 3 } });
  expect(await store.commit([write('a', 0, { appends: [append('one', 'a'), append('two', 'a')] })])).toEqual(['ok']);
  expect(await store.commit([write('b', 0, { appends: [append('three', 'b'), append('four', 'b'), append('five', 'b')] })])).toEqual(['ok']);
  expect(await store.log.bounds()).toEqual({ first: '3', last: '5' });
  expect(await store.log.find('decision', 'one')).toBeNull();
  expect(await store.log.read({ after: null, scope: 'a', limit: 10 })).toEqual({ rows: [], checked: '5', more: false });
  expect(await store.commit([write('a', 1, { appends: [append('one', 'a')] })])).toEqual(['ok']);
  expect(await store.log.find('decision', 'one')).toEqual({ ...append('one', 'a'), position: '6' });
  const defaults = memoryStore();
  expect(await defaults.commit([write('s', 0, { appends: Array.from({ length: 10_001 }, (_, i) => append(String(i))) })])).toEqual(['ok']);
  expect(await defaults.log.bounds()).toEqual({ first: '2', last: '10001' });
});

test('maxAgeMs drops old leading rows on append and always preserves the head', async () => {
  const store = memoryStore({ decisions: { maxAgeMs: 60_000 } });
  const old = Date.now() - 120_000;
  expect(await store.commit([write('s', 0, { appends: [append('old', 's', old)] })])).toEqual(['ok']);
  expect(await store.log.bounds()).toEqual({ first: '1', last: '1' });
  expect(await store.commit([write('s', 1, { appends: [append('new', 's', Date.now())] })])).toEqual(['ok']);
  expect(await store.log.find('decision', 'old')).toBeNull();
  expect(await store.log.bounds()).toEqual({ first: '2', last: '2' });
  const onlyOld = memoryStore({ decisions: { maxAgeMs: 60_000 } });
  expect(await onlyOld.commit([write('s', 0, { appends: [append('one', 's', old), append('two', 's', old), append('three', 's', old)] })])).toEqual(['ok']);
  expect(await onlyOld.log.read({ after: null, limit: 10 })).toEqual({ rows: [{ ...append('three', 's', old), position: '3' }], checked: '3', more: false });
});

test('due buckets preserve exact times, sort ties and page within a second', async () => {
  const store = memoryStore();
  expect(await store.commit([
    write('middle', 0, { due: 1500 }), write('z', 0, { due: 1200 }), write('a', 0, { due: 1200 }),
    write('end', 0, { due: 1999 }), write('next', 0, { due: 2000 }),
  ])).toEqual(['ok', 'ok', 'ok', 'ok', 'ok']);
  expect(await store.due(1499, 10)).toEqual([{ due: 1200, scope: 'a' }, { due: 1200, scope: 'z' }]);
  expect(await store.due(1999, 10)).toEqual([{ due: 1200, scope: 'a' }, { due: 1200, scope: 'z' }, { due: 1500, scope: 'middle' }, { due: 1999, scope: 'end' }]);
  expect(await store.due(2000, 2, { due: 1200, scope: 'a' })).toEqual([{ due: 1200, scope: 'z' }, { due: 1500, scope: 'middle' }]);
  expect(await store.due(2000, 10, { due: 1500, scope: 'middle' })).toEqual([{ due: 1999, scope: 'end' }, { due: 2000, scope: 'next' }]);
  expect(await store.due(2000, 0)).toEqual([]);
  expect(await store.due(2000, -1)).toEqual([]);
  expect(await store.commit([write('a', 1, { due: 2500 }), write('z', 1, { due: null })])).toEqual(['ok', 'ok']);
  expect(await store.due(3000, 10)).toEqual([{ due: 1500, scope: 'middle' }, { due: 1999, scope: 'end' }, { due: 2000, scope: 'next' }, { due: 2500, scope: 'a' }]);
});

test('positions are dense, numeric and validated by reads, waits and comparisons', async () => {
  const store = memoryStore();
  expect(await store.commit([
    write('s', 0, { appends: [append('one'), append('one'), append('two')] }),
    write('other', 0, { appends: [append('two'), append('three', 'other')] }),
  ])).toEqual(['ok', 'ok']);
  expect(await store.log.read({ after: '0', limit: 10 })).toEqual({
    rows: [{ ...append('one'), position: '1' }, { ...append('two'), position: '2' }, { ...append('three', 'other'), position: '3' }], checked: '3', more: false,
  });
  expect(store.log.compare('9', '10')).toBeLessThan(0);
  expect(store.log.compare('10', '10')).toBe(0);
  expect(store.log.compare('10', '9')).toBeGreaterThan(0);
  for (const position of ['01', '-1', '1.5', 'x', '9007199254740992']) {
    await expect(store.log.read({ after: position, limit: 10 })).rejects.toThrow(TypeError);
    await expect(async () => store.log.wait(position, 1, new AbortController().signal)).rejects.toThrow(TypeError);
    expect(() => store.log.compare(position, '0')).toThrow(TypeError);
    expect(() => store.log.compare('0', position)).toThrow(TypeError);
  }
});

test('wait removes abort listeners after timeout, append, abort and immediate completion', async () => {
  const store = memoryStore();
  const timed = new AbortController();
  await store.log.wait(null, 5, timed.signal);
  expect(getEventListeners(timed.signal, 'abort')).toHaveLength(0);
  const appended = new AbortController();
  const pending = store.log.wait(null, 60_000, appended.signal);
  expect(getEventListeners(appended.signal, 'abort')).toHaveLength(1);
  expect(await store.commit([write('s', 0, { appends: [append('one')] })])).toEqual(['ok']);
  await pending;
  expect(getEventListeners(appended.signal, 'abort')).toHaveLength(0);
  const cancelled = new AbortController();
  const aborted = store.log.wait('1', 60_000, cancelled.signal);
  expect(getEventListeners(cancelled.signal, 'abort')).toHaveLength(1);
  cancelled.abort();
  await aborted;
  expect(getEventListeners(cancelled.signal, 'abort')).toHaveLength(0);
  await store.log.wait('1', 60_000, cancelled.signal);
  expect(getEventListeners(cancelled.signal, 'abort')).toHaveLength(0);
  const behind = new AbortController();
  await store.log.wait('0', 60_000, behind.signal);
  expect(getEventListeners(behind.signal, 'abort')).toHaveLength(0);
  expect(await store.log.bounds()).toEqual({ first: '1', last: '1' });
});

test('each instance has a distinct stable UUID epoch', async () => {
  const first = memoryStore();
  const second = memoryStore();
  const epoch = await first.log.epoch();
  expect(epoch).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  expect(await first.log.epoch()).toBe(epoch);
  expect(await second.log.epoch()).not.toBe(epoch);
});

test('retained heap stays bounded across 1,000,000 commits', async () => {
  setFlagsFromString('--expose-gc');
  const gc = runInNewContext('gc') as () => void;
  const store = memoryStore();
  const states = Array.from({ length: 1000 }, (_, i) => ({ scope: `s:${i}`, version: 0, tail: 0 }));
  let warm = 0;
  let committed = 0;
  for (let i = 1; i <= 1_000_000; i += 1) {
    const state = states[(i - 1) % 1000]!;
    if (i % 1009 === 0) {
      await store.remove(state.scope);
      state.version = 0;
      state.tail = 0;
    }
    const finalVersion = state.version + 1;
    const saved = state.tail + 1 === 32 ? snapshot(state.scope, finalVersion) : null;
    const outcomes = await store.commit([write(state.scope, state.version, {
      inputs: [{ input: { kind: 'tick' }, now: i }], snapshot: saved,
      due: i % 7 === 0 ? null : 1000 * i,
      appends: i % 10 === 0 ? [] : [append(`decision:${i}`, state.scope)],
    })]);
    if (outcomes.length !== 1 || outcomes[0] !== 'ok') throw new Error(`commit ${i} failed: ${JSON.stringify(outcomes)}`);
    committed += 1;
    state.version = finalVersion;
    state.tail = saved ? 0 : state.tail + 1;
    if (i === 100_000) {
      gc();
      gc();
      warm = process.memoryUsage().heapUsed;
    }
  }
  gc();
  gc();
  const final = process.memoryUsage().heapUsed;
  expect(committed).toBe(1_000_000);
  expect(warm).toBeGreaterThan(0);
  expect(final - warm, `warm heapUsed=${warm}, final heapUsed=${final}`).toBeLessThan(4 * 1024 * 1024);
  expect(await store.log.bounds()).toEqual({ first: '890001', last: '900000' });
  expect((await store.due(Number.MAX_SAFE_INTEGER, 2000)).length).toBeLessThanOrEqual(1000);
}, 300_000);
