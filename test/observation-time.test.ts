import { expect, test } from 'vitest';
import { createAttend, DEFAULT_POLICY, memoryStore } from '../src/index.js';
import type { Buffer, Entry, Judgment, Kind, SettleReason, Source } from '../src/types.js';
import type { ScopeStore } from '../src/store/types.js';
import { assertWindow, deadline, windowFrom, windowPrune } from '../src/core/window.js';
import { fakeClock, harness, recordingSink, scriptedJudge, type ScriptedAnswer } from '../src/testing.js';
import { initialRecord } from '../src/step.js';

function entry(id: string, seenAt: number, observedAt?: number, kind: Kind = 'continuous'): Entry {
  return { id, at: -1, name: 'change', data: {}, kind, seenAt, ...(observedAt === undefined ? {} : { observedAt }) };
}

function buffer(...entries: Entry[]): Buffer {
  return { entries, dropped: 0, gap: false, forcedAt: null };
}

function gate() {
  const opened = new AbortController();
  const promise = new Promise<void>((resolve) => opened.signal.addEventListener('abort', () => resolve(), { once: true }));
  return { promise, open: () => opened.abort() };
}

test('source observation sets the quiet deadline without changing the other clocks', async () => {
  const clock = fakeClock(1000);
  const judge = scriptedJudge([{ route: 'queue' }]);
  const sink = recordingSink();
  const store = memoryStore();
  const attend = createAttend({ store, clock, kind: { change: 'continuous' }, judge, sink });
  const running = attend.run();
  try {
    await attend.push('s', { id: 'a', at: -1, observedAt: 100, name: 'change' });
    expect((await attend.inspect('s'))?.dueAt).toBe(1100);
    await clock.advance(99);
    expect(judge.calls).toHaveLength(0);
    await clock.advance(1);
    expect(judge.calls.map((call) => call.window.settledAt)).toEqual([1100]);
    expect(sink.delivered[0]?.window.events[0]).toMatchObject({ at: -1, observedAt: 100, seenAt: 1000 });
    expect(sink.delivered[0]?.at).toBe(1100);
  } finally {
    await attend.stop();
    await running;
  }
});

test.each([null, true, '100', NaN, Infinity, -Infinity, 1001])('rejects invalid or future observation %s before admission', async (observedAt) => {
  const store = memoryStore();
  const attend = createAttend({ store, clock: fakeClock(1000), kind: { change: 'continuous' }, judge: scriptedJudge([{ route: 'queue' }]), sink: 'log' });
  try {
    // JavaScript and decoded input can violate the public type.
    // @ts-expect-error invalid input is the boundary under test
    await expect(attend.push('s', { id: 'a', name: 'change', observedAt })).rejects.toThrow('observedAt');
    expect(await attend.inspect('s')).toBeNull();
  } finally {
    await attend.stop();
  }
});

test.each([
  { kind: 'closing', gap: DEFAULT_POLICY.closingGraceMs, reason: 'closing' },
  { kind: 'continuous', gap: DEFAULT_POLICY.streamGapMs, reason: 'stream_quiet' },
  { kind: 'passive', gap: DEFAULT_POLICY.passiveQuietMs, reason: 'passive_quiet' },
] satisfies { kind: Kind; gap: number; reason: SettleReason }[])('older $kind observations do not shorten quiet time', ({ kind, gap, reason }) => {
  expect(deadline(DEFAULT_POLICY, buffer(entry('a', 1000, 900, kind), entry('b', 1000, 100, kind)))).toEqual({ at: 900 + gap, reason });
});

test('source age uses the oldest act while absent observations retain arrival behavior', () => {
  const policy = { ...DEFAULT_POLICY, streamGapMs: 100_000 };
  expect(deadline(policy, buffer(entry('a', 1000, 900), entry('b', 1000, 0)))).toEqual({ at: 60_000, reason: 'age' });
  expect(deadline(policy, buffer(entry('a', 1000), entry('b', 500)))).toEqual({ at: 61_000, reason: 'age' });
  expect(deadline(DEFAULT_POLICY, buffer(entry('a', 1000), entry('b', 500)))).toEqual({ at: 1500, reason: 'stream_quiet' });
  expect(deadline(DEFAULT_POLICY, buffer(entry('a', 1000), entry('b', 500, 350)))).toEqual({ at: 2000, reason: 'stream_quiet' });
});

test('an old background observation behind a fresh one is pruned without moving arrival order', () => {
  const open = windowFrom('s', buffer(entry('fresh', 1000, 900, 'context'), entry('old', 1000, 100, 'context'), entry('latest', 1000, 950, 'context')));
  expect(windowPrune(open, 200, 1000)?.map((event) => event.id)).toEqual(['old']);
  expect(open.entries.slice(open.start).map((event) => event.id)).toEqual(['fresh', 'latest']);
  expect(open.dropped).toBe(1);
  assertWindow('s', open);
});

test('flush, count and byte deadlines keep ingestion time', () => {
  expect(deadline(DEFAULT_POLICY, buffer(entry('a', 1000, 100, 'flush')))).toEqual({ at: 1000, reason: 'flush' });
  expect(deadline({ ...DEFAULT_POLICY, maxEvents: 0 }, buffer(entry('a', 1000, 100)))).toEqual({ at: 1000, reason: 'overflow' });
  expect(deadline({ ...DEFAULT_POLICY, maxWindowBytes: 0 }, buffer(entry('a', 1000, 100)))).toEqual({ at: 1000, reason: 'size' });
});

test('late settlement and the judge lease use processing time', async () => {
  const clock = fakeClock(2000);
  const response = gate();
  let calls = 0;
  const judge = async (): Promise<Judgment> => { calls += 1; await response.promise; return { route: 'skip' }; };
  const attend = createAttend({ clock, kind: { change: 'continuous' }, judge, sink: 'log', settle: { judgeTimeoutMs: 100 } });
  const running = attend.run();
  try {
    await attend.push('s', { id: 'a', name: 'change', observedAt: 100 });
    await clock.advance(0);
    const batch = (await attend.inspect('s'))?.batch;
    expect(batch?.window.settledAt).toBe(2000);
    expect(batch?.phase).toMatchObject({ phase: 'judging', leaseUntil: 2200 });
    expect(calls).toBe(1);
    await clock.advance(99);
    expect(calls).toBe(1);
  } finally {
    response.open();
    await clock.advance(0);
    await attend.stop();
    await running;
  }
});

test('snapshot and tail recovery preserve observation clocks', async () => {
  const store = memoryStore();
  const clock = fakeClock(1000);
  const first = createAttend({ store, clock, kind: { change: 'continuous' }, judge: scriptedJudge([{ route: 'queue' }]), sink: 'log' });
  await first.push('s', { id: 'a', name: 'change', observedAt: 100 });
  await first.push('s', { id: 'b', name: 'change', observedAt: 200 });
  await first.stop();
  const stored = await store.load('s');
  expect(stored?.snapshot?.record.buffer.entries[0]?.observedAt).toBe(100);
  expect(stored?.tail.length).toBeGreaterThan(0);
  const judge = scriptedJudge([{ route: 'queue' }]);
  const second = createAttend({ store, clock, kind: { change: 'continuous' }, judge, sink: 'log' });
  const running = second.run();
  try {
    await second.push('s', []);
    expect((await second.inspect('s'))?.dueAt).toBe(1200);
    await clock.advance(199);
    expect(judge.calls).toHaveLength(0);
    await clock.advance(1);
    expect(judge.calls[0]?.window.events.map((event) => [event.id, event.observedAt, event.seenAt])).toEqual([['a', 100, 1000], ['b', 200, 1000]]);
    expect(judge.calls[0]?.window.settledAt).toBe(1200);
  } finally {
    await second.stop();
    await running;
  }
});

test.each([
  { answer: { route: 'wait' }, phase: 'waiting', delay: 10_000 },
  { answer: { fail: 'temporary' }, phase: 'retrying', delay: 1000 },
] satisfies { answer: ScriptedAnswer; phase: string; delay: number }[])('source time survives a crash while $phase', async ({ answer, phase, delay }) => {
  const judge = scriptedJudge([answer, { route: 'queue' }]);
  const h = harness({ kind: () => 'continuous', judge, sink: recordingSink() });
  try {
    await h.advance(1000);
    await h.push([{ id: 'a', at: -1, name: 'change', data: {}, observedAt: 100 }]);
    await h.advance(100);
    expect((await h.inspect())?.batch?.phase.phase).toBe(phase);
    h.crash();
    await h.restart();
    await h.advance(delay - 1);
    expect(judge.calls).toHaveLength(1);
    await h.advance(1);
    expect(judge.calls.map((call) => call.now)).toEqual([1100, 1100 + delay]);
    expect(judge.calls.map((call) => call.window.settledAt)).toEqual([1100, 1100]);
    expect(judge.calls[1]?.window.events[0]).toMatchObject({ observedAt: 100, seenAt: 1000 });
  } finally {
    await h.stop();
  }
});

test.each([100, 1001])('source attachment validates observation %s before its cursor commits', async (observedAt) => {
  const clock = fakeClock(1000);
  const judge = scriptedJudge([{ route: 'queue' }]);
  const attend = createAttend({ clock, kind: { change: 'continuous' }, judge, sink: 'log' });
  const controller = new AbortController();
  const emitted = gate();
  let refused = false;
  const source: Source = {
    key: 'feed',
    async subscribe(_cursor, emit, signal) {
      try { await emit({ events: [{ id: 'a', at: -1, name: 'change', data: {}, observedAt }], cursor: '1' }); }
      catch (error) { if (!(error instanceof TypeError)) throw error; refused = true; }
      finally { emitted.open(); }
      if (!signal.aborted) await new Promise<void>((resolve) => signal.addEventListener('abort', () => resolve(), { once: true }));
    },
  };
  const running = attend.run();
  const attached = attend.attach('s', source, { signal: controller.signal });
  try {
    await emitted.promise;
    expect(refused).toBe(observedAt > 1000);
    const record = await attend.inspect('s');
    if (refused) expect(record).toBeNull();
    else {
      expect(record?.cursors).toEqual({ feed: '1' });
      await clock.advance(100);
      expect(judge.calls[0]?.window.events[0]).toMatchObject({ observedAt: 100, seenAt: 1000 });
    }
  } finally {
    controller.abort();
    await attached;
    await attend.stop();
    await running;
  }
});

test('a prior reducer snapshot and tail retain ingestion-time deadlines on upgrade', async () => {
  const store = memoryStore();
  const record = { ...initialRecord('s'), version: 1, seen: ['a'], buffer: buffer(entry('a', 1000)), dueAt: 2000 };
  expect(await store.commit([{ scope: 's', expectedVersion: 0, inputs: [{ now: 1000, input: { kind: 'tick' } }], snapshot: { version: 1, record, policy: DEFAULT_POLICY, reducer: 1 }, appends: [] }])).toEqual(['ok']);
  expect(await store.commit([{ scope: 's', expectedVersion: 1, inputs: [{ now: 1100, input: { kind: 'events', events: [{ id: 'b', at: -1, name: 'change', data: {}, kind: 'continuous' }] } }], snapshot: null, appends: [] }])).toEqual(['ok']);
  const attend = createAttend({ store, clock: fakeClock(1200), kind: { change: 'continuous' }, judge: scriptedJudge([{ route: 'queue' }]), sink: 'log' });
  try {
    await attend.push('s', []);
    const loaded = await attend.inspect('s');
    expect(loaded?.dueAt).toBe(2100);
    expect(loaded?.buffer.entries.map((event) => [event.id, event.seenAt, event.observedAt])).toEqual([['a', 1000, undefined], ['b', 1100, undefined]]);
    expect((await store.load('s'))?.snapshot?.reducer).toBe(6);
  } finally {
    await attend.stop();
  }
});

test('a conflicting write reloads without replacing observation time', async () => {
  const inner = memoryStore();
  const clock = fakeClock(1000);
  const raced = gate();
  let injected = false;
  const store: ScopeStore = {
    ...inner,
    async commit(writes) {
      if (!injected) {
        injected = true;
        expect(writes.map((write) => write.scope)).toEqual(['s']);
        await inner.commit([{ scope: 's', expectedVersion: 0, inputs: [{ now: 1000, input: { kind: 'events', events: [{ id: 'racer', at: 0, name: 'context', kind: 'context', data: {} }] } }], snapshot: null, appends: [] }]);
        raced.open();
        return writes.map(() => 'conflict');
      }
      return inner.commit(writes);
    },
  };
  const attend = createAttend({ store, clock, kind: { change: 'continuous' }, judge: scriptedJudge([{ route: 'queue' }]), sink: 'log' });
  try {
    const pushed = attend.push('s', { id: 'a', name: 'change', observedAt: 100, quietForMs: 1000 });
    await raced.promise;
    await clock.advance(50);
    await pushed;
    const record = await attend.inspect('s');
    expect(record?.buffer.entries.map((event) => event.id)).toEqual(['racer', 'a']);
    expect(record?.buffer.entries[1]?.observedAt).toBe(100);
    expect(record?.buffer.entries[1]?.quietForMs).toBe(1000);
    expect(record?.dueAt).toBe(1100);
  } finally {
    await attend.stop();
  }
});
