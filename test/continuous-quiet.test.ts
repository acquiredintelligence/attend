import { expect, test } from 'vitest';
import { createAttend, DEFAULT_POLICY, KINDS, memoryStore, type AttendConfig } from '../src/index.js';
import type { Buffer, Entry, Kind, Source } from '../src/types.js';
import { deadline } from '../src/core/window.js';
import { initialRecord } from '../src/step.js';
import { fakeClock, harness, recordingSink, scriptedJudge, type ScriptedAnswer } from '../src/testing.js';

function entry(id: string, seenAt: number, extra: Partial<Entry> = {}): Entry {
  return { id, at: -1, name: 'move', data: {}, kind: 'continuous', seenAt, ...extra };
}

function buffer(...entries: Entry[]): Buffer {
  return { entries, dropped: 0, gap: false, forcedAt: null };
}

test.each([
  { first: 40, second: 90, due: 210 },
  { first: 90, second: 40, due: 190 },
])('continuous durations $first then $second wait for their latest end', async ({ first, second, due }) => {
  const clock = fakeClock(100);
  const judge = scriptedJudge([{ route: 'queue' }]);
  const sink = recordingSink();
  const attend = createAttend({ clock, kind: { move: 'continuous' }, judge, sink });
  const running = attend.run();
  try {
    await attend.push('s', { id: 'a', name: 'move', quietForMs: first });
    await clock.advance(20);
    await attend.push('s', { id: 'b', name: 'move', quietForMs: second });
    expect((await attend.inspect('s'))?.dueAt).toBe(due);
    await clock.advance(due - 121);
    expect(judge.calls).toHaveLength(0);
    await clock.advance(1);
    expect(judge.calls.map((call) => call.window.settledAt)).toEqual([due]);
    expect(sink.delivered[0]?.window.events.map((e) => [e.id, e.quietForMs, e.seenAt])).toEqual([['a', first, 100], ['b', second, 120]]);
  } finally {
    await attend.stop();
    await running;
  }
});

test('mixed and out-of-order durations take the maximum event end', () => {
  const policy = { ...DEFAULT_POLICY, streamGapMs: 30 };
  expect(deadline(policy, buffer(entry('a', 200, { observedAt: 100, quietForMs: 90 }), entry('b', 200, { observedAt: 140, quietForMs: 20 })))).toEqual({ at: 190, reason: 'stream_quiet' });
  expect(deadline(policy, buffer(entry('a', 200, { observedAt: 140, quietForMs: 20 }), entry('b', 200, { observedAt: 100, quietForMs: 90 })))).toEqual({ at: 190, reason: 'stream_quiet' });
  expect(deadline(policy, buffer(entry('a', 100), entry('b', 120, { quietForMs: 90 })))).toEqual({ at: 210, reason: 'stream_quiet' });
  expect(deadline(policy, buffer(entry('a', 100, { quietForMs: 90 }), entry('b', 180)))).toEqual({ at: 210, reason: 'stream_quiet' });
});

test('absent durations retain backwards clocks and explicit durations include earlier default entries', () => {
  const policy = { ...DEFAULT_POLICY, streamGapMs: 30 };
  expect(deadline(policy, buffer(entry('a', 200), entry('b', 100)))).toEqual({ at: 130, reason: 'stream_quiet' });
  expect(deadline(policy, buffer(entry('a', 200), entry('b', 100), entry('c', 50, { quietForMs: 0 })))).toEqual({ at: 230, reason: 'stream_quiet' });
  expect(deadline(policy, buffer(entry('a', 200, { observedAt: 120 }), entry('b', 200, { observedAt: 100 })))).toEqual({ at: 150, reason: 'stream_quiet' });
});

test('other triggers remain independent of continuous durations', () => {
  const move = entry('a', 100, { quietForMs: 90 });
  expect(deadline({ ...DEFAULT_POLICY, passiveQuietMs: 15 }, buffer(move, entry('b', 105, { kind: 'passive' })))).toEqual({ at: 120, reason: 'passive_quiet' });
  expect(deadline({ ...DEFAULT_POLICY, closingGraceMs: 10 }, buffer(move, entry('b', 110, { kind: 'closing' })))).toEqual({ at: 120, reason: 'closing' });
  expect(deadline(DEFAULT_POLICY, buffer(move, entry('b', 115, { kind: 'flush' })))).toEqual({ at: 115, reason: 'flush' });
  expect(deadline({ ...DEFAULT_POLICY, maxWindowMs: 50 }, buffer(move))).toEqual({ at: 150, reason: 'age' });
  expect(deadline({ ...DEFAULT_POLICY, maxEvents: 0 }, buffer(move))).toEqual({ at: 100, reason: 'overflow' });
});

test.each([null, true, '10', NaN, Infinity, -Infinity, -1])('invalid duration %s refuses the whole batch', async (quietForMs) => {
  const attend = createAttend({ clock: fakeClock(100), kind: { move: 'continuous' }, judge: scriptedJudge([{ route: 'queue' }]), sink: 'log' });
  try {
    // Decoded JavaScript input can violate the declared public type.
    // @ts-expect-error invalid input is the boundary under test
    await expect(attend.push('s', [{ id: 'a', name: 'move' }, { id: 'b', name: 'move', quietForMs }])).rejects.toThrow('quietForMs');
    expect(await attend.inspect('s')).toBeNull();
  } finally {
    await attend.stop();
  }
});

test.each(KINDS.filter((kind) => kind !== 'continuous'))('duration on %s refuses the whole batch', async (kind) => {
  const attend = createAttend({ clock: fakeClock(100), kind: (event) => event.name === 'move' ? 'continuous' : kind, judge: scriptedJudge([{ route: 'queue' }]), sink: 'log' });
  try {
    await expect(attend.push('s', [{ id: 'a', name: 'move' }, { id: 'b', name: 'other', quietForMs: 0 }])).rejects.toThrow('quietForMs');
    expect(await attend.inspect('s')).toBeNull();
  } finally {
    await attend.stop();
  }
});

test('zero duration is immediate and late observations settle at processing time', async () => {
  const clock = fakeClock(100);
  const judge = scriptedJudge([{ route: 'queue' }]);
  const attend = createAttend({ clock, kind: { move: 'continuous' }, judge, sink: 'log' });
  const running = attend.run();
  try {
    await attend.push('s', { id: 'a', name: 'move', observedAt: 10, quietForMs: 0 });
    await clock.advance(0);
    expect(judge.calls.map((call) => call.window.settledAt)).toEqual([100]);
    expect(judge.calls[0]?.window.events[0]).toMatchObject({ at: 100, observedAt: 10, seenAt: 100, quietForMs: 0 });
  } finally {
    await attend.stop();
    await running;
  }
});

test.each(['continuous', 'passive'] satisfies Kind[])('source duration admission for %s controls its cursor', async (kind) => {
  const clock = fakeClock(100);
  const attend = createAttend({ clock, kind: { move: kind }, judge: scriptedJudge([{ route: 'queue' }]), sink: 'log' });
  const controller = new AbortController();
  const emitted = new AbortController();
  const done = new Promise<void>((resolve) => emitted.signal.addEventListener('abort', () => resolve(), { once: true }));
  let refused = false;
  const source: Source = {
    key: 'feed',
    async subscribe(_cursor, emit, signal) {
      try { await emit({ events: [{ id: 'a', at: 0, observedAt: 100, quietForMs: 40, name: 'move', data: {} }], cursor: '1' }); }
      catch (error) { if (!(error instanceof TypeError)) throw error; refused = true; }
      finally { emitted.abort(); }
      if (!signal.aborted) await new Promise<void>((resolve) => signal.addEventListener('abort', () => resolve(), { once: true }));
    },
  };
  const running = attend.run();
  const attached = attend.attach('s', source, { signal: controller.signal });
  try {
    await done;
    expect(refused).toBe(kind !== 'continuous');
    const record = await attend.inspect('s');
    if (refused) expect(record).toBeNull();
    else expect(record).toMatchObject({ dueAt: 140, cursors: { feed: '1' }, buffer: { entries: [{ quietForMs: 40, observedAt: 100 }] } });
  } finally {
    controller.abort();
    await attached;
    await attend.stop();
    await running;
  }
});

test.each([
  { answer: { route: 'wait' }, phase: 'waiting', delay: 10_000 },
  { answer: { fail: 'temporary' }, phase: 'retrying', delay: 1000 },
] satisfies { answer: ScriptedAnswer; phase: string; delay: number }[])('duration survives a crash during $phase without changing its processing clock', async ({ answer, phase, delay }) => {
  const judge = scriptedJudge([answer, { route: 'queue' }]);
  const h = harness({ kind: () => 'continuous', judge, sink: recordingSink() });
  try {
    await h.push([{ id: 'a', at: -1, name: 'move', data: {}, quietForMs: 40 }]);
    await h.advance(40);
    expect((await h.inspect())?.batch?.phase.phase).toBe(phase);
    h.crash();
    await h.restart();
    await h.advance(delay - 1);
    expect(judge.calls).toHaveLength(1);
    await h.advance(1);
    expect(judge.calls.map((call) => call.now)).toEqual([40, 40 + delay]);
    expect(judge.calls.map((call) => call.window.settledAt)).toEqual([40, 40]);
    expect(judge.calls[1]?.window.events[0]?.quietForMs).toBe(40);
  } finally {
    await h.stop();
  }
});

test('a live policy change recomputes only omitted continuous durations', async () => {
  const store = memoryStore();
  const config = { store, clock: fakeClock(100), kind: { move: 'continuous' }, judge: scriptedJudge([{ route: 'queue' }]), sink: 'log' } satisfies AttendConfig;
  const first = createAttend({ ...config, settle: { streamGapMs: 80 } });
  await first.push('s', [{ id: 'a', name: 'move' }, { id: 'b', name: 'move', quietForMs: 40 }]);
  expect((await first.inspect('s'))?.dueAt).toBe(180);
  await first.stop();
  const second = createAttend({ ...config, settle: { streamGapMs: 20 } });
  try {
    expect((await second.inspect('s'))?.dueAt).toBe(180);
    await second.push('s', []);
    expect((await second.inspect('s'))?.dueAt).toBe(140);
    expect((await second.inspect('s'))?.buffer.entries.map((event) => event.quietForMs)).toEqual([undefined, 40]);
  } finally {
    await second.stop();
  }
});

test('a revision-two snapshot and mixed tail rebuild continuous end times', async () => {
  const store = memoryStore();
  const policy = { ...DEFAULT_POLICY, streamGapMs: 30 };
  const record = { ...initialRecord('s'), version: 1, seen: ['a'], buffer: buffer(entry('a', 100, { observedAt: 100 })), dueAt: 130 };
  expect(await store.commit([{ scope: 's', expectedVersion: 0, inputs: [{ now: 100, input: { kind: 'tick' } }], snapshot: { version: 1, record, policy, reducer: 2 }, appends: [] }])).toEqual(['ok']);
  expect(await store.commit([{ scope: 's', expectedVersion: 1, snapshot: null, appends: [], inputs: [{ now: 120, input: { kind: 'events', events: [
    { id: 'b', at: 0, name: 'move', data: {}, kind: 'continuous', observedAt: 110, quietForMs: 90 },
    { id: 'c', at: 0, name: 'move', data: {}, kind: 'continuous' },
  ] } }] }])).toEqual(['ok']);
  const attend = createAttend({ store, clock: fakeClock(120), settle: { streamGapMs: 30 }, kind: { move: 'continuous' }, judge: scriptedJudge([{ route: 'queue' }]), sink: 'log' });
  try {
    await attend.push('s', []);
    const loaded = await attend.inspect('s');
    expect(loaded?.dueAt).toBe(200);
    expect(loaded?.buffer.entries.map((event) => [event.id, event.quietForMs, event.seenAt])).toEqual([['a', undefined, 100], ['b', 90, 120], ['c', undefined, 120]]);
    expect((await store.load('s'))?.snapshot?.reducer).toBe(3);
  } finally {
    await attend.stop();
  }
});
