import { expect, test } from 'vitest';
import { createAttend, DEFAULT_POLICY, KINDS } from '../src/index.js';
import type { Buffer, Entry } from '../src/types.js';
import { deadline } from '../src/core/window.js';
import { fakeClock, recordingSink, scriptedJudge } from '../src/testing.js';

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
