import { expect, test } from 'vitest';
import { createAttend, DEFAULT_POLICY, memoryStore } from '../src/index.js';
import type { Buffer, Entry, Kind } from '../src/types.js';
import { assertWindow, deadline, windowFrom, windowPrune } from '../src/core/window.js';
import { fakeClock, recordingSink, scriptedJudge } from '../src/testing.js';

function entry(id: string, seenAt: number, observedAt?: number, kind: Kind = 'continuous'): Entry {
  return { id, at: -1, name: 'change', data: {}, kind, seenAt, ...(observedAt === undefined ? {} : { observedAt }) };
}

function buffer(...entries: Entry[]): Buffer {
  return { entries, dropped: 0, gap: false, forcedAt: null };
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

test('out-of-order observations do not shorten quiet time', () => {
  expect(deadline(DEFAULT_POLICY, buffer(entry('a', 1000, 900), entry('b', 1000, 100)))).toEqual({ at: 1900, reason: 'stream_quiet' });
});

test('source age uses the oldest act while absent observations retain arrival behavior', () => {
  const policy = { ...DEFAULT_POLICY, streamGapMs: 100_000 };
  expect(deadline(policy, buffer(entry('a', 1000, 900), entry('b', 1000, 0)))).toEqual({ at: 60_000, reason: 'age' });
  expect(deadline(policy, buffer(entry('a', 1000), entry('b', 500)))).toEqual({ at: 61_000, reason: 'age' });
  expect(deadline(DEFAULT_POLICY, buffer(entry('a', 1000), entry('b', 500)))).toEqual({ at: 1500, reason: 'stream_quiet' });
  expect(deadline(DEFAULT_POLICY, buffer(entry('a', 1000), entry('b', 500, 400)))).toEqual({ at: 2000, reason: 'stream_quiet' });
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
