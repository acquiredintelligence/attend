import { expect, test } from 'vitest';
import { readFileSync } from 'node:fs';
import { createAttend, memoryStore } from '../src/index.js';
import { createAttendInternal } from '../src/attend.js';
import { fromRecord } from '../src/core/state.js';
import { DEFAULT_POLICY } from '../src/types.js';
import { openIntake } from '../src/intake.js';
import type { Event, ScopeRecord, ScopeWrite, SettlePolicy, StoredScope } from '../src/types.js';
import { fakeClock, scriptedJudge } from '../src/testing.js';

type Answer = 'wait' | 'queue' | 'fail';
interface Offer { at: number; ids: string[]; wait: boolean; waits: number }
const event = (id: string, observedAt: number, quietForMs = 10): Event => ({ id, at: observedAt, observedAt, quietForMs, name: 'reading', text: 'q'.repeat(20), data: {} });

async function run(answers: Answer[], body: (clock: ReturnType<typeof fakeClock>, put: (events: Event[]) => Promise<void>, calls: Offer[]) => Promise<void>, settle: Partial<SettlePolicy> = {}, start = 0) {
  const clock = fakeClock(start), calls: Offer[] = [];
  const attend = createAttend({ clock, kind: { reading: 'continuous' }, sink: 'log',
    settle: { continuation: 'accumulate', streamGapMs: 10, waitHorizonMs: 20, maxWaits: 2, retryBackoffMs: [30, 60], ...settle },
    judge: async (input) => {
      calls.push({ at: clock.now(), ids: input.window.events.map((e) => e.id), wait: input.routes.some((r) => r.route === 'wait'), waits: input.window.waits });
      const answer = answers[calls.length - 1] ?? 'queue';
      if (answer === 'fail') throw new Error('controlled outage');
      return { route: answer };
    },
  });
  const intake = openIntake(attend, 's'), running = attend.run();
  try { await body(clock, (events) => intake.submit(async () => ({ events })), calls); }
  finally { await intake.close(); await attend.stop(); await running; }
}

test('horizon reoffers until the wait allowance is used', async () => {
  await run(['wait', 'wait', 'queue'], async (clock, put, calls) => {
    await put([event('a', 0)]);
    await clock.advance(50);
    expect(calls).toEqual([
      { at: 10, ids: ['a'], wait: true, waits: 0 },
      { at: 30, ids: ['a'], wait: true, waits: 1 },
      { at: 50, ids: ['a'], wait: false, waits: 2 },
    ]);
  });
});

test('horizon absorbs fresh entries before their quiet deadline', async () => {
  await run(['wait', 'wait', 'queue'], async (clock, put, calls) => {
    await put([event('a', 0)]);
    await clock.advance(25);
    await put([event('b', 25, 100)]);
    await clock.advance(25);
    expect(calls).toEqual([
      { at: 10, ids: ['a'], wait: true, waits: 0 },
      { at: 30, ids: ['a', 'b'], wait: true, waits: 1 },
      { at: 50, ids: ['a', 'b'], wait: false, waits: 2 },
    ]);
  });
});

test.each([10, 100])('retry absorbs fresh events at not-before with quiet duration %s', async (quiet) => {
  await run(['fail', 'queue'], async (clock, put, calls) => {
    await put([event('a', 0)]);
    await clock.advance(15);
    await put([event('b', 15, quiet)]);
    await clock.advance(24);
    expect(calls).toHaveLength(1);
    await clock.advance(1);
    expect(calls).toEqual([
      { at: 10, ids: ['a'], wait: true, waits: 0 },
      { at: 40, ids: ['a', 'b'], wait: true, waits: 0 },
    ]);
  });
});

test('a wait retains the provider failure allowance', async () => {
  await run(['fail', 'wait', 'fail', 'queue'], async (clock, put, calls) => {
    await put([event('a', 0)]);
    await clock.advance(120);
    expect(calls.map((c) => c.at)).toEqual([10, 40, 60, 120]);
    expect(calls.map((c) => c.waits)).toEqual([0, 0, 1, 1]);
  });
});

test.each([1, 2])('merged count evaluates all entries with %s fresh acts', async (fresh) => {
  await run(['fail', 'queue'], async (clock, put, calls) => {
    await put([event('a', 0)]);
    await clock.advance(15);
    await put(Array.from({ length: fresh }, (_, i) => event(`b${i}`, 15, 100)));
    await clock.advance(25);
    expect(calls[1]).toEqual({ at: 40, ids: ['a', ...Array.from({ length: fresh }, (_, i) => `b${i}`)], wait: fresh === 1, waits: 0 });
  }, { maxEvents: 2 });
});

test('age withholds wait at the initial quiet tie', async () => {
  await run(['queue'], async (clock, put, calls) => {
    await put([event('a', 0)]);
    await clock.advance(10);
    expect(calls).toEqual([{ at: 10, ids: ['a'], wait: false, waits: 0 }]);
  }, { maxWindowMs: 10 });
});

test('an older absorbed observation participates in age eligibility', async () => {
  await run(['fail', 'queue'], async (clock, put, calls) => {
    await put([event('a', 100)]);
    await clock.advance(20);
    await put([event('b', 0, 1000)]);
    expect(calls).toHaveLength(1);
    await clock.advance(20);
    expect(calls[1]).toEqual({ at: 140, ids: ['a', 'b'], wait: false, waits: 0 });
  }, { maxWindowMs: 100 }, 100);
});

test.each<Answer>(['wait', 'fail'])('combined byte threshold is enforced after %s', async (answer) => {
  await run([answer, 'queue'], async (clock, put, calls) => {
    await put([event('a', 0)]);
    await clock.advance(15);
    await put([event('b', 15, 100)]);
    if (answer === 'fail') { expect(calls).toHaveLength(1); await clock.advance(25); }
    expect(calls[1]).toEqual({ at: answer === 'wait' ? 15 : 40, ids: ['a', 'b'], wait: false, waits: answer === 'wait' ? 1 : 0 });
  }, { maxWindowBytes: 100 });
});

test.each(['separate', 'accumulate'] as const)('cannot change continuation with an open window from %s', async (from) => {
  const store = memoryStore(), clock = fakeClock();
  const config = { store, clock, kind: { reading: 'continuous' } as const, judge: scriptedJudge([{ route: 'queue' }]), sink: 'log' as const };
  const previous = createAttend({ ...config, settle: { continuation: from } });
  await previous.push('s', event('a', 0));
  const before = await previous.inspect('s');
  const next = createAttend({ ...config, settle: { continuation: from === 'separate' ? 'accumulate' : 'separate' } });
  await expect(next.push('s', event('b', 0))).rejects.toThrow('continuation');
  expect(await next.inspect('s')).toEqual(before);
});

test.each([null, false, 0, 'other'])('refuses invalid continuation %s', (value) => {
  expect(() => Reflect.apply(createAttend, undefined, [{ kind: {}, judge: scriptedJudge([]), sink: 'log', settle: { continuation: value } }])).toThrow('continuation');
});

interface LegacyCase { name: string; through: number; writes: ScopeWrite[][]; stored: StoredScope; expected: ScopeRecord }
const legacy: { source: string; cases: LegacyCase[] } = JSON.parse(readFileSync(new URL('../conformance/fixtures/continuation-legacy.json', import.meta.url), 'utf8'));

test.each(['flush', 'end', 'held-end'] as const)('%s retains its meaning at retry not-before', async (control) => {
  const clock = fakeClock(), calls: { at: number; reason: string; wait: boolean }[] = [];
  const engine = createAttend({ clock, kind: { reading: 'continuous' }, sink: 'log', settle: { continuation: 'accumulate', retryBackoffMs: [30] },
    judge: async (input) => {
      calls.push({ at: clock.now(), reason: input.window.reason, wait: input.routes.some((r) => r.route === 'wait') });
      if (calls.length === 1) throw new Error('controlled outage');
      return { route: 'queue' };
    },
  });
  const running = engine.run();
  try {
    await engine.push('s', event('a', 0));
    await clock.advance(15);
    if (control === 'end') await engine.push('s', event('b', 15, 100));
    await engine[control === 'held-end' ? 'end' : control]('s');
    await clock.advance(24);
    expect(calls).toHaveLength(1);
    await clock.advance(1);
    const reason = control === 'flush' ? 'forced' : control === 'end' ? 'closing' : 'stream_quiet';
    expect(calls[1]).toEqual({ at: 40, reason, wait: control !== 'flush' });
    expect((await engine.inspect('s'))?.buffer.forcedAt).toBeNull();
  } finally { await engine.stop(); await running; }
});

test.each(legacy.cases)('reads revision-3 $name and refuses an unresolved mode switch', async (item) => {
  const store = memoryStore(), clock = fakeClock(item.through);
  for (const writes of item.writes) expect(await store.commit(writes)).toEqual(writes.map(() => 'ok'));
  expect(await store.load('scope')).toEqual(item.stored);
  const engine = createAttend({ store, clock, kind: { reading: 'continuous' }, judge: scriptedJudge([{ route: 'queue' }]), sink: 'log', settle: { continuation: 'accumulate' } });
  expect(await engine.inspect('scope')).toEqual(item.expected);
  const pending = engine.push('scope', event('b', item.through));
  if (item.name === 'complete') {
    await pending;
    const stored = await store.load('scope');
    expect(stored?.snapshot?.policy.continuation).toBe('accumulate');
    expect(stored?.snapshot?.reducer).toBe(5);
    expect((await engine.inspect('scope'))?.recent).toEqual(item.expected.recent);
  } else {
    await expect(pending).rejects.toThrow('continuation');
    expect(await store.load('scope')).toEqual(item.stored);
  }
});

test.each(['ratio', 'always'] as const)('failure budget survives a waiting restart with %s snapshots', async (cadence) => {
  const store = memoryStore(), clock = fakeClock(), resumedCalls: number[] = [];
  let firstCalls = 0;
  const settle = { continuation: 'accumulate', streamGapMs: 10, waitHorizonMs: 20, retryBackoffMs: [30, 60] } as const;
  const config = { store, clock, kind: { reading: 'continuous' } as const, sink: 'log' as const, settle: { ...settle, retryBackoffMs: [30, 60] } };
  const first = createAttendInternal({ ...config, judge: async () => { if (++firstCalls === 1) throw new Error('outage'); return { route: 'wait' }; } }, cadence);
  const intake = openIntake(first, 's'), running = first.run();
  await intake.submit(async () => ({ events: [event('a', 0)] }));
  await clock.advance(40);
  expect((await first.inspect('s'))?.batch?.phase).toMatchObject({ phase: 'waiting', failures: 1 });
  await intake.close(); await first.stop(); await running;
  const resumed = createAttend({ ...config, judge: async () => { resumedCalls.push(clock.now()); if (resumedCalls.length === 1) throw new Error('outage'); return { route: 'queue' }; } });
  const nextIntake = openIntake(resumed, 's'), nextRun = resumed.run();
  try {
    await clock.advance(0);
    expect(clock.nextAt()).toBe(60);
    await clock.advance(80);
    expect(resumedCalls).toEqual([60, 120]);
    expect((await resumed.inspect('s'))?.batch).toBeNull();
  } finally { await nextIntake.close(); await resumed.stop(); await nextRun; }
});

test('an accumulating snapshot cannot invent missing waiting failures', () => {
  const waiting = legacy.cases.find((item) => item.name === 'waiting-snapshot');
  expect(waiting).toBeDefined();
  if (!waiting) throw new Error('missing fixture');
  expect(() => fromRecord(waiting.expected, { ...DEFAULT_POLICY, continuation: 'accumulate' })).toThrow('requires failures');
});

test('exhaustion after a wait keeps the native dead letter', async () => {
  const store = memoryStore(), clock = fakeClock();
  const judge = scriptedJudge([{ fail: 'outage' }, { route: 'wait' }, { fail: 'outage' }, { fail: 'outage' }]);
  const engine = createAttend({ store, clock, kind: { reading: 'continuous' }, judge, sink: 'log', settle: { continuation: 'accumulate', waitHorizonMs: 20, retryBackoffMs: [30, 60] } });
  const intake = openIntake(engine, 's'), running = engine.run();
  try {
    await intake.submit(async () => ({ events: [event('a', 0)] }));
    await clock.advance(120);
    const page = await store.log.read({ after: null, scope: 's', limit: 10 });
    expect(page.rows).toHaveLength(1);
    expect(page.rows[0]).toMatchObject({ kind: 'deadLetter', at: 120, body: { stage: 'judge', attempts: 4, window: { waits: 1 } } });
    expect((await engine.inspect('s'))?.batch).toBeNull();
  } finally { await intake.close(); await engine.stop(); await running; }
});

test.each([134, 135])('whole-window byte eligibility honors threshold %s', async (limit) => {
  await run(['wait', 'queue'], async (clock, put, calls) => {
    await put([event('a', 0)]); await clock.advance(15);
    await put([event('b', 15, 100)]); await clock.advance(15);
    expect(calls[1]).toEqual({ at: limit === 134 ? 15 : 30, ids: ['a', 'b'], wait: limit === 135, waits: 1 });
  }, { maxWindowBytes: limit });
});

test.each(['gap', 'dropped'] as const)('empty fresh %s metadata belongs to the unresolved retry', async (metadata) => {
  const clock = fakeClock(), calls: { ids: string[]; gap: boolean; dropped: number }[] = [];
  const engine = createAttend({ clock, kind: { reading: 'continuous', out: 'consequence' }, sink: 'log', settle: { continuation: 'accumulate', maxWindowMs: 100, retryBackoffMs: [30] },
    judge: async (input) => {
      calls.push({ ids: input.window.events.map((e) => e.id), gap: input.window.gap, dropped: input.window.dropped });
      if (calls.length === 1) throw new Error('outage');
      return { route: 'queue' };
    },
  });
  const intake = openIntake(engine, 's'), running = engine.run();
  try {
    await intake.submit(async () => ({ events: [event('a', 0)] }));
    await clock.advance(15);
    await intake.submit(async () => metadata === 'gap'
      ? { events: [], truncated: true, cursor: '2' }
      : { events: [{ id: 'old', at: -100, observedAt: -100, name: 'out', data: {} }], cursor: '2' }, { source: 'feed' });
    await clock.advance(25);
    expect(calls[1]).toEqual({ ids: ['a'], gap: metadata === 'gap', dropped: metadata === 'dropped' ? 1 : 0 });
    const record = await engine.inspect('s');
    expect(record?.buffer).toMatchObject({ entries: [], gap: false, dropped: 0 });
    expect(record?.cursors).toEqual({ feed: '2' });
    await intake.submit(async () => ({ events: [event('next', 40)] }));
    await clock.advance(10);
    expect(calls[2]).toEqual({ ids: ['next'], gap: false, dropped: 0 });
  } finally { await intake.close(); await engine.stop(); await running; }
});

test('empty gap metadata at a wait horizon does not remove wait allowance', async () => {
  const clock = fakeClock(), judge = scriptedJudge([{ route: 'wait' }, { route: 'queue' }]);
  const engine = createAttend({ clock, kind: { reading: 'continuous' }, judge, sink: 'log', settle: { continuation: 'accumulate', waitHorizonMs: 20 } });
  const intake = openIntake(engine, 's'), running = engine.run();
  try {
    await intake.submit(async () => ({ events: [event('a', 0)] }));
    await clock.advance(15);
    await intake.submit(async () => ({ events: [], truncated: true }));
    await clock.advance(15);
    expect(judge.calls[1]?.window.gap).toBe(true);
    expect(judge.calls[1]?.routes.some((r) => r.route === 'wait')).toBe(true);
    expect((await engine.inspect('s'))?.buffer.gap).toBe(false);
  } finally { await intake.close(); await engine.stop(); await running; }
});
