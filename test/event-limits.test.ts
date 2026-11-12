import { expect, test } from 'vitest';
import { readFileSync } from 'node:fs';
import { createAttend, memoryStore } from '../src/index.js';
import { openIntake } from '../src/intake.js';
import { fakeClock } from '../src/testing.js';
import { apply, newOutput } from '../src/core/reducer.js';
import { assertInvariants, initialState, toRecord } from '../src/core/state.js';
import { DEFAULT_POLICY } from '../src/types.js';
import type { ApplyConfig, Event, Input, ScopeRecord, ScopeWrite, SettlePolicy, StoredScope, Window } from '../src/types.js';

type Answer = 'wait' | 'queue' | 'fail';
interface Offer { at: number; window: Window; wait: boolean }
const event = (id: string, at = 0, name = 'reading'): Event => ({ id, at, observedAt: at, name, data: {}, ...(name === 'reading' ? { quietForMs: 10 } : {}) });

async function run(answers: Answer[], body: (clock: ReturnType<typeof fakeClock>, put: (events: Event[]) => Promise<void>, offers: Offer[], inspect: () => ReturnType<ReturnType<typeof createAttend>['inspect']>) => Promise<void>, options: Record<string, unknown> = {}) {
  const clock = fakeClock(), offers: Offer[] = [];
  const limits = { eventLimits: { counted: 3, background: 2 }, ...options };
  const attend = createAttend({ clock, kind: { reading: 'continuous', meta: 'context', result: 'consequence' }, sink: 'log',
    settle: { continuation: 'accumulate', waitHorizonMs: 20, retryBackoffMs: [30], ...limits },
    judge: async (input) => {
      offers.push({ at: clock.now(), window: structuredClone(input.window), wait: input.routes.some((r) => r.route === 'wait') });
      const answer = answers[offers.length - 1] ?? 'queue';
      if (answer === 'fail') throw new Error('controlled failure');
      return { route: answer };
    },
  });
  const intake = openIntake(attend, 's'), running = attend.run();
  try { await body(clock, (events) => intake.submit(async () => ({ events })), offers, () => attend.inspect('s')); }
  finally { await intake.close(); await attend.stop(); await running; }
}

test('counted equality settles without spending independent background capacity', async () => {
  await run(['queue'], async (_clock, put, offers) => {
    await put([event('x', 0, 'meta'), event('y', 0, 'result')]);
    await put([event('a'), event('b')]);
    expect(offers).toHaveLength(0);
    await put([event('c')]);
    expect(offers.map((o) => [o.window.events.map((e) => e.id), o.wait, o.window.dropped])).toEqual([[['x', 'y', 'a', 'b', 'c'], false, 0]]);
  });
});

test('background retention is FIFO and does not settle alone', async () => {
  await run([], async (_clock, put, offers, inspect) => {
    await put([event('x', 0, 'meta'), event('y', 0, 'result'), event('z', 0, 'meta')]);
    expect(offers).toHaveLength(0);
    const state = await inspect();
    expect(state?.buffer.entries.map((e) => e.id)).toEqual(['y', 'z']);
    expect(state?.buffer.dropped).toBe(1);
  });
});

test.each<Answer>(['wait', 'fail'])('retention spans held and fresh state immediately after %s', async (answer) => {
  await run([answer, 'queue'], async (clock, put, offers, inspect) => {
    await put([event('a'), event('x', 0, 'meta'), event('y', 0, 'result')]);
    await clock.advance(10);
    await put([event('z', 10, 'meta')]);
    const state = await inspect();
    expect(state?.batch?.window.events.map((e) => e.id)).toEqual(['a', 'y']);
    expect(state?.batch?.window.dropped).toBe(1);
    expect(state?.buffer.entries.map((e) => e.id)).toEqual(['z']);
    await clock.advance(answer === 'wait' ? 20 : 30);
    expect(offers[1]?.window.events.map((e) => e.id)).toEqual(['a', 'y', 'z']);
    expect(offers[1]?.window.dropped).toBe(1);
  });
});

test('atomic input may exceed the trigger without splitting or losing counted entries', async () => {
  await run(['queue'], async (_clock, put, offers) => {
    await put([event('a'), event('x', 0, 'meta'), event('b'), event('y', 0, 'result'), event('c'), event('z', 0, 'meta'), event('d')]);
    expect(offers).toHaveLength(1);
    expect(offers[0]?.window.events.map((e) => e.id)).toEqual(['a', 'b', 'y', 'c', 'z', 'd']);
    expect(offers[0]?.wait).toBe(false);
    expect(offers[0]?.window.dropped).toBe(1);
  });
});

test('zero background capacity keeps counted input', async () => {
  await run(['queue'], async (_clock, put, offers) => {
    await put([event('x', 0, 'meta'), event('a'), event('y', 0, 'result')]);
    expect(offers[0]?.window.events.map((e) => e.id)).toEqual(['a']);
    expect(offers[0]?.window.dropped).toBe(2);
  }, { eventLimits: { counted: 1, background: 0 } });
});

test.each<'separate' | undefined>([undefined, 'separate'])('independent limits require explicit accumulate: %s', (continuation) => {
  const options = continuation === undefined ? { eventLimits: { counted: 3, background: 2 } } : { continuation, eventLimits: { counted: 3, background: 2 } };
  expect(() => createAttend({ kind: {}, judge: async () => ({ route: 'skip' }), sink: 'log', settle: options })).toThrow();
});

test('independent limits refuse an explicit scalar', () => {
  const options = { eventLimits: { counted: 3, background: 2 }, maxEvents: 9 };
  expect(() => createAttend({ kind: {}, judge: async () => ({ route: 'skip' }), sink: 'log', settle: { continuation: 'accumulate', ...options } })).toThrow();
});

test.each([undefined, { counted: 4, background: 2 }, { counted: 3, background: 1 }])('unresolved policy changes reject without a write: %j', async (eventLimits) => {
  const store = memoryStore(), clock = fakeClock();
  const options = { eventLimits: { counted: 3, background: 2 } };
  const first = createAttend({ store, clock, kind: { meta: 'context' }, judge: async () => ({ route: 'skip' }), sink: 'log', settle: { continuation: 'accumulate', ...options } });
  await first.push('s', event('x', 0, 'meta'));
  const before = await store.load('s');
  const changed = eventLimits === undefined ? {} : { eventLimits };
  const second = createAttend({ store, clock, kind: { meta: 'context' }, judge: async () => ({ route: 'skip' }), sink: 'log', settle: { continuation: 'accumulate', ...changed } });
  await expect(second.push('s', event('y', 0, 'meta'))).rejects.toThrow('unresolved');
  expect(await store.load('s')).toEqual(before);
});

type Completion = 'judged' | 'judgeFailed' | 'judgeReleased' | 'judgeDeferred';
test.each<Completion>(['judged', 'judgeFailed', 'judgeReleased', 'judgeDeferred'])('direct-writer %s bounds retained state without mutating the live offer', (kind) => {
  const config: ApplyConfig = { policy: { ...DEFAULT_POLICY, continuation: 'accumulate', eventLimits: { counted: 3, background: 2 }, seenCount: 0, retryBackoffMs: [30] }, decisions: 'log', deadLetters: 'log', traces: true };
  const state = initialState('s', config.policy);
  function send(input: Input, now: number) {
    const out = newOutput();
    apply(config, state, { input, now }, out);
    assertInvariants(state, config.policy);
    return out;
  }
  send({ kind: 'events', events: [{ ...event('a'), kind: 'continuous' }, { ...event('x', 0, 'meta'), kind: 'context' }, { ...event('y', 0, 'result'), kind: 'consequence' }] }, 0);
  const started = send({ kind: 'tick' }, 10).effects.find((e) => e.kind === 'judge');
  if (!started || started.kind !== 'judge') throw new Error('expected a live offer');
  const original = structuredClone(started.window);
  send({ kind: 'events', truncated: true, events: [{ ...event('z', 11, 'meta'), kind: 'context' }, { ...event('w', 11, 'result'), kind: 'consequence' }, { ...event('v', 11, 'meta'), kind: 'context' }] }, 11);
  expect(started.window).toEqual(original);
  const token = { batchId: original.id, attempt: started.attempt };
  let completion: Input;
  if (kind === 'judged') completion = { kind, ...token, judgment: { route: 'wait' } };
  else if (kind === 'judgeFailed') completion = { kind, ...token, message: 'controlled', permanent: false };
  else if (kind === 'judgeDeferred') completion = { kind, ...token, until: 42 };
  else completion = { kind, ...token };
  send(completion, 12);
  expect(started.window).toEqual(original);
  const record = toRecord(state);
  expect(record.batch?.window.events.map((e) => e.id)).toEqual(['a']);
  expect(record.batch?.window.dropped).toBe(2);
  expect(record.buffer.entries.map((e) => e.id)).toEqual(['w', 'v']);
  expect(record.buffer.dropped).toBe(1);
  expect(record.buffer.gap).toBe(true);
  expect([...state.known]).toEqual([['a', 1], ['w', 1], ['v', 1]]);
  expect(record.batch?.window.id).not.toBe(original.id);
});

test.each([null, [], {}, { counted: 0, background: 1 }, { counted: true, background: 1 }, { counted: 1.5, background: 1 }, { counted: 1, background: -1 }, { counted: 1, background: Infinity }, { counted: 2 ** 53, background: 0 }, { counted: 1 }, { counted: 1, background: 0, extra: 1 }])('invalid independent limits are refused: %j', (eventLimits) => {
  expect(() => Reflect.apply(createAttend, undefined, [{ kind: {}, judge: async () => ({ route: 'skip' }), sink: 'log', settle: { continuation: 'accumulate', eventLimits } }])).toThrow();
});

test('mutating the supplied pair cannot alter the live policy', async () => {
  const eventLimits = { counted: 2, background: 1 };
  await run(['queue'], async (_clock, put, offers) => {
    eventLimits.counted = 9;
    await put([event('a'), event('b')]);
    expect(offers).toHaveLength(1);
    expect(offers[0]?.wait).toBe(false);
  }, { eventLimits });
});

test('native deduplication happens before counted equality', async () => {
  await run(['queue'], async (_clock, put, offers) => {
    await put([event('a'), event('a'), event('b')]);
    expect(offers).toHaveLength(0);
    await put([event('c')]);
    expect(offers[0]?.window.events.map((e) => e.id)).toEqual(['a', 'b', 'c']);
  });
});

interface Legacy { name: string; now: number; writes: ScopeWrite[][]; stored: StoredScope; expected: ScopeRecord }
const legacy: { cases: Legacy[] } = JSON.parse(readFileSync(new URL('../conformance/fixtures/event-limits-legacy.json', import.meta.url), 'utf8'));
test.each(legacy.cases)('revision-four $name snapshot and tail retain their saved policy', async (item) => {
  const store = memoryStore(), clock = fakeClock(item.now);
  for (const writes of item.writes) expect(await store.commit(writes)).toEqual(writes.map(() => 'ok'));
  expect(await store.load('s')).toEqual(item.stored);
  expect(item.stored.snapshot?.reducer).toBe(4);
  expect(item.stored.tail.length).toBeGreaterThan(0);
  const engine = createAttend({ store, clock, kind: { meta: 'context' }, judge: async () => ({ route: 'queue' }), sink: 'log',
    settle: { continuation: 'accumulate', eventLimits: { counted: 3, background: 2 } },
  });
  expect(await engine.inspect('s')).toEqual(item.expected);
  const write = engine.push('s', event('next', item.now, 'meta'));
  if (item.name === 'complete') {
    await write;
    const stored = await store.load('s');
    expect(stored?.snapshot?.reducer).toBe(5);
    expect(stored?.snapshot?.policy.eventLimits).toEqual({ counted: 3, background: 2 });
    expect((await engine.inspect('s'))?.recent).toEqual(item.expected.recent);
  } else {
    await expect(write).rejects.toThrow('unresolved');
    expect(await store.load('s')).toEqual(item.stored);
  }
});

test.each<Answer>(['wait', 'fail'])('same independent policy resumes persisted %s state', async (answer) => {
  const clock = fakeClock(), store = memoryStore();
  const settle = { continuation: 'accumulate', eventLimits: { counted: 3, background: 2 }, waitHorizonMs: 20, retryBackoffMs: [30] } satisfies Partial<SettlePolicy>;
  const first = createAttend({ store, clock, kind: { reading: 'continuous', meta: 'context' }, sink: 'log', settle,
    judge: async () => { if (answer === 'fail') throw new Error('controlled'); return { route: answer }; },
  });
  const intake = openIntake(first, 's'), run = first.run();
  await intake.submit(async () => ({ events: [event('a'), event('x', 0, 'meta'), event('y', 0, 'meta')] }));
  await clock.advance(15);
  await intake.submit(async () => ({ events: [event('z', 15, 'meta')] }));
  await intake.close(); await first.stop(); await run;
  const before = await first.inspect('s');
  const seen: string[][] = [];
  const second = createAttend({ store, clock, kind: {}, sink: 'log', settle, judge: async ({ window }) => { seen.push(window.events.map((e) => e.id)); return { route: 'queue' }; } });
  const running = second.run();
  try {
    await second.push('s', []);
    expect(await second.inspect('s')).toEqual(before);
    await clock.advance(25);
    expect(seen).toEqual([['a', 'y', 'z']]);
  } finally { await second.stop(); await running; }
});
