import { expect, test } from 'vitest';
import { createAttend, memoryStore } from '../src/index.js';
import { openIntake } from '../src/intake.js';
import { fakeClock } from '../src/testing.js';
import type { Event, Window } from '../src/types.js';

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
  const options = { continuation, eventLimits: { counted: 3, background: 2 } };
  expect(() => createAttend({ kind: {}, judge: async () => ({ route: 'skip' }), sink: 'log', settle: options })).toThrow();
});

test('independent limits refuse an explicit scalar', () => {
  const options = { eventLimits: { counted: 3, background: 2 }, maxEvents: 9 };
  expect(() => createAttend({ kind: {}, judge: async () => ({ route: 'skip' }), sink: 'log', settle: { continuation: 'accumulate', ...options } })).toThrow();
});

test('unresolved policy changes reject without a write', async () => {
  const store = memoryStore(), clock = fakeClock();
  const options = { eventLimits: { counted: 3, background: 2 } };
  const first = createAttend({ store, clock, kind: { meta: 'context' }, judge: async () => ({ route: 'skip' }), sink: 'log', settle: { continuation: 'accumulate', ...options } });
  await first.push('s', event('x', 0, 'meta'));
  const before = await store.load('s');
  const second = createAttend({ store, clock, kind: { meta: 'context' }, judge: async () => ({ route: 'skip' }), sink: 'log', settle: { continuation: 'accumulate' } });
  await expect(second.push('s', event('y', 0, 'meta'))).rejects.toThrow('unresolved');
  expect(await store.load('s')).toEqual(before);
});
