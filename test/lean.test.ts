import { expect, test } from 'vitest';
import { createAttend } from '../src/attend.js';
import { PermanentError } from '../src/errors.js';
import { memoryStore } from '../src/memory.js';
import { fakeClock } from '../src/testing.js';
import type { Decision } from '../src/types.js';

const judge = async ({ window }: { window: { reason: string } }) => ({ route: window.reason === 'flush' ? ('interrupt' as const) : ('note' as const) });

test('a config with no store or deadLetter, a kind map and a function sink delivers single events pushed without ids', async () => {
  const clock = fakeClock();
  const delivered: Decision[] = [];
  const attend = createAttend({ clock, judge, kind: { 'user.message': 'flush', 'build.failed': 'closing' }, sink: (decision) => { delivered.push(decision); } });
  const running = attend.run();
  await attend.push('session-1', { name: 'build.failed', text: 'Build 4521 failed: 1 test' });
  await clock.advance(100);
  await attend.push('session-1', { name: 'user.message', text: 'can you look?' });
  await clock.advance(1);
  await attend.stop();
  await running;

  expect(delivered.map((decision) => [decision.route, decision.window.reason, decision.window.events.map((event) => event.name)])).toEqual([
    ['interrupt', 'flush', ['build.failed', 'user.message']],
  ]);
  const [failed, message] = delivered[0]!.window.events;
  expect(failed!.id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
  expect(failed!.id).not.toBe(message!.id);
  expect([failed!.at, message!.at, failed!.data, message!.data]).toEqual([0, 100, {}, {}]);
});

test('a kind map gives unlisted names consequence, and an unknown kind is rejected at construction', async () => {
  expect(() => createAttend({ judge, kind: { 'user.message': 'shout' as never }, sink: () => {} })).toThrow(RangeError);
  expect(() => createAttend({ judge, kind: [] as never, sink: () => {} })).toThrow(TypeError);

  const clock = fakeClock();
  const delivered: Decision[] = [];
  const attend = createAttend({ clock, judge, kind: { 'user.message': 'flush' }, sink: (decision) => { delivered.push(decision); } });
  const running = attend.run();
  await attend.push('s', { name: 'log.line', text: 'pnpm test' });
  await clock.advance(10_000);
  expect(delivered).toEqual([]);
  await attend.push('s', { name: 'user.message', text: 'any news?' });
  await clock.advance(1);
  await attend.stop();
  await running;
  expect(delivered.map((decision) => decision.window.events.map((event) => event.name))).toEqual([['log.line', 'user.message']]);
});

test('push takes one event or a list, and an explicit id drops a redelivery', async () => {
  const clock = fakeClock();
  const delivered: Decision[] = [];
  const attend = createAttend({ clock, judge, kind: { 'user.message': 'flush' }, sink: (decision) => { delivered.push(decision); } });
  const running = attend.run();
  await attend.push('s', [{ id: 'm1', name: 'user.message', text: 'hello' }]);
  await clock.advance(1);
  await attend.push('s', { id: 'm1', name: 'user.message', text: 'hello' });
  await clock.advance(1);
  await attend.stop();
  await running;
  expect(delivered.map((decision) => decision.window.events.map((event) => event.id))).toEqual([['m1']]);
});

test('event data that is not a JSON object is still rejected', async () => {
  const attend = createAttend({ judge, kind: {}, sink: () => {} });
  await expect(attend.push('s', { name: 'x', data: 5 as never })).rejects.toThrow('event data must be a JSON object');
  await expect(attend.push('s', null as never)).rejects.toThrow('events must be an event or an array of events');
});

test('without a deadLetter handler, a dead letter is written to the decision log', async () => {
  const clock = fakeClock();
  const store = memoryStore();
  const attend = createAttend({ clock, store, kind: { 'user.message': 'flush' }, sink: () => {}, judge: async () => { throw new PermanentError('refused'); } });
  const running = attend.run();
  await attend.push('s', { id: 'm1', name: 'user.message', text: 'hello' });
  await clock.advance(1);
  await attend.stop();
  await running;
  const page = await store.log.read({ after: null, limit: 10, kinds: ['deadLetter'] });
  expect(page.rows.map((row) => [row.kind, row.scope])).toEqual([['deadLetter', 's']]);
});

test('run() without a signal lasts until stop(), stop() resolves when idle, and a stopped instance runs again', async () => {
  const clock = fakeClock();
  const delivered: string[] = [];
  const attend = createAttend({ clock, judge, kind: { 'user.message': 'flush' }, sink: (decision) => { delivered.push(decision.route); } });
  await attend.stop();
  const first = attend.run();
  await expect(attend.run()).rejects.toThrow('attend: already running');
  await attend.stop();
  await first;
  const second = attend.run();
  await attend.push('s', { name: 'user.message', text: 'back again' });
  await clock.advance(1);
  await attend.stop();
  await second;
  expect(delivered).toEqual(['interrupt']);
});

test('stop() also ends a run that was given its own signal', async () => {
  const attend = createAttend({ judge, kind: {}, sink: () => {} });
  const controller = new AbortController();
  const running = attend.run({ signal: controller.signal });
  await attend.stop();
  await running;
  expect(controller.signal.aborted).toBe(false);
});
