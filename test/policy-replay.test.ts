import { expect, test } from 'vitest';
import { createAttend, memoryStore } from '../src/index.js';
import { fakeClock } from '../src/testing.js';

test('a changed writer policy cannot reinterpret an already completed tail', async () => {
  const store = memoryStore(), clock = fakeClock();
  const first = createAttend({ store, clock, kind: { reading: 'continuous' }, judge: async () => ({ route: 'queue' }), sink: 'log', settle: { streamGapMs: 10 } });
  const running = first.run();
  await first.push('s', { id: 'a', name: 'reading', at: 0, data: {} });
  await clock.advance(10);
  await first.stop(); await running;
  expect((await store.load('s'))?.tail.length).toBeGreaterThan(0);
  expect((await first.inspect('s'))?.batch).toBeNull();
  expect((await first.inspect('s'))?.buffer.entries).toEqual([]);
  const second = createAttend({ store, clock, kind: { reading: 'continuous' }, judge: async () => ({ route: 'queue' }), sink: 'log', settle: { streamGapMs: 100 } });
  await second.push('s', { id: 'b', name: 'reading', at: 10, data: {} });
  expect((await second.inspect('s'))?.buffer.entries.map((e) => e.id)).toEqual(['b']);
});
