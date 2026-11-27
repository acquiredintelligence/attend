import { expect, test } from 'vitest';
import { createAttend, memoryStore, type AttendConfig } from '../src/index.js';
import { replay } from '../src/shell/read.js';
import { fakeClock } from '../src/testing.js';

test('delivery retries replay at the committed deadline under a changed writer policy', async () => {
  const clock = fakeClock();
  const store = memoryStore();
  const config = { clock, store, kind: { message: 'flush' }, judge: async () => ({ route: 'note' }) } satisfies Omit<AttendConfig, 'sink'>;
  const schedule = [10];
  const first = createAttend({
    ...config, settle: { retryBackoffMs: [1000], deliveryRetryBackoffMs: schedule },
    sink: () => { throw new Error('store unavailable'); },
  });
  schedule[0] = 500;
  const running = first.run();
  try {
    await first.push('s', { id: 'a', name: 'message' });
    await clock.advance(0);
    expect((await first.inspect('s'))?.outbox.decisions[0]?.phase).toMatchObject({ phase: 'retrying', failures: 1, at: 10 });
    const saved = await store.load('s');
    expect(saved?.snapshot?.policy.deliveryRetryBackoffMs).toEqual([10]);
    expect(saved?.tail.some(({ input }) => input.kind === 'deliveryFailed')).toBe(true);
    if (!saved?.snapshot) throw new Error('expected a saved policy');
    const legacy = { ...saved, snapshot: { ...saved.snapshot, reducer: 7 } };
    expect(replay('s', legacy, { decisions: 'lane', deadLetters: 'log' }).dueAt).toBe(1000);
  } finally {
    await first.stop();
    await running;
  }

  const attempts: number[] = [];
  const second = createAttend({
    ...config, settle: { retryBackoffMs: [1000], deliveryRetryBackoffMs: [50, 60] },
    sink: () => {
      attempts.push(clock.now());
      if (attempts.length === 1) throw new Error('store unavailable');
    },
  });
  const resumed = second.run();
  try {
    await clock.advance(9);
    expect(attempts).toEqual([]);
    await clock.advance(1);
    expect(attempts).toEqual([10]);
    expect((await second.inspect('s'))?.outbox.decisions[0]?.phase).toMatchObject({ phase: 'retrying', failures: 2, at: 70 });
    await clock.advance(60);
    expect(attempts).toEqual([10, 70]);
  } finally {
    await second.stop();
    await resumed;
  }
});

test.each([null, 10, '10', [true], ['10'], [-1], [NaN], [Infinity], Array(1)])('invalid delivery retry schedule %j is refused', (value: unknown) => {
  expect(() => createAttend({
    kind: { message: 'flush' }, judge: async () => ({ route: 'note' }), sink: 'log',
    // @ts-expect-error decoded input can violate the declared type
    settle: { deliveryRetryBackoffMs: value },
  })).toThrow('deliveryRetryBackoffMs');
});
