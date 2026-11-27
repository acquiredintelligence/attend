import { expect, test } from 'vitest';
import { createAttend, memoryStore, type SourceBatch } from '../src/index.js';
import { fakeClock, scriptedJudge } from '../src/testing.js';

test('closing waits for the latest explicit or default deadline', async () => {
  const attend = createAttend({ clock: fakeClock(100), kind: { save: 'closing' }, judge: scriptedJudge([]), sink: 'log' });
  try {
    await attend.push('s', [
      { id: 'long', name: 'save', observedAt: 10, quietForMs: 2000 },
      { id: 'default', name: 'save' },
      { id: 'short', name: 'save', observedAt: 80, quietForMs: 20 },
    ]);
    expect((await attend.inspect('s'))?.dueAt).toBe(2010);
  } finally {
    await attend.stop();
  }
});

test('an attached completion settles a live stream and replays with its cursor', async () => {
  const clock = fakeClock(100);
  const store = memoryStore();
  const judge = scriptedJudge([{ route: 'note' }]);
  const config = { clock, store, judge, sink: 'log', kind: { save: 'closing', move: 'continuous', out: 'consequence' } } as const;
  const writer = createAttend(config);
  const completion: SourceBatch = {
    events: [{ id: 'done', at: 0, name: 'out', data: {}, observedAt: 80, settles: true }],
    cursor: '2',
  };
  try {
    await writer.push('s', [
      { id: 'act', name: 'save', quietForMs: 2000 },
      { id: 'stream', name: 'move', quietForMs: 2000 },
    ]);
    const controller = new AbortController();
    await writer.attach('s', {
      key: 'feed',
      async subscribe(_cursor, emit) {
        try { await emit(completion); }
        finally { controller.abort(); }
      },
    }, { signal: controller.signal });
    const stored = await store.load('s');
    expect(stored?.snapshot?.record.buffer.entries).toMatchObject([{ id: 'act', quietForMs: 2000 }, { id: 'stream' }]);
    expect(stored?.tail).toMatchObject([{ input: { kind: 'events', events: [{ id: 'done', settles: true }], cursor: '2' } }]);
  } finally {
    await writer.stop();
  }

  const reader = createAttend(config);
  expect(await reader.inspect('s')).toMatchObject({
    dueAt: 80, cursors: { feed: '2' }, buffer: { endedAt: 80 },
  });
  const running = reader.run();
  try {
    await clock.advance(0);
    expect(judge.calls).toHaveLength(1);
    expect(judge.calls[0]).toMatchObject({
      routes: expect.arrayContaining([{ route: 'wait' }]),
      window: { reason: 'closing', settledAt: 100, events: [{ quietForMs: 2000 }, { id: 'stream' }, { settles: true }] },
    });
  } finally {
    await reader.stop();
    await running;
  }
});

test.each([false, null, 1, 'true', Symbol('settles'), { toJSON: () => undefined }])('invalid settles value %s refuses the whole batch', async (settles) => {
  const attend = createAttend({ clock: fakeClock(100), kind: { save: 'closing', out: 'consequence' }, judge: scriptedJudge([]), sink: 'log' });
  try {
    // @ts-expect-error decoded input can violate the declared type
    await expect(attend.push('s', [{ name: 'save' }, { name: 'out', settles }])).rejects.toThrow('settles');
    expect(await attend.inspect('s')).toBeNull();
  } finally {
    await attend.stop();
  }
});
