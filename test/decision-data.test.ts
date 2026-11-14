import { expect, test } from 'vitest';
import { createAttend } from '../src/attend.js';
import { memoryStore } from '../src/memory.js';
import { fakeClock, recordingSink } from '../src/testing.js';
import type { Decision, JsonObject, JsonValue } from '../src/types.js';

test('decision data survives a failed delivery and a retained-store restart', async () => {
  const clock = fakeClock();
  const store = memoryStore();
  const data = { revision: 7, tags: ['alpha'] };
  const attempts: Decision[] = [];
  const first = createAttend({
    clock, store, kind: { change: 'closing' }, settle: { closingGraceMs: 1, retryBackoffMs: [10] },
    judge: async () => ({ route: 'note', data }),
    sink: async (decision) => {
      attempts.push(structuredClone(decision));
      if (decision.data && typeof decision.data === 'object' && !Array.isArray(decision.data)) decision.data.revision = 99;
      throw new Error('retry');
    },
  });
  const firstRun = first.run();
  try {
    await first.push('scope', { id: 'a', name: 'change', at: 0, data: {} });
    await clock.advance(1);
    expect(attempts).toHaveLength(1);
    expect(attempts[0]?.data).toEqual({ revision: 7, tags: ['alpha'] });
    data.tags.push('changed after judgment');
    expect((await first.inspect('scope'))?.recent[0]?.data).toEqual({ revision: 7, tags: ['alpha'] });
  } finally {
    await first.stop();
    await firstRun;
  }

  const sink = recordingSink();
  const recent: JsonValue[] = [];
  const second = createAttend({
    clock, store, kind: { change: 'closing' }, settle: { closingGraceMs: 1, retryBackoffMs: [10] }, sink,
    judge: async (input) => {
      const prior = input.recent[0]?.data;
      if (prior !== undefined) recent.push(structuredClone(prior));
      if (prior && typeof prior === 'object' && !Array.isArray(prior)) prior.revision = 100;
      return { route: 'note' };
    },
  });
  const secondRun = second.run();
  try {
    await clock.advance(10);
    expect(sink.delivered).toHaveLength(1);
    expect(sink.delivered[0]?.id).toBe(attempts[0]?.id);
    expect(sink.delivered[0]?.data).toEqual({ revision: 7, tags: ['alpha'] });
    await second.push('scope', { id: 'b', name: 'change', at: 10, data: {} });
    await clock.advance(1);
    expect(recent).toEqual([{ revision: 7, tags: ['alpha'] }]);
    expect((await second.inspect('scope'))?.recent[0]?.data).toEqual({ revision: 7, tags: ['alpha'] });
  } finally {
    await second.stop();
    await secondRun;
  }
});

const values: JsonValue[] = [null, false, 0, '', [], {}];
test.each(values.map((data) => ({ data })))('keeps explicit JSON data $data', async ({ data }) => {
  const clock = fakeClock();
  const sink = recordingSink();
  const attend = createAttend({ clock, kind: { change: 'closing' }, settle: { closingGraceMs: 1 }, sink, judge: async () => ({ route: 'note', data }) });
  const running = attend.run();
  try {
    await attend.push('scope', { id: 'a', name: 'change', at: 0, data: {} });
    await clock.advance(1);
    expect(sink.delivered[0]).toHaveProperty('data', data);
    expect((await attend.inspect('scope'))?.recent[0]).toHaveProperty('data', data);
  } finally {
    await attend.stop();
    await running;
  }
});

test('invalid JSON data retries the judgment before a decision is committed', async () => {
  const clock = fakeClock();
  const sink = recordingSink();
  let calls = 0;
  const invalid: JsonObject = {};
  Reflect.set(invalid, 'value', 1n);
  const attend = createAttend({
    clock, kind: { change: 'closing' }, sink, settle: { closingGraceMs: 1, retryBackoffMs: [10] },
    judge: async () => ({ route: 'note', data: ++calls === 1 ? invalid : { valid: true } }),
  });
  const running = attend.run();
  try {
    await attend.push('scope', { id: 'a', name: 'change', at: 0, data: {} });
    await clock.advance(1);
    expect(sink.delivered).toHaveLength(0);
    await clock.advance(10);
    expect(calls).toBe(2);
    expect(sink.delivered[0]?.data).toEqual({ valid: true });
  } finally {
    await attend.stop();
    await running;
  }
});
