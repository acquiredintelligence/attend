import { describe, expect, test } from 'vitest';
import { memoryLog, memoryStore } from '../src/memory.js';
import { logConformance, storeConformance } from '../src/testing.js';

describe('memoryStore conformance', () => {
  for (const c of storeConformance(() => memoryStore())) test(c.name, c.run);
});

describe('memoryLog conformance', () => {
  for (const c of logConformance(() => memoryLog(), { canBlock: true })) test(c.name, c.run);
});

describe('memoryLog retention', () => {
  test('keeps the newest entries and read(null) starts at the oldest retained', async () => {
    const log = memoryLog({ retain: 2 });
    await log.append('s', [
      { id: 'a', at: 0, name: 'n', data: {} },
      { id: 'b', at: 0, name: 'n', data: {} },
      { id: 'c', at: 0, name: 'n', data: {} },
    ]);
    expect((await log.read('s', null, { limit: 10 })).map((e) => e.event.id)).toEqual(['b', 'c']);
  });
});

describe('memoryStore isolation', () => {
  test('a stored record is not aliased by the caller', async () => {
    const store = memoryStore();
    const record = { version: 1, scope: 's', cursor: null, seen: ['a'], buffer: { entries: [], dropped: 0, gap: false, forcedAt: null }, batch: null, outbox: [], recent: [], dueAt: null };
    await store.put(record, 0);
    record.seen.push('b');
    expect((await store.get('s'))?.seen).toEqual(['a']);
  });
});
