import { describe, expect, test } from 'vitest';
import { memoryLog, memoryStore } from '../src/memory.js';
import { initialRecord } from '../src/step.js';
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
  test('a stored record cannot be changed through the reference that was put', async () => {
    const store = memoryStore();
    const record = { ...initialRecord('s'), version: 1, seen: ['a'], buffer: { entries: [], dropped: 0, gap: false, forcedAt: null } };
    await store.put(record, 0);
    expect(() => record.seen.push('b')).toThrow(TypeError);
    expect(() => {
      record.buffer.gap = true;
    }).toThrow(TypeError);
    expect((await store.get('s'))?.seen).toEqual(['a']);
  });

  test('a record returned by get cannot be changed', async () => {
    const store = memoryStore();
    await store.put({ ...initialRecord('s'), version: 1, seen: ['a'] }, 0);
    const got = await store.get('s');
    expect(() => got?.seen.push('b')).toThrow(TypeError);
    expect((await store.get('s'))?.seen).toEqual(['a']);
  });
});
