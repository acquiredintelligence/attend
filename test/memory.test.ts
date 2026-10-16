import { describe, expect, test } from 'vitest';
import { memoryLog, memoryStore } from '../src/memory.js';
import { logConformance, storeConformance } from '../src/testing.js';
import { aliasingCases } from './support/aliasing.js';

describe('memoryStore conformance', () => {
  for (const c of storeConformance(() => memoryStore())) test(c.name, c.run);
});

describe('aliasing through memoryStore', () => {
  for (const c of aliasingCases(() => memoryStore())) test(c.name, c.run);
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
