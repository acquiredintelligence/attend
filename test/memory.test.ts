import { describe, expect, test } from 'vitest';
import { memoryLog } from '../src/memory.js';
import { logConformance } from '../src/testing.js';

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
