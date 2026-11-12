import { expect, test } from 'vitest';
import { createAttend } from '../src/index.js';
import { fakeClock, scriptedJudge } from '../src/testing.js';

test.each([Symbol('time'), () => 100, { toJSON: () => undefined }])('an observation that JSON would omit still refuses the batch: %s', async (observedAt) => {
  const attend = createAttend({ clock: fakeClock(200), kind: () => 'continuous', judge: scriptedJudge([{ route: 'queue' }]), sink: 'log' });
  try {
    // JavaScript input is not restricted by the declared numeric field.
    // @ts-expect-error invalid observation is the boundary under test
    await expect(attend.push('s', [{ id: 'a', name: 'move' }, { id: 'b', name: 'move', observedAt }])).rejects.toThrow('observedAt');
    expect(await attend.inspect('s')).toBeNull();
  } finally {
    await attend.stop();
  }
});

test('a root toJSON result cannot erase its invalid observation', async () => {
  let calls = 0;
  const event = { id: 'a', name: 'move', toJSON() { calls += 1; return { id: 'a', name: 'move', observedAt: Symbol('time') }; } };
  const attend = createAttend({ clock: fakeClock(200), kind: () => 'continuous', judge: scriptedJudge([{ route: 'queue' }]), sink: 'log' });
  try {
    await expect(attend.push('s', event)).rejects.toThrow('observedAt');
    expect(calls).toBe(1);
    expect(await attend.inspect('s')).toBeNull();
  } finally {
    await attend.stop();
  }
});

test('an observation getter is read once before refusing its value', async () => {
  let reads = 0;
  const event = { id: 'a', name: 'move', get observedAt() { reads += 1; return reads === 1 ? Symbol('time') : 100; } };
  const attend = createAttend({ clock: fakeClock(200), kind: () => 'continuous', judge: scriptedJudge([{ route: 'queue' }]), sink: 'log' });
  try {
    // @ts-expect-error a getter may return a nonnumeric value at runtime
    await expect(attend.push('s', event)).rejects.toThrow('observedAt');
    expect(reads).toBe(1);
    expect(await attend.inspect('s')).toBeNull();
  } finally {
    await attend.stop();
  }
});

test('timing names nested in data retain ordinary JSON normalization', async () => {
  const attend = createAttend({ clock: fakeClock(200), kind: () => 'continuous', judge: scriptedJudge([{ route: 'queue' }]), sink: 'log' });
  try {
    // @ts-expect-error the JSON boundary intentionally omits callable data properties
    await attend.push('s', { id: 'a', name: 'move', observedAt: 100, data: { observedAt: () => 100, quietForMs: Symbol('duration'), value: 1 } });
    expect((await attend.inspect('s'))?.buffer.entries[0]?.data).toEqual({ value: 1 });
  } finally {
    await attend.stop();
  }
});
