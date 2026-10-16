import { test, expect } from 'vitest';
import { allowedCpus, parseCpus, runnerInfo, taskset, finishRunner } from '../lib/runner.mjs';
import { Runner } from '../lib/schema.mjs';

test('CPU lists expand, sort and reject malformed ranges', () => {
  expect(parseCpus('4,1-3,2,7')).toEqual([1, 2, 3, 4, 7]);
  expect(() => parseCpus('4-1')).toThrow('invalid CPU list');
  expect(() => parseCpus('cpu')).toThrow('invalid CPU list');
});
test('Runner identifies the machine without a network host name and retains peak load', () => {
  const info = runnerInfo();
  expect(Runner.safeParse(info).success).toBe(true);
  expect(info.host).toMatch(/^[^/]+\/[^ ]+ .+ x\d+, node v\d+/);
  expect(finishRunner({ ...info, loadAverage: 999999 }).loadAverage).toBe(999999);
  if (process.platform !== 'linux') {
    expect(info.pinned).toBe(false);
    expect(allowedCpus()).toBe(null);
    expect(() => taskset('1', ['node'])).toThrow('CPU pinning requires Linux');
  } else {
    expect(allowedCpus().length).toBeGreaterThan(0);
    expect(taskset('1-2', ['node', 'file'])).toEqual(['taskset', '-c', '1-2', 'node', 'file']);
  }
  expect(taskset(undefined, ['node', 'file'])).toEqual(['node', 'file']);
});
