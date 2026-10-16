import { test, expect } from 'vitest';
import { mkdtemp, writeFile, readFile, rm } from 'node:fs/promises';
import os from 'node:os';
import { join } from 'node:path';
import { createSchedule, eventLine, benchEvent, EVENT_BYTES } from '../lib/workload.mjs';

test('Fixed-rate schedule, event wire format and counts are exact', () => {
  const schedule = createSchedule({ mix: 'continuous', rate: 10000, scopes: 1000, seconds: 2, warmup: 0.2 });
  expect(schedule.total).toBe(22000);
  expect(schedule.warmCount).toBe(2000);
  expect(schedule.intendedMs(1234)).toBe(123.4);
  expect(schedule.scopeOf(1234)).toBe(234);
  expect(schedule.kindOf(1234)).toBe('continuous');
  expect(eventLine(1, 2, 'continuous', 100)).toBe('{"scope":"s2","id":"e1","at":100,"name":"bench.continuous","data":{"seq":1,"x":1,"y":2,"value":0.42},"text":"continuous 1"}\n');
  expect(JSON.parse(eventLine(1, 2, 'continuous', 100))).toEqual({ scope: 's2', ...benchEvent(1, 'continuous', 100) });
  expect(EVENT_BYTES).toBe(120);
});
test('Realistic schedule is seeded, has timed closings and consequence bursts', () => {
  const options = { mix: 'realistic', rate: 10000, scopes: 1000, seconds: 60, warmup: 0, seed: 17 };
  const schedule = createSchedule(options);
  const copy = createSchedule(options);
  const different = createSchedule({ ...options, seed: 18 });
  const a = Array.from({ length: schedule.total }, (_, i) => schedule.kindOf(i));
  expect(a).toEqual(Array.from({ length: copy.total }, (_, i) => copy.kindOf(i)));
  expect(a).not.toEqual(Array.from({ length: different.total }, (_, i) => different.kindOf(i)));
  let sum = 0;
  let count = 0;
  for (let s = 0; s < 1000; s += 1) {
    let previous;
    for (let i = s; i < schedule.total; i += 1000) {
      if (schedule.kindOf(i) !== 'closing') continue;
      const time = schedule.intendedMs(i);
      if (previous !== undefined) { expect(time - previous).toBeGreaterThanOrEqual(1000); sum += time - previous; count += 1; }
      previous = time;
      let burst = 0;
      let j = i + 1000;
      while (j < schedule.total && schedule.kindOf(j) === 'consequence') { burst += 1; j += 1000; }
      if (j < schedule.total) { expect(burst).toBeGreaterThanOrEqual(2); expect(burst).toBeLessThanOrEqual(7); }
    }
  }
  expect(count).toBeGreaterThan(1000);
  expect(sum / count).toBeGreaterThanOrEqual(1800);
  expect(sum / count).toBeLessThanOrEqual(2200);
});
test('Synthetic tape merges sorted scope offsets and derives its rate', async () => {
  const dir = await mkdtemp(join(os.tmpdir(), 'ab-tape-'));
  try {
    const file = join(dir, 'sample.json');
    await writeFile(file, JSON.stringify({ tapes: [{ name: 'sample', events: [{ t: 0, kind: 'continuous' }, { t: 200, kind: 'closing' }] }] }));
    const tapes = JSON.parse(await readFile(file, 'utf8'));
    const schedule = createSchedule({ mix: 'tape', rate: 99, scopes: 2, seconds: 2, warmup: 1, tapes });
    const expected = [
      [0, 0, 'continuous'], [200, 0, 'closing'], [0.6180339887 * 1200, 1, 'continuous'], [0.6180339887 * 1200 + 200, 1, 'closing'],
      [1200, 0, 'continuous'], [1400, 0, 'closing'], [0.6180339887 * 1200 + 1200, 1, 'continuous'], [0.6180339887 * 1200 + 1400, 1, 'closing'],
      [2400, 0, 'continuous'], [2600, 0, 'closing'],
    ];
    expect(schedule.total).toBe(10);
    expect(schedule.warmCount).toBe(4);
    expect(schedule.targetRate).toBe(3);
    for (const [i, [time, scope, kind]] of expected.entries()) {
      expect(schedule.intendedMs(i)).toBeCloseTo(time, 8);
      expect(schedule.scopeOf(i)).toBe(scope);
      expect(schedule.kindOf(i)).toBe(kind);
    }
  } finally { await rm(dir, { recursive: true, force: true }); }
});
