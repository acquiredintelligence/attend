import { describe, test, expect } from 'vitest';
import { createHistogram, histogramFromJSON } from '../lib/histogram.mjs';
import { random } from '../lib/workload.mjs';

describe('Histogram', () => {
  test('empty histogram reports zeros', () => {
    const histogram = createHistogram();
    expect(histogram.count).toBe(0);
    expect(histogram.min).toBe(0);
    expect(histogram.max).toBe(0);
    expect(histogram.percentiles()).toEqual({ p50: 0, p99: 0, p999: 0, max: 0 });
  });
  test.each(['uniform', 'log-normal', 'spike', 'large'])('%s nearest ranks, merge and sparse JSON', (distribution) => {
    const rng = random(72);
    const values = Array.from({ length: 10000 }, () => Math.round(distribution === 'uniform' ? rng() * 100000 : distribution === 'log-normal' ? Math.exp(Math.sqrt(-2 * Math.log(1 - rng())) * Math.cos(2 * Math.PI * rng()) + 12) : distribution === 'spike' ? 99999 : 2 ** 20 + rng() * 2 ** 40));
    const histogram = createHistogram();
    const left = createHistogram();
    const right = createHistogram();
    values.forEach((value, i) => { histogram.record(value); (i % 2 ? left : right).record(value); });
    const sorted = values.sort((a, b) => a - b);
    const merged = left.merge(right);
    const copy = histogramFromJSON(JSON.parse(JSON.stringify(histogram)));
    expect(merged.count).toBe(10000);
    expect(copy.count).toBe(10000);
    expect(histogram.min).toBe(sorted[0]);
    for (const p of [0, 1, 10, 25, 50, 75, 90, 99, 99.9, 100]) {
      const exact = sorted[Math.max(0, Math.ceil(sorted.length * p / 100) - 1)];
      expect(Math.abs(histogram.percentile(p) - exact)).toBeLessThanOrEqual(Math.max(1, 0.001 * exact));
      expect(merged.percentile(p)).toBe(histogram.percentile(p));
      expect(copy.percentile(p)).toBe(histogram.percentile(p));
    }
  });
  test('exact small integers and invalid inputs', () => {
    const histogram = createHistogram();
    for (let i = 0; i < 2048; i += 1) histogram.record(i);
    expect(histogram.percentile(50)).toBe(1023);
    expect(histogram.percentile(100)).toBe(2047);
    for (const value of [-1, NaN, Infinity]) expect(() => histogram.record(value)).toThrow();
  });
});
