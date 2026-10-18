import { test, expect } from 'vitest';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import os from 'node:os';
import { join } from 'node:path';
import { validateReport } from '../lib/schema.mjs';
import { openLoopReport } from '../lib/openloop.mjs';
import { createHistogram } from '../lib/histogram.mjs';
import { openReport, allocReport, durableReport, latencyReport, serveReport, cli } from './helpers.mjs';

test('Schemas accept one complete fixture per report', () => {
  for (const fixture of [openReport(), allocReport(), durableReport(), latencyReport(), serveReport()]) expect(validateReport(fixture)).toEqual({ ok: true, report: fixture });
});
test('Serve reports order p90 between p50 and p99', () => {
  const result = validateReport(serveReport({ commitToSubscriberUs: { p50: 10, p90: 30, p99: 20, max: 40 } }));
  expect(result.ok).toBe(false);
  expect(result.errors.join('\n')).toContain('commitToSubscriberUs.p99');
});
test('Open-loop reports separate unacknowledged lines, rejections and floor evidence', () => {
  const histogram = createHistogram();
  histogram.record(7);
  const report = openLoopReport({
    run: { achievedRate: 42, seconds: 60, latency: histogram, first: histogram, last: histogram, failed: 2, rejected: 3, generatorMaxLagMs: 0 },
    stats: { engine: histogram.toJSON(), rejected: 99, wallMs: 0, gcMs: 0, maxLagMs: 0, windows: 0, meanWindow: 0, p99Window: 0, port: 'state', adapterVersion: 'test' },
    runner: openReport().runner,
  }, { schedule: { targetRate: 50 }, scopes: 1000, mix: 'continuous', floor: { latency: histogram, achievedRate: 49, failed: 4, rejected: 5 } });
  expect(report).toMatchObject({ failed: 2, rejected: 3, noopFloorRate: 49, noopFloorFailed: 9, noopFloorUs: { p50: 7, p99: 7, p999: 7, max: 7 } });
  expect(validateReport(report)).toEqual({ ok: true, report });
});
test.each(['rejected', 'noopFloorRate', 'noopFloorFailed'])('Open-loop schema requires a finite nonnegative %s', (field) => {
  for (const value of [undefined, -1, Infinity]) {
    const result = validateReport(openReport({ [field]: value }));
    expect(result.ok).toBe(false);
    expect(result.errors.join('\n')).toContain(field);
  }
});
test('Schemas reject missing, extra, enum, negative, percentile and durability fields with paths', () => {
  const missing = openReport();
  delete missing.scopes;
  const fixtures = [
    [missing, 'scopes'], [openReport({ extra: true }), 'extra'], [openReport({ mix: 'invalid' }), 'mix'],
    [allocReport({ bytesPerEvent: -1 }), 'bytesPerEvent'], [latencyReport({ samples: Infinity }), 'samples'],
    [openReport({ latencyUs: { p50: 20, p99: 10 } }), 'latencyUs.p99'],
    [openReport({ engineUs: { p50: 1, p99: 2, p999: 1 } }), 'engineUs.p999'],
    [durableReport({ durability: { store: 'redis', appendfsync: 'always' } }), 'durability.appendonly'],
    [durableReport({ durability: { store: 'sqlite', journalMode: 'WAL', synchronous: 'EXTRA', worker: false } }), 'durability.synchronous'],
  ];
  for (const [fixture, field] of fixtures) {
    const result = validateReport(fixture);
    expect(result.ok).toBe(false);
    expect(result.errors.join('\n')).toContain(field);
  }
});
test('validate CLI reports non-JSON lines and exits one', async () => {
  const dir = await mkdtemp(join(os.tmpdir(), 'ab-test-'));
  try {
    const file = join(dir, 'reports.jsonl');
    await writeFile(file, JSON.stringify(openReport()) + '\nnot JSON\n');
    const result = await cli('validate', [file]);
    expect(result.code).toBe(1);
    expect(result.stdout).toContain('2 reports, 1 invalid');
    expect(result.stderr).toContain(':2: invalid JSON');
  } finally { await rm(dir, { recursive: true, force: true }); }
});
