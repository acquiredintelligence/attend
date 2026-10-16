import { test, expect } from 'vitest';
import os from 'node:os';
import { join } from 'node:path';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { evaluateGates } from '../lib/gates.mjs';
import { openReport, allocReport, durableReport, latencyReport, percentiles, cli } from './helpers.mjs';

const gate = (reports, slo) => evaluateGates(reports).find((result) => result.slo === slo);
const openSet = () => ['continuous', 'realistic'].flatMap((mix) => [10000, 30000, 50000].flatMap((targetRate) => Array.from({ length: 3 }, () => openReport({ mix, targetRate, achievedRate: targetRate }))));
const grid = () => ['reducer', 'endToEnd'].flatMap((layer) => [10, 100, 250, 1000].flatMap((window) => [0, 50, 500, 2000].map((seenCount) => allocReport({ layer, window, seenCount }))));
const stores = () => [durableReport(), durableReport({ durability: { store: 'postgres', synchronousCommit: 'on' } }), durableReport({ durability: { store: 'redis', appendonly: true, appendfsync: 'everysec' } })];
const reaction = () => [latencyReport(), latencyReport({ load: { mix: 'realistic', targetRate: 50000, achievedRate: 50000, scopes: 1000 } }), latencyReport({ durability: durableReport().durability, commitLatencyMs: percentiles() })];

test('Gates report all five SLOs as not verified without evidence', () => {
  expect(evaluateGates([]).map(({ slo, status }) => ({ slo, status }))).toEqual([1, 2, 3, 4, 5].map((slo) => ({ slo, status: 'not verified' })));
});
test('SLO 1 checks both mixes, all runs, rate, p50 growth, failures, CPU and knee', () => {
  const reports = openSet();
  expect(gate(reports, 1)).toMatchObject({ status: 'met', info: { knee: { continuous: 50000, realistic: 50000 } } });
  for (const changes of [{ achievedRate: 49900 }, { p50LastUs: 121 }, { failed: 1 }, { attendCores: 1.01 }]) {
    expect(gate(reports.map((r, i) => i === 6 ? { ...r, ...changes } : r), 1).status).toBe('not met');
  }
  expect(gate(reports.filter((r, i) => i !== 6), 1).status).toBe('not verified');
  expect(gate(reports.map((r) => ({ ...r, seconds: 15 })), 1).status).toBe('not verified');
  expect(gate(reports.map((r) => ({ ...r, scopes: 50 })), 1).status).toBe('not verified');
});
test('SLO 2 requires three pinned runs per rate and mix and both p99 limits', () => {
  const reports = openSet();
  expect(gate(reports, 2).status).toBe('met');
  expect(gate(reports.map((r) => ({ ...r, runner: { ...r.runner, pinned: false } })), 2).status).toBe('not verified');
  expect(gate(reports.map((r, i) => i === 0 ? { ...r, engineUs: { p50: 1, p99: 101 } } : r), 2).status).toBe('not met');
  expect(gate(reports.map((r, i) => i === 0 ? { ...r, latencyUs: { p50: 1, p99: 121 } } : r), 2).status).toBe('not met');
  expect(gate(reports.slice(1), 2).status).toBe('not verified');
});
test('SLO 3 checks complete grids, row and column medians and end-to-end bytes', () => {
  const reports = grid();
  expect(gate(reports, 3).status).toBe('met');
  expect(gate(reports.slice(1), 3).status).toBe('not verified');
  expect(gate(reports.map((r, i) => i === 0 ? { ...r, adapterVersion: 'other' } : r), 3).status).toBe('not verified');
  expect(gate(reports.map((r, i) => i === 0 ? { ...r, bytesPerEvent: 1300 } : r), 3).status).toBe('not met');
  expect(gate(reports.map((r) => ({ ...r, bytesPerEvent: 1600 })), 3).status).toBe('not met');
});
test('SLO 4 needs every store and gates grouping at 1500 events/s and written bytes on every report', () => {
  expect(gate(stores(), 4).status).toBe('met');
  expect(gate([...stores(), durableReport({ eventsPerSecond: 1500, eventsPerCommit: 1 })], 4).status).toBe('not met');
  expect(gate([...stores(), durableReport({ eventsPerSecond: 999, eventsPerCommit: 1 })], 4).status).toBe('met');
  expect(gate([...stores(), durableReport({ bytesWrittenPerEvent: 401 })], 4).status).toBe('not met');
  expect(gate(stores().slice(0, 2), 4).checks.filter((check) => check.status !== 'met')).toEqual([{ name: 'redis', status: 'not verified', detail: 'no durable report' }]);
  expect(gate([], 4).status).toBe('not verified');
});
test('SLO 5 evaluates reaction limits and leaves the unavailable subscriber segment unverified', () => {
  const reports = reaction();
  expect(gate(reports, 5).status).toBe('not verified');
  expect(gate(reports, 5).checks.at(-1)).toMatchObject({ name: 'commit to subscriber', status: 'not verified' });
  expect(gate(reports.map((r) => ({ ...r, commitToSubscriberUs: percentiles() })), 5).status).toBe('met');
  for (const [index, p99] of [[0, 2001], [1, 10001], [2, 42001]]) {
    expect(gate(reports.map((r, i) => i === index ? { ...r, nonJudgeUs: { p50: 1, p99 } } : r), 5).status).toBe('not met');
  }
  expect(gate(reports.slice(1), 5).status).toBe('not verified');
});
test('Gates CLI exits one for a failed gate and zero for incomplete passing evidence', async () => {
  const dir = await mkdtemp(join(os.tmpdir(), 'ab-gates-'));
  try {
    const file = join(dir, 'reports.jsonl');
    await writeFile(file, JSON.stringify(durableReport({ eventsPerSecond: 1500, eventsPerCommit: 1 })) + '\n');
    const failed = await cli('gates', [file]);
    expect(failed.code).toBe(1);
    expect(failed.stdout).toContain('not met');
    await writeFile(file, JSON.stringify(durableReport()) + '\n');
    const passed = await cli('gates', [file]);
    expect(passed.code).toBe(0);
    expect(passed.stdout).toContain('not verified');
  } finally { await rm(dir, { recursive: true, force: true }); }
});
