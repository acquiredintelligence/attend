import { test, expect } from 'vitest';
import os from 'node:os';
import { join } from 'node:path';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { evaluateGates } from '../lib/gates.mjs';
import { openReport, allocReport, durableReport, latencyReport, percentiles, cli } from './helpers.mjs';

const gate = (reports, slo) => evaluateGates(reports).find((result) => result.slo === slo);
const openSet = () => ['continuous', 'realistic'].flatMap((mix) => [10000, 30000, 50000].flatMap((targetRate) => Array.from({ length: 3 }, () => openReport({ mix, targetRate, achievedRate: targetRate }))));
const grid = () => ['reducer', 'endToEnd'].flatMap((layer) => [10, 100, 250, 1000].flatMap((window) => [0, 50, 500, 2000].map((seenCount) => allocReport({ layer, window, seenCount }))));
const modes = [
  { store: 'sqlite', journalMode: 'WAL', synchronous: 'NORMAL', worker: false },
  { store: 'sqlite', journalMode: 'WAL', synchronous: 'FULL', worker: false },
  { store: 'sqlite', journalMode: 'DELETE', synchronous: 'FULL', worker: false },
  { store: 'postgres', synchronousCommit: 'on' },
  { store: 'redis', appendonly: true, appendfsync: 'everysec' },
];
const stores = () => modes.flatMap((durability) => [[200, 700], [1000, 140]].flatMap(([scopes, eventsPerScope]) => [50, 250].map((maxWindow) => durableReport({ durability, scopes, eventsPerScope, maxWindow }))));
const reaction = () => [
  latencyReport(), latencyReport({ load: { mix: 'realistic', targetRate: 50000, achievedRate: 50000, scopes: 1000 } }),
  ...[modes[0], modes[3], modes[4]].map((durability) => latencyReport({ durability, commitLatencyMs: percentiles() })),
];

test('Gates report all five SLOs as not verified without evidence', () => {
  expect(evaluateGates([]).map(({ slo, status }) => ({ slo, status }))).toEqual([1, 2, 3, 4, 5].map((slo) => ({ slo, status: 'not verified' })));
});
test('SLO 1 checks both mixes, all runs, rate, p50 growth, failures, CPU and knee', () => {
  const reports = openSet();
  expect(gate(reports, 1)).toMatchObject({ status: 'met', info: { knee: { continuous: 50000, realistic: 50000 } } });
  for (const changes of [{ achievedRate: 49900 }, { p50LastUs: 121 }, { failed: 1 }, { rejected: 1 }, { attendCores: 1.01 }]) {
    expect(gate(reports.map((r, i) => i === 6 ? { ...r, ...changes } : r), 1).status).toBe('not met');
  }
  expect(gate(reports.filter((r, i) => i !== 6), 1).status).toBe('not verified');
  expect(gate(reports.map((r) => ({ ...r, seconds: 15 })), 1).status).toBe('not verified');
  expect(gate(reports.map((r) => ({ ...r, scopes: 50 })), 1).status).toBe('not verified');
});
test('SLO 2 refuses failed lines at a tenth of the target rate', () => {
  const reports = openSet().map((r) => ({ ...r, failed: 0.9 * r.targetRate * 60, achievedRate: 0.1 * r.targetRate }));
  expect(gate(reports, 2).status).toBe('not met');
});
test.each([
  ['rejected lines', { rejected: 1 }, 'not met'],
  ['a missed rate', { achievedRate: 9900 }, 'not met'],
  ['p50 growth', { p50LastUs: 121 }, 'not met'],
  ['a failed floor', { noopFloorFailed: 1 }, 'not verified'],
  ['a slow floor', { noopFloorRate: 9900 }, 'not verified'],
])('SLO 2 refuses %s', (_, changes, status) => {
  const reports = openSet().map((r, i) => i === 0 ? { ...r, ...changes } : r);
  expect(gate(reports, 2).status).toBe(status);
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
test('SLO 3 rejects only row checks when reducer allocation varies across windows', () => {
  const reports = grid().map((r) => r.layer === 'reducer' && r.window >= 250 ? { ...r, bytesPerEvent: 2000 } : r);
  const result = gate(reports, 3);
  expect(result.status).toBe('not met');
  expect(result.checks.filter((check) => check.status === 'not met').map((check) => check.name)).toEqual([
    '1.2.3+abcdef0/reducer/S=0', '1.2.3+abcdef0/reducer/S=50', '1.2.3+abcdef0/reducer/S=500', '1.2.3+abcdef0/reducer/S=2000',
  ]);
});
test('SLO 3 rejects only column checks when reducer allocation varies across seen counts', () => {
  const reports = grid().map((r) => r.layer === 'reducer' && r.seenCount >= 500 ? { ...r, bytesPerEvent: 2000 } : r);
  const result = gate(reports, 3);
  expect(result.status).toBe('not met');
  expect(result.checks.filter((check) => check.status === 'not met').map((check) => check.name)).toEqual([
    '1.2.3+abcdef0/reducer/W=10', '1.2.3+abcdef0/reducer/W=100', '1.2.3+abcdef0/reducer/W=250', '1.2.3+abcdef0/reducer/W=1000',
  ]);
});
test('SLO 4 requires all twenty mode, workload and window cells', () => {
  expect(gate(stores(), 4).status).toBe('met');
  const result = gate(stores().slice(1), 4);
  expect(result.status).toBe('not verified');
  expect(result.checks.filter((check) => check.status !== 'met')).toEqual([
    { name: 'sqlite WAL/NORMAL/200x700/W=50', status: 'not verified', detail: 'no report of at least this size' },
  ]);
});
test('SLO 4 ignores undersized reports including modes with durability off', () => {
  const reports = [
    { store: 'sqlite', journalMode: 'WAL', synchronous: 'OFF', worker: false },
    { store: 'postgres', synchronousCommit: 'off' },
    { store: 'redis', appendonly: false, appendfsync: 'everysec' },
  ].map((durability) => durableReport({ durability, scopes: 1, eventsPerScope: 10, eventsPerSecond: 500, eventsPerCommit: 1 }));
  const result = gate(reports, 4);
  expect(result.status).toBe('not verified');
  expect(result.checks).toHaveLength(20);
  expect(result.checks.filter((check) => check.status === 'not verified')).toHaveLength(20);
  expect(result.checks.filter((check) => check.status === 'not met')).toEqual([]);
  expect(result.info).toEqual({ undersized: 3 });
  expect(gate([...stores(), durableReport({ scopes: 1, eventsPerScope: 10, eventsPerSecond: 1500, eventsPerCommit: 1 })], 4)).toMatchObject({ status: 'met', info: { undersized: 1 } });
});
test('SLO 4 requires Redis appendonly for its four cells', () => {
  const reports = stores().map((r) => r.durability.store === 'redis' ? { ...r, durability: { ...r.durability, appendonly: false } } : r);
  const result = gate(reports, 4);
  expect(result.status).toBe('not verified');
  expect(result.checks.filter((check) => check.status !== 'met').map((check) => check.name)).toEqual([
    'redis appendonly/200x700/W=50', 'redis appendonly/200x700/W=250',
    'redis appendonly/1000 scopes/W=50', 'redis appendonly/1000 scopes/W=250',
  ]);
});
test('SLO 4 checks grouping and bytes on every sized report including worker mode', () => {
  expect(gate([...stores(), durableReport({ eventsPerSecond: 1500, eventsPerCommit: 1 })], 4).status).toBe('not met');
  expect(gate([...stores(), durableReport({ eventsPerSecond: 999, eventsPerCommit: 1 })], 4).status).toBe('not verified');
  expect(gate([...stores(), durableReport({ bytesWrittenPerEvent: 401 })], 4).status).toBe('not met');
  expect(gate([...stores(), durableReport({ durability: { ...modes[0], worker: true }, eventsPerSecond: 1500, eventsPerCommit: 1 })], 4).status).toBe('not met');
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
test('SLO 5 rejects failed probes even when every percentile is zero', () => {
  const zero = { p50: 0, p99: 0, p999: 0, max: 0 };
  const failed = latencyReport({ samples: 0, errors: 3, flushToJudgeUs: zero, judgeToSinkUs: zero, nonJudgeUs: zero, flushToSinkUs: zero, commitToSubscriberUs: percentiles() });
  const result = gate(reaction().map((r, i) => i === 0 ? failed : { ...r, commitToSubscriberUs: percentiles() }), 5);
  expect(result.status).toBe('not met');
  expect(result.checks.filter((check) => check.status !== 'met')).toEqual([
    { name: 'memory idle/1', status: 'not met', detail: '3 failed probes' },
    { name: 'memory idle', status: 'not verified', detail: 'none had at least 200 probes with none failed' },
  ]);
});
test.each([
  ['too few probes', 0, { samples: 199 }],
  ['a missed loaded rate', 1, { load: { mix: 'realistic', targetRate: 50000, achievedRate: 40000, scopes: 1000 } }],
  ['too few loaded scopes', 1, { load: { mix: 'realistic', targetRate: 50000, achievedRate: 50000, scopes: 50 } }],
])('SLO 5 refuses %s', (_, index, changes) => {
  const reports = reaction().map((r, i) => ({ ...r, commitToSubscriberUs: percentiles(), ...(i === index ? changes : {}) }));
  expect(gate(reports, 5).status).toBe('not verified');
});
test('SLO 5 requires each durable store', () => {
  const reports = reaction().filter((r) => r.durability.store !== 'redis').map((r) => ({ ...r, commitToSubscriberUs: percentiles() }));
  expect(gate(reports, 5).status).toBe('not verified');
});
test('SLO 5 cannot verify the subscriber segment from failed probes', () => {
  const reports = reaction().map((r) => ({ ...r, samples: 1, errors: 199, commitToSubscriberUs: percentiles() }));
  const result = gate(reports, 5);
  expect(result.status).toBe('not met');
  expect(result.checks.filter((check) => check.status === 'not met')).toHaveLength(5);
  expect(result.checks.at(-1)).toEqual({ name: 'commit to subscriber', status: 'not verified', detail: 'MCP Events segment unavailable' });
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
