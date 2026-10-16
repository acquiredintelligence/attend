import { beforeAll, test, expect } from 'vitest';
import { spawnTarget } from '../lib/spawn.mjs';
import { runTarget, openLoopReport } from '../lib/openloop.mjs';
import { createSchedule } from '../lib/workload.mjs';
import { validateReport } from '../lib/schema.mjs';
import { createHistogram } from '../lib/histogram.mjs';
import { loadBuild } from '../lib/build.mjs';
import { cli, dist } from './helpers.mjs';

let unavailable;
beforeAll(async () => {
  const target = spawnTarget({ mode: 'noop', dist });
  try { await target.ready; }
  catch (error) { if (/EPERM/.test(error.message)) unavailable = error.message; else throw error; }
  finally { await target.exit(); }
});
function needSocket(context) {
  if (unavailable) context.skip(`Unix socket listen denied by sandbox: ${unavailable}`);
}
const stall = { atLine: 10000, ms: 5 };
function affected(run, schedule) {
  const time = run.lines.intended[stall.atLine];
  const start = schedule.intendedMs(stall.atLine);
  const indexes = [];
  for (let j = 0; j < run.total; j += 1) if (schedule.intendedMs(j) >= start && schedule.intendedMs(j) < start + 5) indexes.push(j);
  expect(indexes.length).toBe(50);
  for (const j of indexes) expect(run.lines.acked[j] - run.lines.intended[j]).toBeGreaterThanOrEqual(time + 5 - run.lines.intended[j]);
  const fromIntended = createHistogram();
  for (let j = schedule.warmCount; j < run.total; j += 1) fromIntended.record(Math.max(0, (run.lines.acked[j] - run.lines.intended[j]) * 1000));
  expect(run.latency.toJSON()).toEqual(fromIntended.toJSON());
  return { indexes, time };
}
test('Coordinated omission: a target stall charges all 50 intended lines', async (context) => {
  needSocket(context);
  const schedule = createSchedule({ mix: 'noop', rate: 10000, scopes: 1000, seconds: 2, warmup: 0.2 });
  const { run } = await runTarget({ mode: 'noop', dist, stall }, schedule, { keepLines: true });
  expect(run.failed).toBe(0);
  affected(run, schedule);
});
test('Coordinated omission: a generator stall charges all 50 intended lines', async (context) => {
  needSocket(context);
  const schedule = createSchedule({ mix: 'noop', rate: 10000, scopes: 1000, seconds: 2, warmup: 0.2 });
  const { run } = await runTarget({ mode: 'noop', dist, credit: 64 }, schedule, { keepLines: true, stall });
  expect(run.failed).toBe(0);
  affected(run, schedule);
});
test('Coordinated omission: timing from the actual send would hide a generator stall', async (context) => {
  needSocket(context);
  const long = { atLine: 10000, ms: 50 };
  const schedule = createSchedule({ mix: 'noop', rate: 10000, scopes: 1000, seconds: 2, warmup: 0.2 });
  const { run } = await runTarget({ mode: 'noop', dist }, schedule, { keepLines: true, stall: long });
  const time = run.lines.intended[long.atLine];
  const early = [];
  for (let j = long.atLine; schedule.intendedMs(j) < schedule.intendedMs(long.atLine) + 25; j += 1) early.push(j);
  expect(early.length).toBe(250);
  for (const j of early) expect(run.lines.acked[j] - run.lines.intended[j]).toBeGreaterThanOrEqual(time + 50 - run.lines.intended[j]);
  expect(early.some((j) => run.lines.acked[j] - run.lines.sent[j] < time + 50 - run.lines.intended[j])).toBe(true);
});
test('No stall: all lines acknowledged and every report field validates', async (context) => {
  needSocket(context);
  const schedule = createSchedule({ mix: 'noop', rate: 10000, scopes: 1000, seconds: 2, warmup: 0.2 });
  const result = await runTarget({ mode: 'noop', dist, credit: 128 }, schedule, { keepLines: true });
  expect(result.run.acked).toBe(22000);
  expect(result.run.failed).toBe(0);
  expect(result.run.measured).toBe(20000);
  expect(result.run.latency.count).toBe(20000);
  expect(result.run.first.count).toBe(4000);
  expect(result.run.last.count).toBe(4000);
  expect(result.stats.lines).toBe(20000);
  const report = openLoopReport(result, { schedule, scopes: 1000, mix: 'noop', floor: result.run.latency.percentiles() });
  expect(validateReport(report)).toEqual({ ok: true, report });
});
test('Latency CLI records exactly twenty memory probes', async (context) => {
  needSocket(context);
  const result = await cli('latency', ['--dist', dist, '--store', 'memory', '--samples', '20', '--probe-ms', '5']);
  expect(result.code, result.stderr).toBe(0);
  const report = JSON.parse(result.stdout);
  expect(validateReport(report).ok).toBe(true);
  expect(report).toMatchObject({ report: 'latency', samples: 20, errors: 0, commitToSubscriberUs: null, commitLatencyMs: null });
  expect(report.flushToSinkUs.p50).toBeGreaterThanOrEqual(report.nonJudgeUs.p50);
});
test('Openloop CLI records a memory run beside its own noop floor', async (context) => {
  needSocket(context);
  const result = await cli('openloop', ['--dist', dist, '--mix', 'continuous', '--rates', '2000', '--scopes', '50', '--seconds', '2', '--warmup', '0.5']);
  expect(result.code, result.stderr).toBe(0);
  const lines = result.stdout.trim().split('\n');
  expect(lines.length).toBe(1);
  const report = JSON.parse(lines[0]);
  expect(validateReport(report).ok).toBe(true);
  expect(report).toMatchObject({ report: 'openloop', failed: 0, overdueRule: (await loadBuild(dist)).port === 'scope' });
  expect(report.windows).toBeGreaterThanOrEqual(0);
  expect(report.noopFloorUs.p50).toBeGreaterThan(0);
});

test('Target acknowledges rejected lines and keeps the contiguous prefix', async (context) => {
  needSocket(context);
  const { default: http } = await import('node:http');
  const target = spawnTarget({ mode: 'attend', dist, credit: 4 });
  try {
    await target.ready;
    await target.request({ type: 'measure', phase: 'start' });
    const frames = await new Promise((resolve, reject) => {
      const request = http.request({ socketPath: target.socketPath, path: '/v1/ingest/stream', method: 'POST' }, (response) => {
        let body = '';
        response.setEncoding('utf8');
        response.on('data', (chunk) => { body += chunk; });
        response.on('end', () => resolve(body.trim().split('\n').map((line) => JSON.parse(line))));
        response.on('error', reject);
      });
      request.on('error', reject);
      request.end('invalid\n{"scope":"s0","id":"one","at":1760000000000,"name":"bench.continuous","data":{}}\n{"scope":"s0","end":true}\n');
    });
    expect(frames[0]).toEqual({ credit: 4 });
    expect(frames.filter((frame) => frame.ack).at(-1).through).toBe(3);
    expect(frames.filter((frame) => frame.credit).reduce((sum, frame) => sum + frame.credit, 0)).toBe(7);
    const { stats } = await target.request('stats');
    expect(stats.lines).toBe(3);
    expect(stats.rejected).toBe((await loadBuild(dist)).port === 'scope' ? 1 : 2);
  } finally { await target.exit(); }
});

test('Generator drains missing credit and acknowledgments without hanging', async (context) => {
  needSocket(context);
  const { default: http } = await import('node:http');
  const { mkdtemp, rm } = await import('node:fs/promises');
  const { join } = await import('node:path');
  const { drive } = await import('../lib/generator.mjs');
  const dir = await mkdtemp('/tmp/ab-drain-');
  const socketPath = join(dir, 's');
  const server = http.createServer((request, response) => {
    response.writeHead(200);
    response.write('{"credit":1}\n');
    request.resume();
  });
  try {
    await new Promise((resolve, reject) => { server.once('error', reject); server.listen(socketPath, resolve); });
    const schedule = createSchedule({ mix: 'noop', rate: 100, scopes: 1, seconds: 0.03, warmup: 0 });
    const result = await drive({ socketPath, schedule, keepLines: true, drainMs: 20 });
    expect(result.total).toBe(3);
    expect(result.acked).toBe(0);
    expect(result.failed).toBe(3);
    expect(result.measured).toBe(0);
    expect(result.achievedRate).toBe(0);
    expect(result.lines.intended[2] - result.lines.intended[0]).toBeCloseTo(20, 8);
  } finally {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
    await rm(dir, { recursive: true, force: true });
  }
});
