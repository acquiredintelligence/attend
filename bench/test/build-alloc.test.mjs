import { test, expect } from 'vitest';
import { mkdtemp, cp, mkdir, writeFile, utimes, rm } from 'node:fs/promises';
import { join, dirname } from 'node:path';
import os from 'node:os';
import { loadBuild } from '../lib/build.mjs';
import { reducerDriver } from '../lib/allocation.mjs';
import { validateReport } from '../lib/schema.mjs';
import { cli, dist, fakeDist, stateDist } from './helpers.mjs';

test('Build loading names this build by version plus revision', async () => {
  const build = await loadBuild(dist);
  expect(build.adapterVersion).toMatch(/^\d+\.\d+\.\d+.*\+[0-9a-f]{7,}/);
  expect(typeof (await build.module('sqlite')).sqliteStore).toBe('function');
});
test('A state-port build with a stub core reducer loads as state/step', async () => {
  const build = await loadBuild(stateDist);
  expect(build.port).toBe('state');
  expect(build.reducer).toBe('step');
  expect(build.adapterVersion).toMatch(/^0\.9\.0\+/);
});
test('Fake build loads the scope port and functioning core reducer', async () => {
  const build = await loadBuild(fakeDist);
  expect(build.port).toBe('scope');
  expect(build.reducer).toBe('core');
  expect(build.adapterVersion).toMatch(/^1\.2\.3\+/);
});
test('Build loading rejects source newer than index.js', async () => {
  const dir = await mkdtemp(join(os.tmpdir(), 'ab-build-'));
  try {
    await cp(dirname(fakeDist), dir, { recursive: true });
    await mkdir(join(dir, 'src'));
    const file = join(dir, 'src', 'changed.ts');
    await writeFile(file, 'export {};\n');
    await utimes(file, new Date(Date.now() + 10000), new Date(Date.now() + 10000));
    await expect(loadBuild(join(dir, 'dist'))).rejects.toThrow(`stale build: ${join(dir, 'dist')} is older than ${file}; run pnpm build`);
  } finally { await rm(dir, { recursive: true, force: true }); }
});
test.each(['reducer', 'endToEnd'])('Allocation CLI drives the %s cell of this build and the sampling crosscheck', async (layer) => {
  const result = await cli('alloc', ['--dist', dist, '--cell', `${layer}:10:0`, '--events', '20', '--crosscheck'], ['--expose-gc']);
  expect(result.code, result.stderr).toBe(0);
  const report = JSON.parse(result.stdout);
  expect(validateReport(report).ok).toBe(true);
  expect(report.reducer).toBe((await loadBuild(dist)).reducer);
  expect(report.layer).toBe(layer);
  expect(report.bytesPerEvent).toBeGreaterThan(0);
  expect(report.cpuMicrosPerEvent).toBeGreaterThan(0);
  expect(report.sampledBytesPerEvent).toBeGreaterThan(0);
  expect(report.events).toBe(layer === 'reducer' ? 20 : 100);
});
test('Core allocation wiring uses one state, one output and logged lifecycle inputs', async () => {
  const build = await loadBuild(fakeDist);
  const core = await build.module('core/reducer');
  const state = await build.module('core/state');
  core.calls.length = 0;
  core.outputs.clear();
  state.records.length = 0;
  const driver = await reducerDriver(build, 2, 0);
  expect(driver.run(1)).toBe(2);
  expect(state.records.length).toBe(1);
  expect(core.outputs.size).toBe(1);
  expect(core.calls).toEqual([
    { input: { kind: 'events', events: [{ id: 'e0', at: 1760000000001, name: 'bench.continuous', data: { seq: 0, x: 1, y: 2, value: 0.42 }, text: 'continuous 0', kind: 'continuous' }] }, now: 1760000000001 },
    { input: { kind: 'events', events: [{ id: 'e1', at: 1760000000002, name: 'bench.continuous', data: { seq: 1, x: 1, y: 2, value: 0.42 }, text: 'continuous 1', kind: 'continuous' }] }, now: 1760000000002 },
    { input: { kind: 'tick' }, now: 1760000000013 },
    { input: { kind: 'judged', batchId: 'b1', attempt: 1, judgment: { route: 'note' } }, now: 1760000000013 },
    { input: { kind: 'delivered', deliveryId: 'd1', attempt: 1 }, now: 1760000000013 },
  ]);
  await driver.close();
  const result = await cli('alloc', ['--dist', fakeDist, '--cell', 'reducer:2:0', '--events', '2'], ['--expose-gc']);
  expect(result.code, result.stderr).toBe(0);
  expect(JSON.parse(result.stdout)).toMatchObject({ report: 'alloc', reducer: 'core', window: 2, events: 2 });
});
