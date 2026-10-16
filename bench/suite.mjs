#!/usr/bin/env node
// Runs allocation, ingest, durable-store and reaction measurements, alternating build order between rounds. Usage: node bench/suite.mjs --dist <dir> [--dist <dir2>] [--out-dir bench/reports] [--quick] [--stores sqlite,postgres,redis] [--target-cpus list] [--runs 3]
import { parseArgs } from 'node:util';
import { mkdirSync, writeFileSync, appendFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { loadBuild } from './lib/build.mjs';
import { positive } from './lib/cli.mjs';

try {
  const { values } = parseArgs({ options: {
    dist: { type: 'string', multiple: true }, 'out-dir': { type: 'string', default: 'bench/reports' }, quick: { type: 'boolean', default: false },
    stores: { type: 'string', default: 'sqlite,postgres,redis' }, 'target-cpus': { type: 'string' }, runs: { type: 'string', default: '3' },
  } });
  if (!values.dist?.length) throw new Error('--dist is required');
  const builds = await Promise.all(values.dist.map(loadBuild));
  const outDir = resolve(values['out-dir']);
  mkdirSync(outDir, { recursive: true });
  const files = builds.map((build) => resolve(outDir, `${build.adapterVersion}.jsonl`));
  if (new Set(files).size !== files.length) throw new Error('builds must have different adapter versions');
  for (const file of files) writeFileSync(file, '');
  const stores = values.stores.split(',').filter((store) => {
    if (store === 'postgres' && !process.env.ATTEND_POSTGRES_URL || store === 'redis' && !process.env.ATTEND_REDIS_URL) { console.error(`Skipping ${store}: URL is unset`); return false; }
    if (!['sqlite', 'postgres', 'redis'].includes(store)) throw new Error(`unknown store: ${store}`);
    return true;
  });
  const run = async (i, script, flags) => {
    const argv = [fileURLToPath(new URL(`./${script}.mjs`, import.meta.url)), '--dist', builds[i].dist, '--out', files[i], ...flags];
    if (values['target-cpus'] && ['openloop', 'latency'].includes(script)) argv.push('--target-cpus', values['target-cpus']);
    await new Promise((resolveDone, reject) => {
      const child = spawn(process.execPath, argv, { stdio: 'inherit' });
      child.once('error', reject);
      child.once('exit', (code, signal) => code === 0 ? resolveDone() : reject(new Error(`${script} exited ${code ?? signal}`)));
    });
  };
  const order = builds.map((_, i) => i);
  const quick = values.quick;
  for (const i of order) await run(i, 'alloc', quick ? ['--events', '2000'] : []);
  for (let round = 0; round < positive(values.runs, 'runs'); round += 1) {
    const indices = round % 2 ? [...order].reverse() : order;
    for (const mix of ['continuous', 'realistic']) for (const rate of quick ? [10000] : [10000, 30000, 50000]) for (const i of indices) {
      await run(i, 'openloop', ['--mix', mix, '--rates', String(rate), '--seconds', quick ? '10' : '60', '--warmup', quick ? '2' : '5', '--seed', String(round + 1)]);
    }
  }
  for (const store of stores) for (const i of order) {
    const modes = store === 'sqlite' ? [
      ['--journal', 'WAL', '--synchronous', 'NORMAL'], ['--journal', 'WAL', '--synchronous', 'FULL'], ['--journal', 'DELETE', '--synchronous', 'FULL'],
      ...(builds[i].port === 'scope' ? [['--journal', 'WAL', '--synchronous', 'NORMAL', '--worker'], ['--journal', 'WAL', '--synchronous', 'FULL', '--worker']] : []),
    ] : store === 'postgres' ? [['--synchronous-commit', 'on'], ...(builds[i].port === 'scope' ? [['--synchronous-commit', 'off']] : [])] : [[]];
    for (const mode of modes) await run(i, 'stores', ['--store', store, ...mode, ...(quick ? ['--workloads', '50x100', '--max-windows', '50'] : [])]);
  }
  for (const i of order) {
    const flags = quick ? ['--samples', '30', '--seconds', '10'] : [];
    await run(i, 'latency', flags);
    await run(i, 'latency', [...flags, '--load', '50000', '--mix', 'realistic']);
    for (const store of stores) await run(i, 'latency', [...flags, '--store', store]);
  }
  if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, `head_report=${files[0]}\n`);
  console.log(files[0]);
} catch (error) { console.error(error.stack); process.exitCode = 1; }
