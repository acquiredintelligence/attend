#!/usr/bin/env node
// Measures open-loop throughput and intended-time ingest latency beside a separate no-op floor. Usage: node bench/openloop.mjs [--dist dist] [--mix continuous|realistic|tape|noop] [--rates 10000,30000,50000] [--scopes 1000] [--seconds 60] [--warmup 5] [--runs 1] [--seed 1] [--tapes file.json] [--target-cpus list] [--out file.jsonl]
import { readFileSync } from 'node:fs';
import { args, positive, list, emit } from './lib/cli.mjs';
import { createSchedule } from './lib/workload.mjs';
import { runTarget, openLoopReport } from './lib/openloop.mjs';

try {
  const options = args({ dist: 'dist', mix: 'continuous', rates: '10000,30000,50000', scopes: '1000', seconds: '60', warmup: '5', runs: '1', seed: '1', tapes: undefined, 'target-cpus': undefined, out: undefined });
  const scopes = positive(options.scopes, 'scopes');
  const rates = options.mix === 'tape' ? [0] : list(options.rates);
  for (const rate of rates) for (let run = 0; run < positive(options.runs, 'runs'); run += 1) {
    const schedule = createSchedule({ mix: options.mix, rate, scopes, seconds: positive(options.seconds, 'seconds'), warmup: positive(options.warmup, 'warmup', true), seed: Number(options.seed) + run, tapes: options.tapes ? JSON.parse(readFileSync(options.tapes, 'utf8')) : undefined });
    const targetOptions = { dist: options.dist, cpus: options['target-cpus'] };
    const noop = await runTarget({ ...targetOptions, mode: 'noop' }, schedule);
    const result = options.mix === 'noop' ? noop : await runTarget({ ...targetOptions, mode: 'attend' }, schedule);
    emit(openLoopReport(result, { schedule, scopes, mix: options.mix, floor: noop.run.latency.percentiles() }), options.out);
  }
} catch (error) { console.error(error.stack); process.exitCode = 1; }
