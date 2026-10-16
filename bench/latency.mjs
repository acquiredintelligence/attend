#!/usr/bin/env node
// Measures flush-to-judge and judge-to-sink reaction segments, idle or under open-loop load; samples is the idle count and loaded minimum. Usage: node bench/latency.mjs [--dist dist] [--store memory|sqlite|postgres|redis] [--samples 200] [--probe-ms 20] [--load 0|rate] [--mix continuous|realistic] [--scopes 1000] [--seconds 30] [--warmup 5] [--target-cpus list] [--out file.jsonl]
import { setTimeout as delay } from 'node:timers/promises';
import { args, positive, storeOptions, storeSpec, emit } from './lib/cli.mjs';
import { createSchedule } from './lib/workload.mjs';
import { spawnTarget } from './lib/spawn.mjs';
import { drive } from './lib/generator.mjs';
import { histogramFromJSON } from './lib/histogram.mjs';
import { runnerInfo, finishRunner } from './lib/runner.mjs';

let target;
try {
  const options = args({ dist: 'dist', ...storeOptions, 'probe-ms': '20', samples: '200', load: '0', mix: 'realistic', scopes: '1000', seconds: '30', warmup: '5', 'target-cpus': undefined, out: undefined });
  const runner = runnerInfo();
  const rate = positive(options.load, 'load', true);
  const samples = positive(options.samples, 'samples');
  const scopes = positive(options.scopes, 'scopes');
  const schedule = rate ? createSchedule({ mix: options.mix, rate, scopes, seconds: positive(options.seconds, 'seconds'), warmup: positive(options.warmup, 'warmup', true) }) : null;
  target = spawnTarget({ mode: 'attend', dist: options.dist, store: storeSpec(options), cpus: options['target-cpus'], probe: { intervalMs: positive(options['probe-ms'], 'probe-ms') }, warmLines: schedule?.warmCount });
  await target.ready;
  let load = null;
  if (schedule) {
    const run = await drive({ socketPath: target.socketPath, schedule, onEnd: () => target.request({ type: 'measure', phase: 'end' }) });
    if (run.failed || run.rejected) throw new Error(`${run.failed} load lines not acknowledged and ${run.rejected} rejected`);
    load = { mix: options.mix, targetRate: schedule.targetRate, achievedRate: run.achievedRate, scopes };
  } else {
    await target.request({ type: 'measure', phase: 'start', samples });
    while (true) {
      const { stats } = await target.request('stats');
      if (stats.probe.samples + stats.probe.errors >= samples) break;
      await delay(Math.min(20, Number(options['probe-ms'])));
    }
    await target.request({ type: 'measure', phase: 'end' });
  }
  const { stats } = await target.request('stats');
  const percentiles = (name) => histogramFromJSON(stats.probe[name]).percentiles();
  emit({
    report: 'latency', durability: stats.durability, load, samples: stats.probe.samples, errors: stats.probe.errors,
    flushToJudgeUs: percentiles('flushToJudge'), judgeToSinkUs: percentiles('judgeToSink'), nonJudgeUs: percentiles('nonJudge'),
    flushToSinkUs: percentiles('flushToSink'), commitToSubscriberUs: null, commitLatencyMs: stats.store?.commitLatencyMs ?? null,
    adapterVersion: stats.adapterVersion, runner: finishRunner(runner, stats),
  }, options.out);
  if (stats.probe.errors > 0 || stats.probe.samples < samples) {
    console.error(`${stats.probe.errors} probes failed; ${stats.probe.samples} of ${samples} samples`);
    process.exitCode = 1;
  }
} catch (error) { console.error(error.stack); process.exitCode = 1; }
finally { await target?.exit(); }
