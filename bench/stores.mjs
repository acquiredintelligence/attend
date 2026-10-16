#!/usr/bin/env node
// Measures durable commit grouping, bound bytes, commit latency and CPU across concurrent scope producers. Usage: node bench/stores.mjs [--dist dist] --store sqlite|postgres|redis [--journal WAL|DELETE] [--synchronous OFF|NORMAL|FULL] [--worker] [--postgres-url u] [--redis-url u] [--synchronous-commit on|off] [--workloads 200x700,1000x140] [--max-windows 50,250] [--out file.jsonl]
import { performance } from 'node:perf_hooks';
import { setImmediate } from 'node:timers/promises';
import { args, storeOptions, storeSpec, list, emit, positive } from './lib/cli.mjs';
import { loadBuild } from './lib/build.mjs';
import { openStore } from './lib/store.mjs';
import { startEngine } from './lib/engine.mjs';
import { benchEvent, EVENT_BYTES } from './lib/workload.mjs';
import { runnerInfo, finishRunner } from './lib/runner.mjs';

try {
  const options = args({ dist: 'dist', ...storeOptions, workloads: '200x700,1000x140', 'max-windows': '50,250', out: undefined });
  if (options.store === 'memory') throw new Error('--store must be sqlite, postgres or redis');
  const build = await loadBuild(options.dist);
  for (const workload of options.workloads.split(',')) for (const maxWindow of list(options['max-windows'])) {
    const [scopes, eventsPerScope] = workload.split('x').map((value) => positive(value, 'workload'));
    if (!eventsPerScope) throw new Error('invalid workload');
    const opened = await openStore(build, storeSpec(options));
    let engine;
    try {
      engine = startEngine(build, { store: opened.store, settle: { maxEvents: maxWindow } });
      opened.resetStats();
      const runner = runnerInfo();
      const cpu = process.cpuUsage();
      const start = performance.now();
      await Promise.all(Array.from({ length: scopes }, async (_, scope) => {
        for (let i = 0; i < eventsPerScope; i += 1) {
          await engine.attend.push(`s${scope}`, [benchEvent(i, 'continuous', Date.now())]);
          // A synchronous driver resolves push in microtasks; yielding lets the runner's timers settle windows.
          await setImmediate();
        }
      }));
      const elapsed = (performance.now() - start) / 1000;
      const used = process.cpuUsage(cpu);
      const stats = opened.stats();
      await engine.stop();
      engine = undefined;
      const events = scopes * eventsPerScope;
      emit({
        report: 'durable', durability: opened.durability, scopes, eventsPerScope, maxWindow,
        eventsPerSecond: events / elapsed, eventsPerCommit: events / stats.commits, commitsPerSecond: stats.commits / elapsed,
        commitLatencyMs: stats.commitLatencyMs, bytesWrittenPerEvent: stats.bytes / events, eventBytes: EVENT_BYTES,
        cpuMicrosPerEvent: (used.user + used.system) / events, adapterVersion: build.adapterVersion, runner: finishRunner(runner),
      }, options.out);
    } finally { try { await engine?.stop(); } finally { await opened.close(); } }
  }
} catch (error) { console.error(error.stack); process.exitCode = 1; }
