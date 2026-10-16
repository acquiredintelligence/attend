#!/usr/bin/env node
// Measures reducer and end-to-end allocation per event in isolated W x S cells, with optional heap sampling. Usage: node bench/alloc.mjs [--dist dist] [--layers reducer,endToEnd] [--windows 10,100,250,1000] [--seen 0,50,500,2000] [--events 20000] [--crosscheck] [--out file.jsonl]
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { args, list, positive, emit } from './lib/cli.mjs';
import { loadBuild } from './lib/build.mjs';
import { runnerInfo, finishRunner } from './lib/runner.mjs';
import { reducerDriver, endToEndDriver, measureAllocation, sampleAllocation } from './lib/allocation.mjs';

try {
  const options = args({ dist: 'dist', layers: 'reducer,endToEnd', windows: '10,100,250,1000', seen: '0,50,500,2000', events: '20000', crosscheck: false, out: undefined, cell: undefined });
  if (options.cell) {
    if (!globalThis.gc) throw new Error('allocation cells require --expose-gc');
    const [layer, w, s] = options.cell.split(':');
    if (!['reducer', 'endToEnd'].includes(layer)) throw new Error('invalid layer');
    const window = positive(w, 'window');
    const seenCount = positive(s, 'seen', true);
    const build = await loadBuild(options.dist);
    const driver = layer === 'reducer' ? await reducerDriver(build, window, seenCount) : endToEndDriver(build, window, seenCount);
    try {
      await driver.run(Math.ceil(seenCount / window) + 2);
      const runner = runnerInfo();
      const count = Math.ceil(positive(options.events, 'events') / (window * (layer === 'reducer' ? 1 : 10)));
      const measurement = await measureAllocation(() => driver.run(count));
      const sampled = options.crosscheck ? { sampledBytesPerEvent: await sampleAllocation(() => driver.run(count)) } : {};
      emit({ report: 'alloc', layer, reducer: build.reducer, window, seenCount, ...measurement, ...sampled, adapterVersion: build.adapterVersion, runner: finishRunner(runner) }, options.out);
    } finally { await driver.close(); }
  } else {
    for (const layer of options.layers.split(',')) for (const window of list(options.windows)) for (const seen of list(options.seen)) {
      const argv = ['--expose-gc', fileURLToPath(import.meta.url), '--cell', `${layer}:${window}:${seen}`, '--dist', options.dist, '--events', options.events];
      if (options.crosscheck) argv.push('--crosscheck');
      if (options.out) argv.push('--out', options.out);
      await new Promise((resolve, reject) => {
        const child = spawn(process.execPath, argv, { stdio: 'inherit' });
        child.once('error', reject);
        child.once('exit', (code, signal) => code === 0 ? resolve() : reject(new Error(`allocation cell exited ${code ?? signal}`)));
      });
    }
  }
} catch (error) { console.error(error.stack); process.exitCode = 1; }
