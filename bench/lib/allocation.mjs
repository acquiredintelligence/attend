import v8 from 'node:v8';
import { Session } from 'node:inspector/promises';
import { setImmediate as immediate } from 'node:timers/promises';
import { benchEvent } from './workload.mjs';
import { startEngine } from './engine.mjs';

export async function reducerDriver(build, window, seenCount) {
  const { initialRecord, step } = await build.module('step');
  const policy = { ...build.core.DEFAULT_POLICY, seenCount, maxEvents: window + 1, maxWindowMs: 1e12 };
  let record = initialRecord('s0');
  let apply;
  if (build.reducer === 'core') {
    const { fromRecord } = await build.module('core/state');
    const reducer = await build.module('core/reducer');
    const state = fromRecord(record, policy);
    const out = reducer.newOutput();
    const config = { policy, decisions: 'lane', deadLetters: 'lane', traces: false };
    apply = (input, now) => {
      out.effects.length = 0; out.appends.length = 0; out.changed = false;
      reducer.apply(config, state, { input, now }, out);
      return out.effects;
    };
  } else {
    const config = { kind: (event) => event.kind ?? 'continuous', policy };
    apply = (input, now) => {
      const out = step(config, record, input, now);
      record = { ...out.record, version: record.version + 1 };
      return out.effects;
    };
  }
  let id = 0;
  let now = 1760000000000;
  return {
    run(windows) {
      for (let i = 0; i < windows; i += 1) {
        for (let j = 0; j < window; j += 1) {
          now += 1;
          apply({ kind: 'events', events: [{ ...benchEvent(id++, 'continuous', now), kind: 'continuous' }] }, now);
        }
        now += policy.streamGapMs + 1;
        const judge = apply({ kind: 'tick' }, now).find((effect) => effect.kind === 'judge');
        if (!judge) throw new Error('reducer did not judge the completed window');
        const delivery = apply({ kind: 'judged', batchId: judge.batchId, attempt: judge.attempt, judgment: { route: 'note' } }, now).find((effect) => effect.kind === 'deliver');
        if (!delivery) throw new Error('reducer did not deliver the judgment');
        apply({ kind: 'delivered', deliveryId: delivery.delivery.item.id, attempt: delivery.delivery.phase.attempt }, now);
      }
      return windows * window;
    },
    async close() {},
  };
}

export function endToEndDriver(build, window, seenCount) {
  const waiting = new Map();
  let id = 0;
  const engine = startEngine(build, {
    store: build.core.memoryStore(), settle: { seenCount, maxEvents: window + 1 },
    onDelivered(decision) { waiting.get(decision.scope)?.(); },
  });
  return {
    async run(windows) {
      await Promise.all(Array.from({ length: 10 }, async (_, s) => {
        const scope = `s${s}`;
        for (let round = 0; round < windows; round += 1) {
          for (let i = 0; i < window; i += 1) await engine.attend.push(scope, [benchEvent(id++, 'continuous', Date.now())]);
          let timer;
          const delivered = new Promise((resolve, reject) => {
            timer = setTimeout(() => reject(new Error('allocation decision timed out')), 10000);
            waiting.set(scope, resolve);
          });
          try { await engine.attend.flush(scope); await delivered; }
          finally { clearTimeout(timer); waiting.delete(scope); }
        }
      }));
      await immediate();
      return windows * window * 10;
    },
    close: () => engine.stop(),
  };
}

export async function measureAllocation(run) {
  globalThis.gc();
  const profiler = new v8.GCProfiler();
  profiler.start();
  const startHeap = v8.getHeapStatistics().used_heap_size;
  const startCpu = process.cpuUsage();
  const events = await run();
  const cpu = process.cpuUsage(startCpu);
  const endHeap = v8.getHeapStatistics().used_heap_size;
  const result = profiler.stop();
  let previous = startHeap;
  let allocated = 0;
  for (const record of result.statistics) {
    allocated += record.beforeGC.heapStatistics.usedHeapSize - previous;
    previous = record.afterGC.heapStatistics.usedHeapSize;
  }
  allocated += endHeap - previous;
  return { events, bytesPerEvent: Math.max(0, allocated) / events, cpuMicrosPerEvent: (cpu.user + cpu.system) / events };
}

export async function sampleAllocation(run) {
  globalThis.gc();
  const session = new Session();
  session.connect();
  try {
    await session.post('HeapProfiler.startSampling', { samplingInterval: 4096, includeObjectsCollectedByMajorGC: true, includeObjectsCollectedByMinorGC: true });
    const events = await run();
    const { profile } = await session.post('HeapProfiler.stopSampling');
    const sum = (node) => node.selfSize + node.children.reduce((total, child) => total + sum(child), 0);
    return sum(profile.head) / events;
  } finally { session.disconnect(); }
}
