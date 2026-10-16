#!/usr/bin/env node
// Measures engine CPU, parse-to-ack latency, commits and reaction segments behind a streaming Unix socket. Usage: node bench/target.mjs '<options JSON>' (normally launched by spawnTarget).
import http from 'node:http';
import { performance, PerformanceObserver, monitorEventLoopDelay } from 'node:perf_hooks';
import { rm } from 'node:fs/promises';
import { loadBuild } from './lib/build.mjs';
import { openStore } from './lib/store.mjs';
import { startEngine } from './lib/engine.mjs';
import { createHistogram } from './lib/histogram.mjs';
import { runnerInfo, allowedCpus } from './lib/runner.mjs';
import { benchEvent } from './lib/workload.mjs';

const options = JSON.parse(process.argv[2]);
let opened;
let engine;
let server;
let closing = false;
let measuring = false;
let frozen;
let state;
let cpu;
let wall;
let gcMs = 0;
let probeTimer;
let probePending;
let probeLimit = Infinity;
let probeId = 0;
const lag = monitorEventLoopDelay({ resolution: 5 });
lag.enable();
const observer = new PerformanceObserver((list) => {
  if (measuring) for (const entry of list.getEntries()) gcMs += entry.duration;
});
observer.observe({ entryTypes: ['gc'] });
const empty = () => ({
  lines: 0, rejected: 0, engine: createHistogram(), windows: createHistogram(), windowTotal: 0,
  probe: { samples: 0, errors: 0, flushToJudge: createHistogram(), judgeToSink: createHistogram(), nonJudge: createHistogram(), flushToSink: createHistogram() },
});
state = empty();

function scheduleProbe() {
  if (!measuring || !options.probe || probePending || state.probe.samples + state.probe.errors >= probeLimit) return;
  probeTimer = setTimeout(async () => {
    if (!measuring) return;
    const id = probeId++;
    const current = { t0: performance.now(), eventId: `e${id}` };
    probePending = current;
    const timeout = setTimeout(() => finishProbe(current, false), 5000);
    current.timeout = timeout;
    try { await engine.attend.push('bench-probe', [benchEvent(id, 'flush', Date.now())]); }
    catch { finishProbe(current, false); }
  }, options.probe.intervalMs);
}
function finishProbe(current, ok) {
  if (probePending !== current) return;
  clearTimeout(current.timeout);
  probePending = null;
  if (measuring) {
    if (ok) {
      const t3 = performance.now();
      const first = (current.t1 - current.t0) * 1000;
      const second = (t3 - current.t2) * 1000;
      state.probe.samples += 1;
      state.probe.flushToJudge.record(first);
      state.probe.judgeToSink.record(second);
      state.probe.nonJudge.record(first + second);
      state.probe.flushToSink.record((t3 - current.t0) * 1000);
    } else state.probe.errors += 1;
  }
  scheduleProbe();
}
let build;
function startMeasure(samples = Infinity) {
  state = empty(); frozen = undefined; gcMs = 0; opened?.resetStats(); lag.reset();
  cpu = process.cpuUsage(); wall = performance.now(); measuring = true;
  probeLimit = samples;
  scheduleProbe();
}
function stats() {
  const elapsed = wall === undefined ? 0 : performance.now() - wall;
  const usage = cpu ? process.cpuUsage(cpu) : { user: 0, system: 0 };
  return {
    lines: state.lines, rejected: state.rejected, engine: state.engine.toJSON(),
    cpuMicros: usage.user + usage.system, wallMs: elapsed, gcMs,
    maxLagMs: Number.isFinite(lag.max) ? lag.max / 1e6 : 0,
    windows: state.windows.count, meanWindow: state.windows.count ? state.windowTotal / state.windows.count : 0, p99Window: state.windows.percentile(99),
    probe: options.probe ? Object.fromEntries(Object.entries(state.probe).map(([key, value]) => [key, typeof value === 'number' ? value : value.toJSON()])) : null,
    store: opened?.durability.store !== 'memory' ? opened?.stats() ?? null : null,
    durability: opened?.durability ?? { store: 'memory' }, adapterVersion: build?.adapterVersion ?? 'unknown+unknown',
    port: build?.port ?? 'state', cpus: allowedCpus(), pinned: runnerInfo().pinned,
  };
}
async function exit() {
  if (closing) return;
  closing = true;
  measuring = false;
  clearTimeout(probeTimer);
  clearTimeout(probePending?.timeout);
  try { await engine?.stop(); }
  finally {
    try { await opened?.close(); }
    finally {
      server?.closeAllConnections();
      if (server?.listening) await new Promise((resolve) => server.close(resolve));
      await rm(options.socketPath, { force: true });
      observer.disconnect();
      lag.disable();
    }
  }
}

try {
  build = await loadBuild(options.dist);
  if (options.mode !== 'noop') {
    opened = await openStore(build, options.store);
    engine = startEngine(build, {
      store: opened.store, settle: options.settle,
      onWindow(size, input) {
        if (measuring && input.scope !== 'bench-probe') { state.windows.record(size); state.windowTotal += size; }
      },
      judge: (input) => {
        if (input.scope === 'bench-probe' && probePending && input.window.events.some((event) => event.id === probePending.eventId)) {
          probePending.t1 = performance.now();
          probePending.t2 = performance.now();
          return { route: 'interrupt' };
        }
        return { route: 'note' };
      },
      onDelivered(decision) { if (decision.scope === 'bench-probe' && probePending && decision.window.events.some((event) => event.id === probePending.eventId)) finishProbe(probePending, true); },
    });
  }
  const credit = options.credit ?? 8192;
  const slice = options.slice ?? 64;
  server = http.createServer((request, response) => {
    if (request.method !== 'POST' || request.url !== '/v1/ingest/stream') { response.writeHead(404).end(); return; }
    response.writeHead(200, { 'content-type': 'application/x-ndjson' });
    response.flushHeaders();
    response.write(JSON.stringify({ credit }) + '\n');
    const size = 2 * credit;
    const parsed = new Float64Array(size);
    const rejected = new Uint8Array(size);
    const done = new Uint8Array(size);
    const measured = new Uint8Array(size);
    let queue = [];
    let cursor = 0;
    let partial = '';
    let scheduled = false;
    let lineIndex = 0;
    let through = 0;
    let written = 0;
    let seq = 0;
    let ackScheduled = false;
    let ended = false;
    const flushAck = () => {
      ackScheduled = false;
      const now = performance.now();
      const rejections = [];
      for (let i = written; i < through; i += 1) {
        if (measuring && measured[i % size]) {
          state.lines += 1;
          if (!rejected[i % size]) state.engine.record((now - parsed[i % size]) * 1000);
        }
        if (rejected[i % size]) rejections.push(i);
        rejected[i % size] = 0;
      }
      const count = through - written;
      if (count && !response.destroyed) response.write(JSON.stringify({ ack: ++seq, through, ...(rejections.length ? { rejected: rejections } : {}) }) + '\n' + JSON.stringify({ credit: count }) + '\n');
      written = through;
      if (ended && written === lineIndex && cursor === queue.length) response.end();
    };
    const finish = (index, isRejected) => {
      rejected[index % size] = isRejected ? 1 : 0;
      if (isRejected && measuring && measured[index % size]) state.rejected += 1;
      done[index % size] = 1;
      while (through < lineIndex && done[through % size]) { done[through % size] = 0; through += 1; }
      if (through > written && !ackScheduled) { ackScheduled = true; queueMicrotask(flushAck); }
    };
    const processSlice = () => {
      scheduled = false;
      let count = 0;
      while (cursor < queue.length && count++ < slice) {
        const index = lineIndex++;
        if (index === options.warmLines) startMeasure();
        parsed[index % size] = performance.now();
        measured[index % size] = measuring ? 1 : 0;
        const line = queue[cursor++];
        if (index === options.stall?.atLine) {
          const until = performance.now() + options.stall.ms;
          while (performance.now() < until) {}
        }
        try {
          const { scope, flush, end, ...event } = JSON.parse(line);
          if (typeof scope !== 'string') throw new Error('missing scope');
          let operation;
          if (options.mode !== 'noop') {
            if (flush === true) operation = engine.attend.flush(scope);
            else if (end === true) {
              if (!engine.attend.end) throw new Error('end is unavailable');
              operation = engine.attend.end(scope);
            } else operation = engine.attend.push(scope, [event]);
          }
          Promise.resolve(operation).then(() => finish(index, false), () => finish(index, true));
        } catch { finish(index, true); }
      }
      if (cursor === queue.length) { queue = []; cursor = 0; }
      if (cursor < queue.length) { scheduled = true; setImmediate(processSlice); }
      else if (ended && through === lineIndex) flushAck();
    };
    request.setEncoding('utf8');
    request.on('data', (chunk) => {
      partial += chunk;
      const lines = partial.split('\n');
      partial = lines.pop();
      queue.push(...lines);
      if (!scheduled) { scheduled = true; setImmediate(processSlice); }
    });
    request.on('end', () => {
      ended = true;
      if (partial) queue.push(partial);
      if (!scheduled) { scheduled = true; setImmediate(processSlice); }
    });
    request.on('error', () => response.destroy());
  });
  server.requestTimeout = 0;
  server.headersTimeout = 0;
  server.keepAliveTimeout = 0;
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(options.socketPath, resolve); });
  process.on('message', async (message) => {
    try {
      if (message.type === 'measure') {
        if (message.phase === 'start') startMeasure(message.samples);
        else {
          frozen = stats(); measuring = false; clearTimeout(probeTimer);
        }
        process.send?.({ type: 'measured', requestId: message.requestId });
      } else if (message.type === 'stats') process.send?.({ type: 'stats', stats: frozen ?? stats(), requestId: message.requestId });
      else if (message.type === 'exit') { await exit(); process.exit(0); }
    } catch (error) { process.send?.({ type: 'error', error: error.stack }); await exit(); process.exit(1); }
  });
  process.on('disconnect', () => { exit().finally(() => process.exit(0)); });
  process.send?.({ type: 'ready' });
} catch (error) {
  process.send?.({ type: 'error', error: error.stack });
  await exit();
  process.exit(1);
}
