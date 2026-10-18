#!/usr/bin/env node
// Measures attend serve's hops on its real HTTP routes: POST /v1/ingest to its reply after commit, a decision's commit to its arrival on a waiting events/stream subscriber, and ingest to arrival end to end. The server is the build's serve(), which the CLI runs; the client shares its process, so a commit and an arrival are timed on one clock. Each sample waits a random gap first, so commits land at random phases of any poll. Usage: node bench/serve.mjs [--dist dist] [--store memory|sqlite|postgres|redis] [--samples 200] [--gap-ms 250] [--out file.jsonl]
import { performance } from 'node:perf_hooks';
import { setTimeout as delay } from 'node:timers/promises';
import { args, positive, storeOptions, storeSpec, emit } from './lib/cli.mjs';
import { loadBuild } from './lib/build.mjs';
import { openStore } from './lib/store.mjs';
import { createHistogram } from './lib/histogram.mjs';
import { runnerInfo, finishRunner } from './lib/runner.mjs';

const WARMUP = 10;
const TIMEOUT_MS = 5000;
let opened;
let server;
const subscriber = new AbortController();
try {
  const options = args({ dist: 'dist', ...storeOptions, samples: '200', 'gap-ms': '250', out: undefined });
  const samples = positive(options.samples, 'samples');
  const gapMs = positive(options['gap-ms'], 'gap-ms', true);
  const runner = runnerInfo();
  const build = await loadBuild(options.dist);
  opened = await openStore(build, storeSpec(options));
  const committedAt = new Map();
  const store = { ...opened.store, async commit(writes) {
    const outcomes = await opened.store.commit(writes);
    const at = performance.now();
    for (const [i, write] of writes.entries()) if (outcomes[i] === 'ok') for (const row of write.appends) if (row.kind === 'decision') committedAt.set(row.id, at);
    return outcomes;
  } };
  const attend = build.core.createAttend({
    store, kind: (event) => (event.name === 'user.said' ? 'flush' : 'consequence'),
    judge: async ({ window }) => ({ route: window.reason === 'flush' ? 'interrupt' : 'queue' }), sink: 'log', deadLetter: 'log',
  });
  server = (await build.module('server')).serve(attend, store, { listen: { port: 0 } });
  await server.ready;
  const base = server.url();

  const arrivals = new Map();
  let active;
  const subscribed = new Promise((resolve) => { active = resolve; });
  const response = await fetch(`${base}/mcp`, {
    method: 'POST', signal: subscriber.signal,
    headers: { 'content-type': 'application/json', accept: 'application/json, text/event-stream', 'mcp-protocol-version': '2026-07-28', 'mcp-method': 'events/stream' },
    body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'events/stream', params: { name: 'attention.decision' } }),
  });
  void (async () => {
    const decoder = new TextDecoder();
    let buffer = '';
    try {
      for await (const chunk of response.body) {
        buffer += decoder.decode(chunk, { stream: true });
        for (let split = buffer.indexOf('\n\n'); split >= 0; split = buffer.indexOf('\n\n')) {
          const frame = buffer.slice(0, split);
          buffer = buffer.slice(split + 2);
          if (!frame.startsWith('data: ')) continue;
          const at = performance.now();
          const message = JSON.parse(frame.slice(6));
          if (message.method === 'notifications/events/active') active();
          const data = message.params?.data;
          if (data?.decisionId) for (const id of data.eventIds) arrivals.get(id)?.({ at, decisionId: data.decisionId });
        }
      }
    } catch { /* aborted at exit */ }
  })();
  await subscribed;

  const ingestUs = createHistogram();
  const commitToSubscriberUs = createHistogram();
  const endToEndUs = createHistogram();
  let errors = 0;
  for (let i = 0; ingestUs.count + errors < samples; i += 1) {
    await delay(5 + Math.random() * gapMs);
    const id = `s${i}`;
    let timer;
    const arrived = new Promise((resolve) => { arrivals.set(id, resolve); timer = setTimeout(resolve, TIMEOUT_MS, null); });
    const sent = performance.now();
    const reply = await fetch(`${base}/v1/ingest`, { method: 'POST', headers: { 'content-type': 'application/x-ndjson' }, body: `${JSON.stringify({ scope: 'bench-serve', id, at: Date.now(), name: 'user.said', data: {}, text: 'now' })}\n` });
    const body = await reply.json().catch(() => null);
    const replied = performance.now();
    const arrival = await arrived;
    clearTimeout(timer);
    arrivals.delete(id);
    // A commit and its first reader can resolve in either order across connections; give the commit a moment.
    if (arrival && !committedAt.has(arrival.decisionId)) await delay(50);
    const committed = arrival && committedAt.get(arrival.decisionId);
    if (arrival) committedAt.delete(arrival.decisionId);
    if (i < WARMUP) continue;
    if (!reply.ok || body?.accepted !== 1 || committed === undefined || committed === null) { errors += 1; continue; }
    ingestUs.record((replied - sent) * 1000);
    commitToSubscriberUs.record(Math.max(0, arrival.at - committed) * 1000);
    endToEndUs.record((arrival.at - sent) * 1000);
  }
  const spread = (histogram) => ({ p50: histogram.percentile(50), p90: histogram.percentile(90), p99: histogram.percentile(99), max: histogram.max });
  emit({
    report: 'serve', durability: opened.durability, samples: ingestUs.count, errors,
    ingestUs: spread(ingestUs), commitToSubscriberUs: spread(commitToSubscriberUs), endToEndUs: spread(endToEndUs),
    adapterVersion: build.adapterVersion, runner: finishRunner(runner),
  }, options.out);
  if (errors > 0) {
    console.error(`${errors} samples failed; ${ingestUs.count} of ${samples} samples`);
    process.exitCode = 1;
  }
} catch (error) { console.error(error.stack); process.exitCode = 1; }
finally {
  subscriber.abort();
  await server?.close().catch(() => {});
  await opened?.close();
}
