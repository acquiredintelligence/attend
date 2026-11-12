import { createAttend } from '../../../dist/index.js';
import { openIntake } from '../../../dist/intake.js';
import { fakeClock } from '../../../dist/testing.js';
import { openStore } from './store.mjs';

const [kind, url, prefix, mode, scope] = process.argv.slice(2);
const { store, close } = await openStore(kind, url, prefix);
const baseClock = fakeClock(10_000);
const retrying = Promise.withResolvers(), waiting = Promise.withResolvers(), armed = Promise.withResolvers(), delivered = Promise.withResolvers();
const calls = [], retries = [], deliveries = [];
const clock = { now: baseClock.now, at(at, fn) { const cancel = baseClock.at(at, fn); if (at === 10_060) armed.resolve(); return cancel; } };
const engine = createAttend({ store, clock, kind: { reading: 'continuous' },
  settle: { continuation: 'accumulate', streamGapMs: 10, waitHorizonMs: 20, retryBackoffMs: [30, 60] },
  observe(trace) {
    if (trace.kind === 'retrying' && trace.stage === 'judge') { retries.push(trace.at); retrying.resolve(); }
    if (trace.kind === 'waiting') waiting.resolve();
    if (trace.kind === 'delivered') delivered.resolve();
  },
  judge: async (input) => {
    calls.push({ at: clock.now(), waits: input.window.waits });
    if (calls.length === 1) throw new Error('controlled outage');
    return { route: mode === 'write' ? 'wait' : 'queue' };
  },
  sink: async (decision) => { deliveries.push(decision.id); },
});
const intake = openIntake(engine, scope), running = engine.run();
try {
  if (mode === 'write') {
    await intake.submit(async () => ({ events: [{ id: 'a', name: 'reading', at: 10_000, data: {}, text: 'x'.repeat(1000) }], cursor: '1' }), { source: 'feed' });
    await baseClock.advance(10); await retrying.promise;
    await baseClock.advance(30); await waiting.promise;
  } else {
    await armed.promise;
    await baseClock.advance(60); await retrying.promise;
    await baseClock.advance(60); await delivered.promise;
  }
} finally {
  await intake.close(); await engine.stop(); await running;
}
try {
  process.stdout.write(JSON.stringify({ record: await engine.inspect(scope), calls, retries, deliveries }));
} finally { await close(); }
