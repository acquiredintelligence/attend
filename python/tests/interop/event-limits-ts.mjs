import assert from 'node:assert/strict';
import { createAttend } from '../../../dist/index.js';
import { openIntake } from '../../../dist/intake.js';
import { fakeClock } from '../../../dist/testing.js';
import { openStore } from './store.mjs';

const [kind, url, prefix, mode, scope, phase] = process.argv.slice(2);
const owned = await openStore(kind, url, prefix);
const clock = fakeClock(mode === 'write' ? 10000 : 10015);
const done = new AbortController();
const finished = new Promise((resolve) => done.signal.addEventListener('abort', resolve, { once: true }));
const calls = [];
const event = (id, name = 'reading') => ({ id, name, at: clock.now(), data: {}, text: 'q'.repeat(128) });
const engine = createAttend({ store: owned.store, clock, kind: { reading: 'continuous', meta: 'context', result: 'consequence' }, sink: 'log',
  settle: { continuation: 'accumulate', eventLimits: { counted: mode === 'refuse' ? 4 : 3, background: 2 }, streamGapMs: 10, waitHorizonMs: 20, retryBackoffMs: [30] },
  judge: async (input) => {
    calls.push({ at: clock.now(), ids: input.window.events.map((e) => e.id), wait: input.routes.some((r) => r.route === 'wait'), dropped: input.window.dropped, gap: input.window.gap });
    if (mode === 'write' && phase === 'retrying') throw new Error('controlled');
    return { route: mode === 'write' ? 'wait' : 'queue' };
  },
  observe(trace) {
    if ((mode === 'write' && (trace.kind === 'waiting' || trace.kind === 'retrying')) || (mode === 'continue' && trace.kind === 'judged')) done.abort();
  },
});
try {
  if (mode === 'read') {
    console.log(JSON.stringify({ record: await engine.inspect(scope) }));
  } else if (mode === 'refuse') {
    const before = await owned.store.load(scope);
    await assert.rejects(engine.push(scope, event('refused', 'meta')), /unresolved/);
    assert.deepEqual(await owned.store.load(scope), before);
    console.log(JSON.stringify({ rejected: true, unchanged: true }));
  } else {
    const intake = openIntake(engine, scope), running = engine.run();
    try {
      if (mode === 'write') {
        await intake.submit(async () => ({ events: [event('a'), event('x', 'meta'), event('y', 'result')], cursor: '1' }), { source: 'feed' });
        await clock.advance(10); await finished;
        await clock.advance(5);
        await intake.submit(async () => ({ events: [event('z', 'meta')], cursor: '2', truncated: true }), { source: 'feed' });
      } else {
        await intake.submit(async () => ({ events: [event('b'), event('c')], cursor: '3' }), { source: 'feed' });
        if (phase === 'retrying') {
          await clock.advance(24); assert.equal(calls.length, 0);
          await clock.advance(1);
        }
        await finished;
      }
    } finally { await intake.close(); await engine.stop(); await running; }
    console.log(JSON.stringify({ record: await engine.inspect(scope), calls }));
  }
} finally { await owned.close(); }
