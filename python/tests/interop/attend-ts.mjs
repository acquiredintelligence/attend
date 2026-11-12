// The TypeScript half of python/tests/test_interop.py. Runs the built package in dist/ over the same Redis or
// Postgres the Python test uses. Usage: node attend-ts.mjs <redis|postgres> <url> <prefix> <write|read> <scope>
import { createAttend } from '../../../dist/index.js';
import { openIntake } from '../../../dist/intake.js';
import { fakeClock } from '../../../dist/testing.js';
import { openStore } from './store.mjs';

const [kind, url, prefix, mode, scope] = process.argv.slice(2);
const kinds = { close: 'closing', say: 'flush', look: 'passive', out: 'consequence', move: 'continuous' };
const judge = async ({ window, routes }) => ({ route: window.events.some((e) => e.name === 'look') && routes.some((r) => r.route === 'wait') ? 'wait' : 'interrupt' });
const delivered = [];

const { store, close } = await openStore(kind, url, prefix);

const clock = fakeClock(10_000);
const attend = createAttend({ store, clock, kind: kinds, judge, sink: (d) => { delivered.push(d.id); }, settle: { closingGraceMs: 100 } });
const intake = mode === 'intake-write' ? openIntake(attend, scope) : null;
const controller = new AbortController();
const running = attend.run({ signal: controller.signal, sweepMs: 60_000 });
if (intake) {
  await intake.submit(async () => ({ events: [{ id: 'raw-a', name: 'move', at: 10_000, data: {}, text: 'x'.repeat(2000) }], cursor: '1' }), { source: 'feed' });
  await intake.submit(async () => ({ events: [], cursor: '2' }), { source: 'feed' });
  await intake.submit(async () => ({ events: [{ id: 'raw-b', name: 'move', at: 10_000, data: {} }], cursor: '3' }), { source: 'feed' });
  await intake.close();
} else if (mode === 'write') {
  await attend.push(scope, [{ id: 'a', name: 'close', text: 'close a' }, { id: 'b', name: 'out', data: { n: 1.5, s: 'ünï 🌍' } }]);
  await clock.advance(100);
  // The store does real I/O, which advancing the fake clock does not wait for.
  for (let i = 0; i < 500 && delivered.length === 0; i += 1) await new Promise((resolve) => setTimeout(resolve, 5));
  await attend.push(scope, [{ id: 'c', name: 'look', text: 'look c' }]);
  await clock.advance(4000);
  await attend.push(scope, [{ id: 'd', name: 'out', text: 'out d' }]);
} else if (mode === 'pending') {
  await attend.push(scope, [{ id: 'p', name: 'close', text: 'close p' }]);
} else if (mode === 'continue') {
  await attend.push(scope, [{ id: 'e', name: 'say', text: 'say e' }]);
  for (let i = 0; i < 500 && delivered.length === 0; i += 1) {
    await clock.advance(0);
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}
controller.abort();
await running;
const record = await attend.inspect(scope);
const { rows } = await store.log.read({ after: null, scope, limit: 100 });
await close();
process.stdout.write(JSON.stringify({ record, rows, delivered }));
