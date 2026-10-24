// The TypeScript half of python/tests/test_interop.py. Runs the built package in dist/ over the same Redis or
// Postgres the Python test uses. Usage: node attend-ts.mjs <redis|postgres> <url> <prefix> <write|read> <scope>
import { createAttend } from '../../../dist/index.js';
import { fakeClock } from '../../../dist/testing.js';

const [kind, url, prefix, mode, scope] = process.argv.slice(2);
const kinds = { close: 'closing', say: 'flush', look: 'passive', out: 'consequence' };
const judge = async ({ window, routes }) => ({ route: window.events.some((e) => e.name === 'look') && routes.some((r) => r.route === 'wait') ? 'wait' : 'interrupt' });
const delivered = [];

let store;
let close = async () => {};
if (kind === 'redis') {
  const { createClient } = await import('redis');
  const { redisStore } = await import('../../../dist/redis.js');
  const client = createClient({ url });
  await client.connect();
  store = redisStore((args) => client.sendCommand(args), { prefix });
  close = () => client.close();
} else {
  const pg = (await import('pg')).default;
  const { postgresStore } = await import('../../../dist/postgres.js');
  const pool = new pg.Pool({ connectionString: url, max: 2 });
  store = postgresStore(pool, { prefix });
  close = () => pool.end();
}

const clock = fakeClock(10_000);
const attend = createAttend({ store, clock, kind: kinds, judge, sink: (d) => { delivered.push(d.id); }, settle: { closingGraceMs: 100 } });
const controller = new AbortController();
const running = attend.run({ signal: controller.signal, sweepMs: 60_000 });
if (mode === 'write') {
  await attend.push(scope, [{ id: 'a', name: 'close', text: 'close a' }, { id: 'b', name: 'out', data: { n: 1.5, s: 'ünï 🌍' } }]);
  await clock.advance(100);
  // The store does real I/O, which advancing the fake clock does not wait for.
  for (let i = 0; i < 500 && delivered.length === 0; i += 1) await new Promise((resolve) => setTimeout(resolve, 5));
  await attend.push(scope, [{ id: 'c', name: 'look', text: 'look c' }]);
  await clock.advance(4000);
  await attend.push(scope, [{ id: 'd', name: 'out', text: 'out d' }]);
}
controller.abort();
await running;
const record = await attend.inspect(scope);
const { rows } = await store.log.read({ after: null, scope, limit: 100 });
await store.close?.();
await close();
process.stdout.write(JSON.stringify({ record, rows, delivered }));
