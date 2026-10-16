// The server process the interop test kills and restarts. It runs the built package:
// createAttend over a durable store, eventsServer with the stream, and NDJSON ingest.
//
// Environment: INTEROP_STORE (sqlite | postgres), INTEROP_SQLITE_FILE, INTEROP_PG_URL,
// INTEROP_PG_SCHEMA, INTEROP_TRANSPORT (http | stdio), INTEROP_PORT, INTEROP_INGEST_PORT and
// INTEROP_HEARTBEAT_MS.
// In http mode one listener serves /mcp and /v1/ingest. In stdio mode MCP runs on stdin and
// stdout and a second listener serves only /v1/ingest.
import { createServer } from 'node:http';
import { fileURLToPath } from 'node:url';
import { resolve, dirname } from 'node:path';

const dist = resolve(dirname(fileURLToPath(import.meta.url)), '../../dist');
const load = (file) => import(resolve(dist, file));
const { createAttend } = await load('attend.js');
const { eventsServer, ndjsonIngest } = await load('server.js');

const env = process.env;
const ROUTES = ['interrupt', 'queue', 'note', 'ask'];
const hash = (text) => { let h = 0; for (const c of text) h = (h * 31 + c.charCodeAt(0)) >>> 0; return h; };

let store;
let pool;
if (env.INTEROP_STORE === 'postgres') {
  const pg = (await import('pg')).default;
  const { migrate, postgresStore } = await load('store/postgres.js');
  pool = new pg.Pool({ connectionString: env.INTEROP_PG_URL, max: 4, options: `-c search_path=${env.INTEROP_PG_SCHEMA}` });
  await migrate(pool);
  store = postgresStore(pool);
} else {
  const { sqliteStore } = await load('store/sqlite.js');
  store = sqliteStore({ file: env.INTEROP_SQLITE_FILE, worker: true });
}

const attend = createAttend({
  store,
  kind: () => 'flush',
  sink: 'log',
  deadLetter: 'log',
  // A kill during a judge call leaves the window in flight until its timeout; the defaults would hold a scope for about 26 s.
  settle: { judgeTimeoutMs: 1000, deliveryTimeoutMs: 1000, retryBackoffMs: [100, 200, 400] },
  judge: async (input) => {
    const route = ROUTES[hash(input.window.id) % ROUTES.length];
    return { route, probabilities: { [route]: 1 } };
  },
});
const events = eventsServer(store.log, { heartbeatMs: Number(env.INTEROP_HEARTBEAT_MS ?? 1000) });
const abort = new AbortController();
const running = attend.run({ signal: abort.signal, sweepMs: 100 });
const ingest = ndjsonIngest(attend);

const listen = (handler, port) => new Promise((ready) => {
  const server = createServer((req, res) => { void Promise.resolve(handler(req, res)).catch(() => res.destroy()); });
  server.listen(port, '127.0.0.1', () => ready(server));
});

const servers = [];
if (env.INTEROP_TRANSPORT === 'stdio') {
  const { serveStdio } = await import('@modelcontextprotocol/server/stdio');
  const { Server } = await import('@modelcontextprotocol/server');
  serveStdio(() => {
    const server = new Server({ name: 'attend-interop', version: '0' }, { capabilities: {} });
    events.install(server);
    return server;
  });
  servers.push(await listen((req, res) => {
    if (req.method === 'POST' && req.url === '/v1/ingest') return ingest(req, res);
    res.writeHead(404); res.end();
  }, Number(env.INTEROP_INGEST_PORT)));
} else {
  servers.push(await listen(events.http({ routes: { 'POST /v1/ingest': ingest } }), Number(env.INTEROP_PORT)));
}
process.stderr.write('ready\n');

let closing = false;
async function shutdown() {
  if (closing) return;
  closing = true;
  abort.abort();
  await events.close();
  for (const server of servers) server.closeAllConnections();
  await running.catch(() => undefined);
  await store.close?.();
  await pool?.end();
  process.exit(0);
}
process.on('SIGTERM', () => void shutdown());
process.stdin.on('end', () => { if (env.INTEROP_TRANSPORT === 'stdio') void shutdown(); });
