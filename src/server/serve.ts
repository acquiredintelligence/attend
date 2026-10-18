import { createServer, type Server } from 'node:http';
import { readFileSync } from 'node:fs';
import type { AddressInfo } from 'node:net';
import { createAttend } from '../attend.js';
import { KINDS, type Event, type Judge, type Kind } from '../core/types.js';
import { memoryStore } from '../memory.js';
import type { Attend } from '../shell/types.js';
import type { ScopeStore } from '../store/types.js';
import { eventsServer } from './install.js';
import { ndjsonIngest } from './ingest.js';
import type { HttpRoute, PrincipalResolver, ServeOptions } from './types.js';

/** What `attend serve` runs: attend.run, eventsServer over store.log, ndjsonIngest, one HTTP listener. */
export function serve(attend: Attend, store: ScopeStore, options: ServeOptions): { url(): string; ready: Promise<void>; closed: Promise<void>; close(): Promise<void> } {
  const events = eventsServer(store.log, { ...options.events, ...(options.principal ? { principal: options.principal } : {}) });
  const ingest = options.principal ? { principal: options.principal } : {};
  const routes: Record<string, HttpRoute> = { 'POST /v1/ingest': ndjsonIngest(attend, ingest) };
  if (options.ingest?.stream !== false) routes['POST /v1/ingest/stream'] = ndjsonIngest(attend, { ...ingest, stream: true });
  const handler = events.http({ path: '/mcp', routes });
  const server: Server = createServer((req, res) => { void Promise.resolve(handler(req, res)).catch(() => res.destroy()); });
  const controller = new AbortController();
  let failure: { error: unknown } | undefined;
  let closing: Promise<void> | undefined;
  let address = '';

  const running = attend.run({ signal: controller.signal }).catch((error: unknown) => { failure = { error }; void close().catch(() => undefined); });
  const ready = new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.once('listening', () => {
      const bound = server.address();
      address = typeof bound === 'string' || bound === null
        ? `http+unix://${encodeURIComponent(bound ?? '')}`
        : `http://${bound.family === 'IPv6' ? `[${bound.address}]` : bound.address}:${(bound as AddressInfo).port}`;
      resolve();
    });
  });
  ready.catch(() => { void close().catch(() => undefined); });
  const closed = new Promise<void>((resolve) => server.once('close', () => resolve()));
  if ('path' in options.listen) server.listen(options.listen.path);
  else server.listen(options.listen.port, options.listen.host ?? '127.0.0.1');

  function close(): Promise<void> {
    closing ??= (async () => {
      controller.abort();
      await running;
      await events.close();
      server.closeAllConnections();
      if (server.listening) await new Promise<void>((resolve) => server.close(() => resolve()));
      if (failure) throw failure.error;
    })();
    return closing;
  }

  return {
    url() {
      if (!address) throw new Error('attend: serve is not listening yet; await ready');
      return address;
    },
    ready,
    closed,
    close,
  };
}

export interface CliIo {
  out?: (text: string) => void;
  err?: (text: string) => void;
  /** Stops the server like SIGINT would. Without it the process's own signals do. */
  signal?: AbortSignal;
}

const USAGE = 'usage: attend serve [--store memory|sqlite:<file>|postgres:<url>|redis:<url>] [--judge rule|jev|decisions] [--kinds <json|file>] [--host h] [--port n] [--socket path] [--act-threshold n] [--token t]';
const HELP = `${USAGE}

  flag              env                    default     meaning
  --store           ATTEND_STORE           memory      where state and the decision log live
  --judge           ATTEND_JUDGE           rule        rule | jev (TYPESAFE_API_KEY) | decisions (OPENAI_API_KEY, --judge-model)
  --judge-model     ATTEND_JUDGE_MODEL                 model id for the decisions judge
  --kinds           ATTEND_KINDS                       event name -> kind, inline JSON or a JSON file
  --default-kind    ATTEND_DEFAULT_KIND    consequence kind for names --kinds does not list
  --host            ATTEND_HOST            127.0.0.1
  --port            ATTEND_PORT            8787        0 picks a free port
  --socket          ATTEND_SOCKET                      listen on a unix socket instead
  --act-threshold   ATTEND_ACT_THRESHOLD   off         number in (0, 1]
  --token           ATTEND_TOKEN                       require Authorization: Bearer <token>
`;

class UsageError extends Error {}

const FLAGS: Record<string, string> = {
  store: 'ATTEND_STORE', judge: 'ATTEND_JUDGE', 'judge-model': 'ATTEND_JUDGE_MODEL', kinds: 'ATTEND_KINDS', 'default-kind': 'ATTEND_DEFAULT_KIND',
  host: 'ATTEND_HOST', port: 'ATTEND_PORT', socket: 'ATTEND_SOCKET', 'act-threshold': 'ATTEND_ACT_THRESHOLD', token: 'ATTEND_TOKEN',
};

function parseFlags(args: string[], env: NodeJS.ProcessEnv): Record<string, string | undefined> {
  const given: Record<string, string> = {};
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i]!;
    const match = /^--([a-z-]+)(?:=(.*))?$/s.exec(arg);
    if (!match || !(match[1]! in FLAGS)) throw new UsageError(`unknown argument ${arg}`);
    const value = match[2] ?? args[++i];
    if (value === undefined) throw new UsageError(`--${match[1]} needs a value`);
    given[match[1]!] = value;
  }
  return Object.fromEntries(Object.entries(FLAGS).map(([flag, name]) => [flag, given[flag] ?? env[name]]));
}

function kindFunction(flags: Record<string, string | undefined>): (event: Event) => Kind {
  const fallback = flags['default-kind'] ?? 'consequence';
  if (!(KINDS as readonly string[]).includes(fallback)) throw new UsageError(`--default-kind must be one of ${KINDS.join(', ')}`);
  let map: Record<string, unknown> = {};
  const source = flags.kinds;
  if (source !== undefined && source !== '') {
    try { map = JSON.parse(source.trimStart().startsWith('{') ? source : readFileSync(source, 'utf8')); }
    catch (error) { throw new UsageError(`--kinds is not a JSON object or a readable JSON file: ${error instanceof Error ? error.message : String(error)}`); }
    if (typeof map !== 'object' || map === null || Array.isArray(map)) throw new UsageError('--kinds must be a JSON object of event name to kind');
    for (const [name, kind] of Object.entries(map)) {
      if (typeof kind !== 'string' || !(KINDS as readonly string[]).includes(kind)) throw new UsageError(`--kinds: "${name}" must map to one of ${KINDS.join(', ')}`);
    }
  }
  return (event) => (Object.hasOwn(map, event.name) ? map[event.name] : fallback) as Kind;
}

async function judgeFor(flags: Record<string, string | undefined>, env: NodeJS.ProcessEnv): Promise<Judge> {
  switch (flags.judge ?? 'rule') {
    case 'rule': return async ({ window }) => ({ route: window.reason === 'flush' ? 'interrupt' : 'queue' });
    case 'jev': {
      if (!env.TYPESAFE_API_KEY) throw new UsageError('--judge jev needs TYPESAFE_API_KEY in the environment');
      return (await import('../jev.js')).jevJudge({ apiKey: env.TYPESAFE_API_KEY });
    }
    case 'decisions': {
      if (!env.OPENAI_API_KEY) throw new UsageError('--judge decisions needs OPENAI_API_KEY in the environment');
      if (!flags['judge-model']) throw new UsageError('--judge decisions needs --judge-model (or ATTEND_JUDGE_MODEL)');
      return (await import('../decisions.js')).decisionsJudge({ apiKey: env.OPENAI_API_KEY, model: flags['judge-model'] });
    }
    default: throw new UsageError(`--judge must be rule, jev or decisions, not "${flags.judge}"`);
  }
}

async function storeFor(spec: string, env: NodeJS.ProcessEnv, cleanup: (() => Promise<void>)[]): Promise<ScopeStore> {
  if (spec === 'memory') return memoryStore();
  const missing = (name: string) => new UsageError(`--store ${spec.split(':')[0]} needs the "${name}" package: npm install ${name}`);
  if (spec.startsWith('sqlite:')) {
    const file = spec.slice('sqlite:'.length);
    if (!file) throw new UsageError('--store sqlite:<file> needs a file path');
    const { DatabaseSync } = await import('node:sqlite');
    const db = new DatabaseSync(file);
    cleanup.push(async () => db.close());
    return (await import('../sqlite.js')).sqliteStore(db);
  }
  if (spec === 'postgres' || spec.startsWith('postgres:') || spec.startsWith('postgresql:')) {
    const url = spec === 'postgres' ? env.DATABASE_URL : spec.startsWith('postgres://') || spec.startsWith('postgresql://') ? spec : spec.slice('postgres:'.length);
    if (!url) throw new UsageError('--store postgres needs a URL: postgres:<url> or DATABASE_URL');
    const pg = await import('pg').catch(() => { throw missing('pg'); });
    const { Client, Pool } = pg.default ?? pg;
    const pool = new Pool({ connectionString: url });
    cleanup.push(() => pool.end());
    const { migrate, postgresStore } = await import('../postgres.js');
    await migrate(pool);
    // LISTEN belongs to its session, so the decision log hears NOTIFY on a client of its own. Without it, subscribers hear a decision on the next 100 ms poll.
    const listening = new Client({ connectionString: url });
    await listening.connect();
    cleanup.push(() => listening.end());
    return postgresStore(pool, { listener: {
      async listen(channel, onNotify) {
        const notified = (message: { channel: string }) => { if (message.channel === channel) onNotify(); };
        listening.on('notification', notified);
        await listening.query(`LISTEN "${channel}"`);
        return async () => { listening.off('notification', notified); await listening.query(`UNLISTEN "${channel}"`); };
      },
    } });
  }
  if (spec === 'redis' || spec.startsWith('redis:') || spec.startsWith('rediss:')) {
    const url = spec === 'redis' ? env.REDIS_URL : spec.startsWith('redis://') || spec.startsWith('rediss://') ? spec : spec.slice('redis:'.length);
    if (!url) throw new UsageError('--store redis needs a URL: redis:<url> or REDIS_URL');
    const redis = await import('redis').catch(() => { throw missing('redis'); });
    const client = redis.createClient({ url });
    await client.connect();
    cleanup.push(async () => { client.destroy(); });
    // XREAD BLOCK holds its connection, so the decision log waits on one of its own. Without it, subscribers hear a decision on the next 250 ms poll.
    const blocking = client.duplicate();
    await blocking.connect();
    cleanup.push(async () => { blocking.destroy(); });
    return (await import('../redis.js')).redisStore((args) => client.sendCommand(args), { blockingSend: (args) => blocking.sendCommand(args) });
  }
  throw new UsageError(`--store must be memory, sqlite:<file>, postgres:<url> or redis:<url>, not "${spec}"`);
}

/** The `attend` command. Returns the exit code once the server has shut down. */
export async function cli(argv: string[], env: NodeJS.ProcessEnv, io: CliIo = {}): Promise<number> {
  const out = io.out ?? ((text) => void process.stdout.write(text));
  const err = io.err ?? ((text) => void process.stderr.write(text));
  if (argv.includes('--help') || argv.includes('-h')) { out(HELP); return 0; }
  const cleanup: (() => Promise<void>)[] = [];
  let store: ScopeStore | undefined;
  let server: ReturnType<typeof serve> | undefined;
  try {
    if (argv[0] !== 'serve') throw new UsageError(argv[0] === undefined ? 'missing command' : `unknown command "${argv[0]}"`);
    const flags = parseFlags(argv.slice(1), env);
    const kind = kindFunction(flags);
    const threshold = flags['act-threshold'] === undefined ? undefined : Number(flags['act-threshold']);
    if (threshold !== undefined && !(threshold > 0 && threshold <= 1)) throw new UsageError('--act-threshold must be a number in (0, 1]');
    const port = flags.port === undefined ? 8787 : Number(flags.port);
    if (!Number.isInteger(port) || port < 0 || port > 65535) throw new UsageError('--port must be an integer from 0 to 65535');
    const judge = await judgeFor(flags, env);
    await import('@modelcontextprotocol/server').catch(() => { throw new Error('attend serve needs the MCP server package: npm install @modelcontextprotocol/server zod'); });
    store = await storeFor(flags.store ?? 'memory', env, cleanup);
    const token = flags.token;
    const principal: PrincipalResolver | undefined = token ? (req) => (req.headers.authorization === `Bearer ${token}` ? { id: 'attend', scopes: [''] } : null) : undefined;
    const attend = createAttend({ store, judge, kind, sink: 'log', deadLetter: 'log', ...(threshold === undefined ? {} : { actThreshold: threshold }) });
    server = serve(attend, store, { listen: flags.socket ? { path: flags.socket } : { port, host: flags.host ?? '127.0.0.1' }, ...(principal ? { principal } : {}) });
    await server.ready;
    const url = server.url();
    out(`attend serving ${url}  (ingest POST ${url}/v1/ingest, MCP ${url}/mcp)\n`);
    const stop = new AbortController();
    const onSignal = () => stop.abort();
    if (io.signal) io.signal.addEventListener('abort', onSignal, { once: true });
    else { process.once('SIGINT', onSignal); process.once('SIGTERM', onSignal); }
    if (io.signal?.aborted) stop.abort();
    await Promise.race([new Promise<void>((resolve) => stop.signal.addEventListener('abort', () => resolve(), { once: true })), server.closed]);
    process.off('SIGINT', onSignal);
    process.off('SIGTERM', onSignal);
    const finished = server;
    server = undefined;
    await finished.close();
    return 0;
  } catch (error) {
    if (error instanceof UsageError) { err(`attend: ${error.message}\n${USAGE}\n`); return 2; }
    err(`attend: ${error instanceof Error ? error.message : String(error)}\n`);
    return 1;
  } finally {
    await server?.close().catch((error: unknown) => { err(`attend: ${error instanceof Error ? error.message : String(error)}\n`); });
    await store?.close?.();
    for (const close of cleanup) await close().catch(() => undefined);
  }
}
