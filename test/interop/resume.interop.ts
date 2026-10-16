/**
 * An independent client resumes from a persisted cursor (design 11.7). A scripted producer drives
 * 2,000 decisions across 50 scopes into a server process running the built package while the
 * client's connection is aborted at random points, the server is killed with SIGKILL and
 * restarted twice, and the log is trimmed once past the client's cursor.
 *
 * Requires `pnpm build` first: the server process loads `dist/`.
 */
import { randomUUID } from 'node:crypto';
import { createServer } from 'node:net';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import type { ChildProcess } from 'node:child_process';
import pg from 'pg';
import { describe, expect, test } from 'vitest';
import { migrate, postgresStore } from '../../src/store/postgres.js';
import { sqliteStore } from '../../src/store/sqlite.js';
import type { DecisionQuery, DecisionRow, ScopeStore } from '../../src/store/types.js';
import { CursorFile, HttpLink, StdioLink, newLedger, pollLoop, pushLoop, sleep, spawnNode, type Control, type Link } from './raw-client.js';

const SCOPES = 50;
const ROUNDS = 40;
const DECISIONS = SCOPES * ROUNDS;
const HEARTBEAT_MS = 500;
const POSTGRES_URL = process.env.ATTEND_POSTGRES_URL ?? 'postgres://postgres:attend@127.0.0.1:15432/attend';
const SEED = Number(process.env.INTEROP_SEED ?? 20261016);
const SERVER = resolve(import.meta.dirname, 'server.mjs');
const CLI = resolve(import.meta.dirname, '../../dist/cli.js');
const SUBSCRIPTION = { name: 'attention.decision', arguments: { scopePrefix: 'ops/', routes: ['interrupt', 'ask'] } };

function random(seed: number) {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const scopeName = (index: number) => `${index < SCOPES / 2 ? 'ops' : 'inbox'}/${String(index).padStart(2, '0')}`;

async function freePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((ready) => server.listen(0, '127.0.0.1', ready));
  const port = (server.address() as { port: number }).port;
  await new Promise<void>((done) => server.close(() => done()));
  return port;
}

async function readAll(store: ScopeStore, filter: Partial<DecisionQuery> = {}): Promise<DecisionRow[]> {
  const rows: DecisionRow[] = [];
  let after: string | null = null;
  for (;;) {
    const page = await store.log.read({ ...filter, after, limit: 500, scanLimit: 100_000, kinds: ['decision'] });
    rows.push(...page.rows);
    if (!page.more) return rows;
    after = page.checked;
  }
}

const matching = (rows: DecisionRow[]) => rows.filter((row) => row.scope.startsWith('ops/') && (row.route === 'interrupt' || row.route === 'ask'));

async function until(what: string, check: () => boolean | Promise<boolean>, timeoutMs = 60_000, detail: () => Promise<unknown> | unknown = () => undefined) {
  const deadline = Date.now() + timeoutMs;
  while (!(await check())) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}: ${JSON.stringify(await detail())}`);
    await sleep(25);
  }
}

class ServerProcess {
  child: ChildProcess | undefined;
  starts = 0;
  constructor(private readonly spawnServer: () => ChildProcess) {}
  async start() {
    const child = this.spawnServer();
    this.child = child;
    this.starts++;
    let output = '';
    await new Promise<void>((ready, fail) => {
      const check = (chunk: Buffer) => { output += chunk.toString(); if (/\bready\b|attend serving/.test(output)) ready(); };
      child.stdout!.on('data', check);
      child.stderr!.on('data', check);
      child.once('exit', () => fail(new Error(`server exited early: ${output}`)));
    });
  }
  async kill() {
    const child = this.child;
    if (!child || child.exitCode !== null || child.signalCode !== null) return;
    const exited = new Promise<void>((done) => child.once('exit', () => done()));
    child.kill('SIGKILL');
    await exited;
  }
}

type Store = 'sqlite' | 'postgres';
type Transport = 'http' | 'stdio';
type Mode = 'push' | 'poll';
/** `script` is test/interop/server.mjs; `cli` is the built `attend serve` (dist/cli.js), which cannot run the SQLite worker or stdio. */
type Server = 'script' | 'cli';

async function scenario(storeKind: Store, transport: Transport, mode: Mode, server: Server) {
  const roll = random(SEED);
  const dir = mkdtempSync(join(tmpdir(), 'attend-interop-'));
  const schema = `interop_${randomUUID().replaceAll('-', '').slice(0, 12)}`;
  const admin = storeKind === 'postgres' ? new pg.Pool({ connectionString: POSTGRES_URL, max: 2 }) : undefined;
  const ingestPort = await freePort();
  const mcpPort = await freePort();
  const env: Record<string, string> = {
    INTEROP_STORE: storeKind,
    INTEROP_SQLITE_FILE: join(dir, 'attend.db'),
    INTEROP_PG_URL: POSTGRES_URL,
    INTEROP_PG_SCHEMA: schema,
    INTEROP_TRANSPORT: transport,
    INTEROP_PORT: String(mcpPort),
    INTEROP_INGEST_PORT: String(ingestPort),
    INTEROP_HEARTBEAT_MS: String(HEARTBEAT_MS),
  };
  const ingestUrl = `http://127.0.0.1:${transport === 'http' ? mcpPort : ingestPort}/v1/ingest`;
  let pool: pg.Pool | undefined;
  if (admin) {
    await admin.query(`CREATE SCHEMA ${schema}`);
    pool = new pg.Pool({ connectionString: POSTGRES_URL, max: 2, options: `-c search_path=${schema}` });
    await migrate(pool);
  }
  const store = pool ? postgresStore(pool) : sqliteStore({ file: env.INTEROP_SQLITE_FILE!, worker: true });

  const cliArgs = ['serve', '--store', `postgres:${POSTGRES_URL}?options=${encodeURIComponent(`-c search_path=${schema}`)}`, '--port', String(mcpPort), '--default-kind', 'flush'];
  const proc = new ServerProcess(() => server === 'cli' ? spawnNode(CLI, {}, cliArgs) : spawnNode(SERVER, env));
  let link: Link;
  let kill: () => Promise<void>;
  if (transport === 'http') {
    await proc.start();
    link = new HttpLink(`http://127.0.0.1:${mcpPort}/mcp`);
    kill = async () => {
      await proc.kill();
      void sleep(100 + roll() * 300).then(() => proc.start());
    };
  } else {
    const stdio = new StdioLink(() => {
      const child = spawnNode(SERVER, env);
      child.stderr!.on('data', (chunk: Buffer) => { if (process.env.INTEROP_DEBUG) console.error(`[server] ${chunk.toString().trim()}`); });
      return child;
    });
    link = stdio;
    void stdio.start();
    kill = async () => { stdio.kill(); };
  }

  const controller = new AbortController();
  const ledger = newLedger();
  const cursor = new CursorFile(join(dir, 'cursor.json'));
  const options = { link, subscription: SUBSCRIPTION, cursor, ledger, signal: controller.signal, deadMs: server === 'cli' ? 60_000 : HEARTBEAT_MS * 4, maxWaitMs: 30 };
  const client = mode === 'push' ? pushLoop(options) : pollLoop(options);
  const control: Control = client.control;

  const post = async (round: number) => {
    const body = Array.from({ length: SCOPES }, (_, index) => JSON.stringify({ scope: scopeName(index), id: `${scopeName(index)}#${round}`, at: round, name: 'ping', data: { round } })).join('\n') + '\n';
    for (let attempt = 0; ; attempt++) {
      try {
        const response = await fetch(ingestUrl, { method: 'POST', headers: { 'Content-Type': 'application/x-ndjson' }, body, signal: AbortSignal.timeout(10_000) });
        const result = await response.json() as { accepted: number; duplicates: number; rejected: unknown[] };
        if (response.ok && result.rejected.length === 0) return;
      } catch (error) { if (process.env.INTEROP_DEBUG) console.error('post', round, String(error)); /* the server is down or was killed mid-request: send the same round again */ }
      if (attempt > 400) throw new Error(`round ${round} never accepted`);
      await sleep(50);
    }
  };

  const ever = new Map<string, DecisionRow>();
  const snapshot = async () => {
    const rows = await readAll(store);
    for (const row of rows) ever.set(row.id, row);
    return rows;
  };
  const events = () => new Set([...ever.values()].flatMap((row) => (row.body as { window: { events: { id: string }[] } }).window.events.map((event) => event.id)));
  const received = () => new Set(ledger.received.map((event) => event.eventId));
  const caughtUp = async () => {
    const have = received();
    return matching(await snapshot()).every((row) => have.has(row.id));
  };

  let chaos = true;
  let drops = 0;
  const dropper = (async () => {
    while (!controller.signal.aborted) {
      await sleep(40 + roll() * 360, controller.signal);
      if (chaos) { control.drop(); drops += 1; }
    }
  })();

  try {
    await until('the first subscription', () => ledger.starts.length > 0);
    const killRounds = new Set([6 + Math.floor(roll() * 6), 26 + Math.floor(roll() * 6)]);
    const trimRound = 16 + Math.floor(roll() * 4);
    let retained = new Set<string>();
    let lost: string[] = [];
    let startsAtRelease = -1;

    for (let round = 0; round < ROUNDS; round++) {
      if (round === trimRound) {
        await store.log.trim({ maxRows: 1_000_000 });
        await until('the client to catch up', caughtUp);
        const before = ledger.starts.length;
        await control.hold();
        await post(round);
        await post(round + 1);
        await until('the held rounds to settle', async () => { await snapshot(); return events().size === (round + 2) * SCOPES; }, 60_000, () => ({ events: events().size, want: (round + 2) * SCOPES }));
        const pre = [...ever.values()];
        const have = received();
        await store.log.trim({ maxRows: 1 });
        retained = new Set((await readAll(store)).map((row) => row.id));
        lost = matching(pre).filter((row) => !retained.has(row.id) && !have.has(row.id)).map((row) => row.id);
        startsAtRelease = ledger.starts.length;
        expect(startsAtRelease).toBe(before);
        control.release();
        round++;
        continue;
      }
      const killing = killRounds.has(round);
      const posting = post(round);
      if (killing) { await sleep(roll() * 40); await kill(); }
      await posting;
      await until(`round ${round} to be decided`, async () => { await snapshot(); return events().size >= (round + 1) * SCOPES; }, 60_000, () => ({ events: events().size }));
      if (roll() < 0.3) await sleep(roll() * 60);
    }

    chaos = false;
    await until('every event to be decided', async () => { await snapshot(); return events().size === DECISIONS; }, 60_000, () => ({ events: events().size }));
    const expected = new Set(matching([...ever.values()]).map((row) => row.id));
    for (const id of lost) expected.delete(id);
    await until('the client to receive every retained row', () => [...expected].every((id) => received().has(id)), 60_000);
    const head = (await store.log.bounds()).last!;
    await until('the cursor to reach the head', () => (cursor.read() ?? '').endsWith(`.${head}`), 30_000, () => ({ head, cursor: cursor.read() }));
    await sleep(2 * HEARTBEAT_MS + 200);

    const ids = ledger.received.map((event) => event.eventId);
    expect(ids.length, 'duplicates').toBe(new Set(ids).size);
    expect([...received()].sort(), 'received equals the matching log rows, less the ones retention removed').toEqual([...expected].sort());
    expect(lost.length, 'the trim must have removed rows the client had not read').toBeGreaterThan(0);
    for (const id of lost) expect(received().has(id)).toBe(false);
    expect(expected.size).toBeGreaterThan(20);

    if (process.env.INTEROP_DEBUG) console.error('starts', startsAtRelease, JSON.stringify(ledger.starts.map((start, index) => [index, start.truncated, start.cursor?.slice(-8)])));
    const truncated = ledger.starts.filter((start) => start.truncated);
    expect(truncated.length, 'truncated is set exactly once, for the trimmed cursor').toBe(1);
    const flagged = ledger.starts.findIndex((start) => start.truncated);
    expect(flagged, 'the truncated start comes after the trim').toBeGreaterThanOrEqual(startsAtRelease);
    expect(ledger.starts[flagged]!.afterEvents, 'no event is delivered between the reconnect and the truncated start').toBe(ledger.starts[startsAtRelease]!.afterEvents);
    expect(ledger.violations).toEqual([]);
    expect(ledger.errors).toEqual([]);
    expect(ledger.terminated).toEqual([]);
    expect(drops, 'the connection was aborted at random points').toBeGreaterThan(2);
    if (mode === 'push' && server === 'script') {
      expect(cursor.writes).toBeGreaterThanOrEqual(ledger.received.length + ledger.heartbeats);
      expect(ledger.heartbeats).toBeGreaterThan(0);
    }
    if (transport === 'http') expect(proc.starts).toBe(3);
    else expect((link as StdioLink).spawns).toBeGreaterThanOrEqual(3);
    return { decisions: ever.size, connections: ledger.connections, received: ids.length, expected: expected.size, lost: lost.length, starts: ledger.starts.length, heartbeats: ledger.heartbeats, seed: SEED };
  } finally {
    controller.abort();
    await dropper;
    await client.done;
    if (transport === 'http') await proc.kill(); else (link as StdioLink).close();
    await store.close?.();
    await pool?.end();
    if (admin) { await admin.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`); await admin.end(); }
    rmSync(dir, { recursive: true, force: true });
  }
}

describe.each<[Store, Transport, Mode, Server]>([
  ['sqlite', 'http', 'push', 'script'], ['sqlite', 'stdio', 'push', 'script'], ['sqlite', 'http', 'poll', 'script'], ['sqlite', 'stdio', 'poll', 'script'],
  ['postgres', 'http', 'push', 'script'], ['postgres', 'stdio', 'push', 'script'], ['postgres', 'http', 'poll', 'script'], ['postgres', 'stdio', 'poll', 'script'],
  ['postgres', 'http', 'push', 'cli'], ['postgres', 'http', 'poll', 'cli'],
])('%s over %s, %s, %s server', (storeKind, transport, mode, server) => {
  test('no loss, no duplicates, truncated exactly once', { timeout: 300_000 }, async () => {
    const summary = await scenario(storeKind, transport, mode, server);
    console.log(`interop ${storeKind}/${transport}/${mode}/${server}`, JSON.stringify(summary));
  });
});
