import { randomBytes } from 'node:crypto';
import pg from 'pg';
import { afterAll, describe, expect, test } from 'vitest';
import { DEFAULT_POLICY } from '../../src/core/types.js';
import type { DecisionAppend, LoggedInput, ScopeRecord } from '../../src/core/types.js';
import { migrate, postgresStore } from '../../src/store/postgres.js';
import type { PostgresClient, PostgresListener } from '../../src/store/postgres.js';
import type { ScopeStore, ScopeWrite, Snapshot } from '../../src/store/types.js';
import { decisionLogConformance, scopeStoreConformance } from '../../src/testing/conformance.js';

const url = process.env.ATTEND_POSTGRES_URL ?? 'postgres://postgres:attend@127.0.0.1:15432/attend';
const schema = `attend_p5c_${randomBytes(6).toString('hex')}`;
const config = { connectionString: url, options: `-c search_path=${schema}`, connectionTimeoutMillis: 5000 };
let ready: Promise<pg.Pool> | undefined;
let listenerReady: Promise<{ client: pg.Client; listener: PostgresListener }> | undefined;
let tables = 0;
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

const pool = () => ready ??= (async () => {
  const admin = new pg.Client({ connectionString: url, connectionTimeoutMillis: 5000 });
  try {
    await admin.connect();
    await admin.query(`CREATE SCHEMA ${schema}`);
  } finally {
    await admin.end();
  }
  return new pg.Pool({ ...config, max: 4 });
})();

async function connect(): Promise<pg.Client> {
  await pool();
  const client = new pg.Client(config);
  try {
    await client.connect();
    return client;
  } catch (error) {
    await client.end();
    throw error;
  }
}

function sharedListener() {
  return listenerReady ??= (async () => {
    const client = await connect();
    const channels = new Map<string, { callbacks: Set<() => void>; ready: Promise<unknown> }>();
    client.on('notification', ({ channel }) => {
      for (const callback of channels.get(channel)?.callbacks ?? []) callback();
    });
    const listener: PostgresListener = {
      async listen(channel, callback) {
        let entry = channels.get(channel);
        if (!entry) {
          entry = { callbacks: new Set(), ready: client.query(`LISTEN "${channel}"`) };
          channels.set(channel, entry);
        }
        entry.callbacks.add(callback);
        await entry.ready;
        return async () => {
          entry.callbacks.delete(callback);
          if (entry.callbacks.size === 0 && channels.get(channel) === entry) {
            channels.delete(channel);
            await client.query(`UNLISTEN "${channel}"`);
          }
        };
      },
    };
    return { client, listener };
  })();
}

async function fresh(): Promise<string> {
  const prefix = `t${tables++}_`;
  await migrate(await pool(), { prefix });
  return prefix;
}

async function make(): Promise<ScopeStore> {
  return postgresStore(await pool(), { prefix: await fresh(), listener: (await sharedListener()).listener });
}

async function concurrent(): Promise<[ScopeStore, ScopeStore]> {
  const prefix = await fresh();
  const wrap = (): ScopeStore => {
    // A pool per store, so the two stores' commits run on separate connections. Async commit skips the WAL flush wait, so the
    // case's 200 commits fit its 15 s budget on a loaded server; visibility order is the same either way.
    const own = new pg.Pool({ ...config, max: 2, connectionTimeoutMillis: 30_000 });
    const store = postgresStore(own, { prefix, asyncCommit: true });
    return { ...store, async close() { await store.close?.(); await own.end(); } };
  };
  return [wrap(), wrap()];
}

afterAll(async () => {
  const listener = await listenerReady?.catch(() => undefined);
  await listener?.client.end();
  const opened = await ready?.catch(() => undefined);
  try {
    await opened?.query(`DROP SCHEMA ${schema} CASCADE`);
  } finally {
    await opened?.end();
  }
});

function input(id: string): LoggedInput {
  return { input: { kind: 'events', events: [{ id, at: 1, name: 'changed', data: { id, text: 'héllo 🌍\u0000' }, kind: 'closing' }] }, now: 1000 };
}

function append(id: string, scope = 's', at = 1000): DecisionAppend {
  return {
    kind: 'decision', id, scope, at, route: 'interrupt',
    body: {
      id, scope, at, route: 'interrupt', confidence: 0.8, predicates: {}, probabilities: { interrupt: 1, queue: 0, note: 0, ask: 0, skip: 0, wait: 0 },
      window: { id, events: [], reason: 'closing', waits: 0, dropped: 0, gap: false, settledAt: at },
    },
  };
}

function snapshot(scope: string, version: number): Snapshot {
  const record: ScopeRecord = {
    scope, version, cursors: {}, seen: ['héllo 🌍\u0000'], buffer: { entries: [], dropped: 0, gap: false, forcedAt: null },
    batch: null, outbox: { decisions: [], deadLetters: [] }, recent: [], dueAt: null,
  };
  return { version, record, policy: DEFAULT_POLICY, reducer: 1 };
}

function write(scope = 's', expectedVersion = 0, extra: Partial<ScopeWrite> = {}): ScopeWrite {
  const result: ScopeWrite = { scope, expectedVersion, inputs: [input(`${scope}:${expectedVersion}`)], snapshot: null, appends: [], ...extra };
  result.serialized = {
    inputs: result.inputs.map((i) => JSON.stringify(i)),
    snapshot: result.snapshot === null ? null : JSON.stringify(result.snapshot.record),
    appends: result.appends.map((a) => JSON.stringify(a.body)),
  };
  return result;
}

const ids = async (store: ScopeStore) => (await store.log.read({ after: null, limit: 100 })).rows.map((row) => row.id);

describe('postgres scope store conformance', () => {
  for (const c of scopeStoreConformance(make)) test(c.name, c.run, 30_000);
});

describe('postgres decision log conformance', () => {
  for (const c of decisionLogConformance(make, { concurrent })) test(c.name, c.run, 30_000);
});

test('out-of-order commits stay hidden until the lower xid finishes', async () => {
  const prefix = await fresh();
  const client = await connect();
  const a = postgresStore(client, { prefix });
  const b = postgresStore(await pool(), { prefix });
  const reader = postgresStore(await pool(), { prefix });
  try {
    await client.query('BEGIN');
    await client.query('SELECT pg_current_xact_id()');
    expect(await a.commit([write('a', 0, { appends: [append('early', 'a')] })])).toEqual(['ok']);
    expect(await b.commit([write('b', 0, { appends: [append('late', 'b')] })])).toEqual(['ok']);
    expect(await reader.log.read({ after: null, limit: 10 })).toEqual({ rows: [], checked: null, more: false });
    expect(await reader.log.bounds()).toEqual({ first: null, last: null });
    expect(await reader.log.find('decision', 'late')).toBeNull();
    const started = Date.now();
    await reader.log.wait(null, 300, new AbortController().signal);
    expect(Date.now() - started).toBeGreaterThanOrEqual(250);
    await client.query('COMMIT');
    await expect.poll(() => ids(reader)).toEqual(['early', 'late']);
    const rows = (await reader.log.read({ after: null, limit: 10 })).rows;
    const [early, late] = rows;
    if (!early || !late) throw new Error('missing committed rows');
    expect(reader.log.compare(early.position, late.position)).toBeLessThan(0);
  } finally {
    await client.query('ROLLBACK');
    await Promise.all([a.close?.(), b.close?.(), reader.close?.()]);
    await client.end();
  }
}, 30_000);

test('read your own writes waits for the horizon', async () => {
  const blocker = await connect();
  const store = await make();
  try {
    await blocker.query('BEGIN');
    await blocker.query('SELECT pg_current_xact_id()');
    expect(await store.commit([write('s', 0, { appends: [append('mine')] })])).toEqual(['ok']);
    const started = Date.now();
    const pending = store.log.read({ after: null, limit: 10 }).then((page) => ({ page, elapsed: Date.now() - started }));
    await sleep(200);
    await blocker.query('COMMIT');
    const { page, elapsed } = await pending;
    expect(page.rows.map((row) => row.id)).toEqual(['mine']);
    expect(elapsed).toBeGreaterThanOrEqual(150);
  } finally {
    await blocker.query('ROLLBACK');
    await blocker.end();
    await store.close?.();
  }
}, 30_000);

test('a read waits only for the commits made before it started', async () => {
  const db = await pool();
  let gate: Promise<void> | undefined;
  const client: PostgresClient = {
    async query(sql, values) {
      if (gate && sql.startsWith('SELECT pg_snapshot_xmin')) await gate;
      return db.query(sql, values);
    },
  };
  const store = postgresStore(client, { prefix: await fresh() });
  const blocker = await connect();
  try {
    expect(await store.commit([write('s', 0, { appends: [append('before')] })])).toEqual(['ok']);
    let release = () => {};
    gate = new Promise<void>((resolve) => { release = resolve; });
    let blocked = true;
    const reading = store.log.read({ after: null, limit: 10 }).then((page) => ({ page, blocked }));
    await blocker.query('BEGIN');
    await blocker.query('SELECT pg_current_xact_id()');
    expect(await store.commit([write('s', 1, { appends: [append('after')] })])).toEqual(['ok']);
    gate = undefined;
    const released = Date.now();
    release();
    const timer = sleep(3000).then(async () => { blocked = false; await blocker.query('COMMIT'); });
    const { page, blocked: stillBlocked } = await reading;
    expect(page.rows.map((row) => row.id)).toEqual(['before']);
    expect(stillBlocked).toBe(true);
    expect(Date.now() - released).toBeLessThan(1500);
    await timer;
  } finally {
    await blocker.query('ROLLBACK');
    await blocker.end();
    await store.close?.();
  }
}, 30_000);

test('opposite scope orders do not deadlock for updates or creates', async () => {
  const prefix = await fresh();
  const a = await connect();
  const b = await connect();
  const sa = postgresStore(a, { prefix });
  const sb = postgresStore(b, { prefix });
  const scopes = Array.from({ length: 20 }, (_, i) => `s${String(i).padStart(2, '0')}`);
  try {
    // Nested loops in input order plus a per-row pause make an unsorted lock order deadlock reliably.
    for (const client of [a, b]) await client.query('SET enable_seqscan = off; SET enable_hashjoin = off; SET enable_mergejoin = off');
    await a.query(`CREATE FUNCTION ${prefix}pause_write() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN PERFORM pg_sleep(0.002); RETURN NEW; END $$;
      CREATE TRIGGER pause_write BEFORE UPDATE OR INSERT ON ${prefix}scopes FOR EACH ROW EXECUTE FUNCTION ${prefix}pause_write()`);
    expect(await sa.commit(scopes.map((scope) => write(scope)))).toEqual(Array(20).fill('ok'));
    async function race(names: string[], version: number, round: number) {
      const first = names.map((scope) => write(scope, version, { inputs: [input(`a:${round}`)] }));
      const second = [...names].reverse().map((scope) => write(scope, version, { inputs: [input(`b:${round}`)] }));
      const [left, right] = await Promise.all([sa.commit(first), sb.commit(second)]);
      for (const [i, scope] of names.entries()) {
        const outcomes = [left[i], right[names.length - 1 - i]];
        expect(outcomes.filter((outcome) => outcome === 'ok')).toEqual(['ok']);
        expect(outcomes.filter((outcome) => outcome === 'conflict')).toEqual(['conflict']);
        const loaded = await sa.load(scope);
        expect(loaded?.version).toBe(version + 1);
        expect(loaded?.tail.at(-1)).toEqual(input(`${left[i] === 'ok' ? 'a' : 'b'}:${round}`));
      }
    }
    for (let round = 0; round < 10; round += 1) await race(scopes, round + 1, round);
    for (let round = 0; round < 10; round += 1) await race(scopes.map((scope) => `new${round}:${scope}`), 0, round);
  } finally {
    await Promise.all([sa.close?.(), sb.close?.()]);
    await Promise.all([a.end(), b.end()]);
  }
}, 30_000);

test('NOTIFY fires only when a commit inserts decision rows', async () => {
  const prefix = await fresh();
  const listener = await connect();
  const store = postgresStore(await pool(), { prefix });
  const notifications: string[] = [];
  listener.on('notification', ({ channel }) => notifications.push(channel));
  try {
    await listener.query(`LISTEN "${prefix}decisions"`);
    expect(await store.commit([write()])).toEqual(['ok']);
    await sleep(300);
    expect(notifications).toEqual([]);
    expect(await store.commit([write('s', 1, { appends: [append('once')] })])).toEqual(['ok']);
    await expect.poll(() => notifications.length).toBe(1);
    expect(notifications).toEqual([`${prefix}decisions`]);
    expect(await store.commit([write('s', 2, { appends: [append('once')] })])).toEqual(['ok']);
    await sleep(300);
    expect(notifications).toEqual([`${prefix}decisions`]);
  } finally {
    await store.close?.();
    await listener.end();
  }
}, 30_000);

test('HOT updates dominate scope writes', async () => {
  const prefix = await fresh();
  const client = await connect();
  const store = postgresStore(client, { prefix });
  const scopes = Array.from({ length: 1000 }, (_, i) => `s${String(i).padStart(4, '0')}`);
  const parts = Array.from({ length: 5 }, (_, i) => scopes.slice(i * 200, (i + 1) * 200));
  try {
    for (const part of parts) expect(await store.commit(part.map((scope) => write(scope)))).toEqual(Array(200).fill('ok'));
    for (let round = 1; round <= 5; round += 1) {
      for (const part of parts) {
        const group = part.map((scope, i) => write(scope, round, i % 50 === 0 ? { due: round } : {}));
        expect(await store.commit(group)).toEqual(Array(200).fill('ok'));
      }
    }
  } finally {
    await store.close?.();
    await client.end();
  }
  const counters = async () => (await (await pool()).query('SELECT n_tup_upd::text, n_tup_hot_upd::text FROM pg_stat_user_tables WHERE schemaname = $1 AND relname = $2', [schema, `${prefix}scopes`])).rows[0];
  await expect.poll(async () => Number((await counters())?.n_tup_upd), { timeout: 10_000 }).toBe(5000);
  const counts = await counters();
  expect(Number(counts?.n_tup_hot_upd) / Number(counts?.n_tup_upd)).toBeGreaterThanOrEqual(0.9);
}, 60_000);

test('each mixed group and async commit use one statement', async () => {
  const prefix = await fresh();
  const db = await pool();
  const calls: { sql: string; values: unknown[] | undefined }[] = [];
  const client: PostgresClient = { query(sql, values) { calls.push({ sql, values }); return db.query(sql, values); } };
  const store = postgresStore(client, { prefix });
  try {
    await store.commit([write('b'), write('c')]);
    calls.length = 0;
    const snap = snapshot('b', 2);
    expect(await store.commit([write('a', 0, { due: 12.5 }), write('b', 1, { snapshot: snap }), write('c', 1, { appends: [append('c1', 'c'), append('c2', 'c')] })])).toEqual(['ok', 'ok', 'ok']);
    expect(calls).toHaveLength(1);
    expect(await store.load('a')).toEqual({ version: 1, snapshot: null, tail: [input('a:0')] });
    expect(await store.load('b')).toEqual({ version: 2, snapshot: snap, tail: [] });
    expect(await store.due(12.5, 10)).toEqual([{ scope: 'a', due: 12.5 }]);
    expect(await ids(store)).toEqual(['c1', 'c2']);
    const asyncStore = postgresStore(client, { prefix, asyncCommit: true });
    calls.length = 0;
    expect(await asyncStore.commit([write('a', 1, { due: null }), write('b', 2), write('c', 2, { appends: [append('c3', 'c')] })])).toEqual(['ok', 'ok', 'ok']);
    expect(calls).toHaveLength(1);
    expect(calls[0]?.sql).toContain("set_config('synchronous_commit', 'off', true)");
    expect(await asyncStore.load('b')).toEqual({ version: 3, snapshot: snap, tail: [input('b:2')] });
    expect(await asyncStore.due(100, 10)).toEqual([]);
    expect(await ids(asyncStore)).toEqual(['c1', 'c2', 'c3']);
    calls.length = 0;
    expect(await store.commit([])).toEqual([]);
    await expect(store.commit([write('x'), write('x')])).rejects.toThrow('duplicate scope');
    expect(calls).toEqual([]);
    await asyncStore.close?.();
  } finally {
    await store.close?.();
  }
}, 30_000);

test('snapshots tails decisions and epoch survive new instances and migration', async () => {
  const prefix = await fresh();
  const first = await connect();
  const a = postgresStore(first, { prefix });
  const snap = snapshot('s', 1);
  let epoch: string;
  try {
    expect(await a.commit([write('s', 0, { snapshot: snap, appends: [append('saved')] })])).toEqual(['ok']);
    expect(await a.commit([write('s', 1)])).toEqual(['ok']);
    epoch = await a.log.epoch();
    expect(epoch).toEqual(expect.any(String));
  } finally {
    await a.close?.();
    await first.end();
  }
  const second = await connect();
  const b = postgresStore(second, { prefix });
  try {
    await migrate(second, { prefix });
    expect(await b.load('s')).toEqual({ version: 2, snapshot: snap, tail: [input('s:1')] });
    await expect.poll(() => ids(b)).toEqual(['saved']);
    expect(await b.log.epoch()).toBe(epoch);
  } finally {
    await b.close?.();
    await second.end();
  }
}, 30_000);

test('four concurrent migrations of one prefix succeed', async () => {
  const prefix = `t${tables++}_`;
  const clients: pg.Client[] = [];
  try {
    for (let i = 0; i < 4; i += 1) clients.push(await connect());
    await Promise.all(clients.map((client) => migrate(client, { prefix })));
    const store = postgresStore(await pool(), { prefix });
    expect(await store.commit([write()])).toEqual(['ok']);
    expect(await store.load('s')).toEqual({ version: 1, snapshot: null, tail: [input('s:0')] });
    await store.close?.();
  } finally {
    await Promise.all(clients.map((client) => client.end()));
  }
}, 30_000);

test('prefixes must be lowercase identifiers', async () => {
  const db = await pool();
  for (const prefix of ['x; DROP TABLE y; --', 'Attend_']) {
    const message = `attend/postgres: prefix must be lowercase letters, digits and underscores, got "${prefix}"`;
    await expect(migrate(db, { prefix })).rejects.toThrow(message);
    expect(() => postgresStore(db, { prefix })).toThrow(message);
  }
});

test('remove waits for an in-flight commit and deletes its inputs', async () => {
  const prefix = await fresh();
  const client = await connect();
  const writer = postgresStore(client, { prefix });
  const remover = postgresStore(await pool(), { prefix });
  let pending: Promise<void> | undefined;
  try {
    expect(await writer.commit([write()])).toEqual(['ok']);
    await client.query('BEGIN');
    expect(await writer.commit([write('s', 1)])).toEqual(['ok']);
    let finished = false;
    pending = remover.remove('s').then(() => { finished = true; });
    await sleep(200);
    expect(finished).toBe(false);
    await client.query('COMMIT');
    await pending;
    expect(await remover.load('s')).toBeNull();
    const replacement = write('s', 0, { inputs: [input('replacement'), input('second')] });
    expect(await remover.commit([replacement])).toEqual(['ok']);
    expect(await remover.load('s')).toEqual({ version: 2, snapshot: null, tail: replacement.inputs });
  } finally {
    await client.query('ROLLBACK');
    await pending;
    await Promise.all([writer.close?.(), remover.close?.()]);
    await client.end();
  }
}, 30_000);

test('writes without serialized pieces round-trip identically', async () => {
  const stores = [await make(), await make()];
  try {
    for (const [i, store] of stores.entries()) {
      const first = write('s', 0, { snapshot: snapshot('s', 1), appends: [append('one')] });
      const second = write('s', 1, { appends: [append('two')] });
      if (i === 1) { delete first.serialized; delete second.serialized; }
      expect(await store.commit([first])).toEqual(['ok']);
      expect(await store.commit([second])).toEqual(['ok']);
      expect(await store.load('s')).toEqual({ version: 2, snapshot: snapshot('s', 1), tail: second.inputs });
      expect(await ids(store)).toEqual(['one', 'two']);
    }
  } finally {
    await Promise.all(stores.map((store) => store.close?.()));
  }
}, 30_000);

test('positions are validated and compared numerically', async () => {
  const store = await make();
  try {
    await expect(store.log.read({ after: 'nope', limit: 1 })).rejects.toThrow('attend/postgres: not a position from this log: "nope"');
    expect(() => store.log.compare('1.2', 'x')).toThrow('attend/postgres: not a position from this log: "x"');
    expect(store.log.compare('9.1', '10.0')).toBeLessThan(0);
    expect(store.log.compare('18446744073709551614.1', '18446744073709551615.1')).toBeLessThan(0);
    expect(store.log.compare('10.1', '10.1')).toBe(0);
  } finally {
    await store.close?.();
  }
});

test('retain trims in the background and close waits for it', async () => {
  const store = postgresStore(await pool(), { prefix: await fresh(), retain: { maxRows: 2 } });
  try {
    expect(await store.commit([write('s', 0, { appends: Array.from({ length: 5 }, (_, i) => append(`d${i}`)) })])).toEqual(['ok']);
    await expect.poll(() => ids(store), { timeout: 2000 }).toEqual(['d3', 'd4']);
  } finally {
    await expect(store.close?.()).resolves.toBeUndefined();
  }
}, 30_000);
