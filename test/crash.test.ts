import { randomBytes } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import pg from 'pg';
import { createClient } from 'redis';
import { sqliteStore } from '../src/store/sqlite.js';
import { migrate, postgresStore } from '../src/store/postgres.js';
import { redisStore, type RedisSend } from '../src/store/redis.js';
import type { ScopeStore } from '../src/store/types.js';
import { aliasingCases } from './support/aliasing.js';
import { windowId } from './oracle/step.js';
import { afterAll, describe, expect, test } from 'vitest';
import { memoryStore } from '../src/memory.js';
import { CrashError, crashSuite, multiScopeCrashSuite, harness, recordingSink, scriptedJudge, type ScriptedAnswer } from '../src/testing.js';
import type { Event, Kind } from '../src/types.js';
import { freezingStore } from '../src/store/memory.js';

const kind = (e: Event): Kind => (e.name === 'close' ? 'closing' : 'consequence');
const ev = (id: string): Event => ({ id, at: 0, name: 'close', data: {} });
const ID_A = 'cf8923dfae84f930';
const ID_AB = 'dc811072354ed32b';

function setup(script: ScriptedAnswer[]) {
  const judge = scriptedJudge(script);
  const sink = recordingSink();
  const h = harness({ judge, sink, kind });
  return { h, judge, sink };
}

const directory = mkdtempSync(join(tmpdir(), 'attend-p6a-'));
const schema = `attend_p6a_${randomBytes(6).toString('hex')}`;
const redisPrefix = `attend-p6a:{${randomBytes(6).toString('hex')}}:`;
const closers: (() => Promise<void> | void)[] = [];
let files = 0;
let prefixes = 0;
let postgres: Promise<pg.Pool> | undefined;
let redis: Promise<RedisSend> | undefined;
const pool = () => postgres ??= (async () => {
  const url = process.env.ATTEND_POSTGRES_URL ?? 'postgres://postgres:attend@127.0.0.1:15432/attend';
  const admin = new pg.Client({ connectionString: url, connectionTimeoutMillis: 5000 });
  try { await admin.connect(); await admin.query(`CREATE SCHEMA ${schema}`); }
  finally { await admin.end(); }
  return new pg.Pool({ connectionString: url, options: `-c search_path=${schema}`, connectionTimeoutMillis: 5000, max: 4 });
})();
const redisSend = () => redis ??= (async () => {
  const client = createClient({ url: process.env.ATTEND_REDIS_URL ?? 'redis://127.0.0.1:16379', socket: { reconnectStrategy: false, connectTimeout: 5000 } });
  client.on('error', () => undefined);
  await client.connect();
  closers.push(() => client.close());
  return (args: string[]) => client.sendCommand(args);
})();

afterAll(async () => {
  const opened = await postgres?.catch(() => undefined);
  if (opened) { await opened.query(`DROP SCHEMA ${schema} CASCADE`); await opened.end(); }
  const send = await redis?.catch(() => undefined);
  const keys = await send?.(['KEYS', `${redisPrefix}*`]);
  if (send && Array.isArray(keys) && keys.length) await send(['UNLINK', ...keys.map(String)]);
  for (const close of closers) await close();
  rmSync(directory, { force: true, recursive: true });
}, 120000);

const stores: [string, () => ScopeStore | Promise<ScopeStore>][] = [
  ['memory', () => memoryStore()],
  ['frozen memory', () => freezingStore(memoryStore())],
  ['SQLite in-thread WAL', () => {
    const db = new DatabaseSync(join(directory, `${files++}.sqlite`));
    db.exec('PRAGMA journal_mode=WAL');
    const store = sqliteStore(db);
    const close = async () => { await store.close?.(); if (db.isOpen) db.close(); };
    closers.push(close);
    return { ...store, close };
  }],
  ['SQLite worker WAL', () => {
    const store = sqliteStore({ file: join(directory, `${files++}.sqlite`), worker: true });
    let closed = false;
    const close = async () => { if (!closed) { closed = true; await store.close?.(); } };
    closers.push(close);
    return { ...store, close };
  }],
  ['Postgres', async () => {
    const client = await pool();
    const prefix = `t${prefixes++}_`;
    await migrate(client, { prefix });
    return postgresStore(client, { prefix });
  }],
  ['Redis', async () => redisStore(await redisSend(), { prefix: `${redisPrefix}${prefixes++}:` })],
];

for (const [name, make] of stores) describe(`crash and aliasing through ${name}`, () => {
  let elapsed = 0;
  afterAll(() => { console.log(`${name} crash and aliasing cases: ${elapsed} ms`); });
  for (const c of [...crashSuite(make), ...multiScopeCrashSuite(make), ...aliasingCases(make)]) test(c.name, async () => {
    const start = Date.now();
    try { await c.run(); }
    finally { elapsed += Date.now() - start; }
  }, 300000);
});

/** Reports the first attempt at every multi-scope group as a conflict for all but its last scope, as a racing writer would. */
function conflictingFirstAttempts(inner: ScopeStore): ScopeStore {
  const seen = new Set<string>();
  return {
    ...inner,
    async commit(writes) {
      const key = writes.map((write) => `${write.scope}@${write.expectedVersion}`).join('|');
      if (writes.length < 2 || seen.has(key)) return inner.commit(writes);
      seen.add(key);
      const last = writes.reduce((a, b) => (a.scope > b.scope ? a : b));
      const [outcome] = await inner.commit([last]);
      return writes.map((write) => (write === last ? outcome! : 'conflict'));
    },
  };
}

test('the multi-scope session settles every act while groups conflict on their first attempt', async () => {
  const [clean] = multiScopeCrashSuite(() => conflictingFirstAttempts(memoryStore()));
  await clean!.run();
}, 60000);

test('crash-suite pinned ids equal the frozen reducer ids', () => {
  expect(windowId('scope', ['a'])).toBe(ID_A);
  expect(windowId('scope', ['a', 'b'])).toBe(ID_AB);
  const expected: [string, string[], string][] = [
    ['m/x', ['m/x/a'], '57926a9b506cd3a1'],
    ['m/x', ['m/x/c', 'm/x/h'], '4b0d164deed6032f'],
    ['m/x', ['m/x/e'], '57926e9b506cda6d'],
    ['m/x', ['m/x/b', 'm/x/d'], 'ba2ddaa47740a48a'],
    ['m/x', ['m/x/f'], '57926b9b506cd554'],
    ['m/y', ['m/y/a'], 'd29b96ea48510c9f'],
    ['m/y', ['m/y/h'], 'd29b9dea48511884'],
    ['m/y', ['m/y/e'], 'd29b92ea485105d3'],
    ['m/y', ['m/y/b', 'm/y/d'], '51a3be624f9cf887'],
    ['m/y', ['m/y/f'], 'd29b93ea48510786'],
    ['m/z', ['m/z/a'], '6be88f91d9776351'],
    ['m/z', ['m/z/h'], '6be88691d9775406'],
    ['m/z', ['m/z/e'], '6be89391d9776a1d'],
    ['m/z', ['m/z/b', 'm/z/d'], 'c43155c9c7ae9dd8'],
    ['m/z', ['m/z/f'], '6be89091d9776504'],
  ];
  for (const [scope, ids, id] of expected) expect(windowId(scope, ids)).toBe(id);
});

describe('restart regressions', () => {
  test('an open window survives a restart and settles on time', async () => {
    const { h, judge, sink } = setup([{ route: 'queue' }]);
    await h.push([ev('a')]);
    await h.restart();
    await h.advance(499);
    expect(judge.calls).toHaveLength(0);
    await h.advance(1);
    expect(judge.calls[0]?.window.events.map((e) => e.id)).toEqual(['a']);
    expect(sink.delivered.map((d) => d.id)).toEqual([ID_A]);
    await h.stop();
  });

  test('an open window whose deadline passed while down settles at the restart', async () => {
    const { h, sink } = setup([{ route: 'queue' }]);
    await h.push([ev('a')]);
    h.crash();
    await h.clock.set(5000);
    await h.restart();
    expect(sink.delivered.map((d) => d.id)).toEqual([ID_A]);
    await h.stop();
  });

  test('a waiting window survives a restart and merges with the next settle', async () => {
    const { h, judge, sink } = setup([{ route: 'wait' }, { route: 'queue' }]);
    await h.push([ev('a')]);
    await h.advance(500);
    expect(await h.inspect()).toMatchObject({ batch: { window: { waits: 1 }, phase: { phase: 'waiting' } } });
    await h.restart();
    await h.push([ev('b')]);
    await h.advance(500);
    expect(judge.calls[1]?.window).toMatchObject({ id: ID_AB, waits: 1, events: [{ id: 'a' }, { id: 'b' }] });
    expect(sink.delivered.map((d) => d.id)).toEqual([ID_AB]);
    await h.stop();
  });

  test('an undelivered decision survives a crash between the judge and the sink', async () => {
    const { h, sink } = setup([{ route: 'queue' }]);
    await h.push([ev('a')]);
    h.crashAt(2, 'after');
    await expect(h.advance(500)).rejects.toBeInstanceOf(CrashError);
    expect(sink.delivered).toEqual([]);
    expect(await h.inspect()).toMatchObject({ outbox: { decisions: [{ item: { id: ID_A, kind: 'decision' }, phase: { phase: 'inFlight', attempt: 1, leaseUntil: 10_500 } }], deadLetters: [] } });
    await h.restart();
    expect(sink.delivered).toEqual([]);
    await h.advance(10_000);
    expect(h.traces).toContainEqual(expect.objectContaining({ kind: 'retrying', stage: 'sink', message: 'delivery lease expired' }));
    await h.advance(1000);
    expect(sink.delivered.map((d) => d.id)).toEqual([ID_A]);
    expect(await h.inspect()).toMatchObject({ outbox: { decisions: [], deadLetters: [] } });
    await h.stop();
  });

  test('a judge that always throws dead-letters after the retry budget, across a restart', async () => {
    const { h, judge, sink } = setup([{ fail: 'down' }]);
    await h.push([ev('a')]);
    await h.advance(500 + 1000);
    expect(judge.calls).toHaveLength(2);
    await h.restart();
    await h.advance(4000 + 16_000);
    expect(judge.calls).toHaveLength(4);
    expect(h.deadLetters.map((l) => [l.id, l.stage, l.attempts])).toEqual([[ID_A, 'judge', 4]]);
    expect(sink.delivered).toEqual([]);
    expect(await h.inspect()).toMatchObject({ batch: null, outbox: { decisions: [], deadLetters: [] } });
    await h.stop();
  });

  test('a flush through the harness settles at the next tick', async () => {
    const { h, judge } = setup([{ route: 'note' }]);
    await h.push([{ id: 'x', at: 0, name: 'out', data: {} }]);
    await h.flush();
    expect(judge.calls[0]?.window).toMatchObject({ reason: 'forced', events: [{ id: 'x' }] });
    await h.stop();
  });
});

describe('harness', () => {
  test('a crash before a commit writes nothing; a crash after it writes but runs no effect', async () => {
    const before = setup([{ route: 'queue' }]);
    before.h.crashAt(1, 'before');
    await expect(before.h.push([ev('a')])).rejects.toBeInstanceOf(CrashError);
    expect(await before.h.inspect()).toBeNull();
    expect(before.h.crashed).toBe(true);

    const after = setup([{ route: 'queue' }]);
    after.h.crashAt(2, 'after');
    await after.h.push([ev('a')]);
    await expect(after.h.advance(500)).rejects.toBeInstanceOf(CrashError);
    expect(after.judge.calls).toHaveLength(0);
    expect(await after.h.inspect()).toMatchObject({ version: 2, batch: { phase: { phase: 'judging' } } });
    await after.h.restart();
    expect(after.h.crashed).toBe(false);
    await after.h.stop();
  });

  test('a dead instance never writes again, even after a restart', async () => {
    const { h } = setup([{ route: 'queue' }]);
    const old = h.attend;
    h.crash();
    await h.restart();
    await expect(old.push('scope', [ev('a')])).rejects.toBeInstanceOf(CrashError);
    expect(await h.inspect()).toBeNull();
    await h.stop();
  });
});
