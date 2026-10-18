import os from 'node:os';
import { join } from 'node:path';
import { mkdtemp, rm } from 'node:fs/promises';
import { randomBytes } from 'node:crypto';
import { performance } from 'node:perf_hooks';
import { createHistogram } from './histogram.mjs';

function bytes(value) {
  if (typeof value === 'string') return value.length;
  if (value instanceof Uint8Array) return value.byteLength;
  return 0;
}
function forward(target, overrides) {
  const bound = new Map();
  return new Proxy(target, { get(inner, property) {
    if (Object.hasOwn(overrides, property)) return overrides[property];
    const value = Reflect.get(inner, property, inner);
    if (typeof value !== 'function') return value;
    if (!bound.has(property)) bound.set(property, value.bind(inner));
    return bound.get(property);
  } });
}
export function countingSqlite(db, counter) {
  return forward(db, {
    prepare(...args) {
      const statement = db.prepare(...args);
      return forward(statement, {
        run(...params) {
          for (const value of params) counter.bytes += bytes(value);
          return statement.run(...params);
        },
      });
    },
  });
}
export function countingPostgres(client, counter) {
  return forward(client, {
    query(query, values, ...rest) {
      const text = typeof query === 'string' ? query : query.text;
      const params = typeof query === 'string' ? values : query.values;
      if (!/^\s*SELECT\b/i.test(text)) for (const value of params ?? []) counter.bytes += bytes(value);
      return client.query(query, values, ...rest);
    },
  });
}
const reads = new Set('GET MGET HGET HMGET HGETALL EXISTS ZRANGE ZRANGEBYSCORE ZSCORE XRANGE XREVRANGE XREAD XLEN SCAN TYPE PTTL TTL CONFIG PING INFO'.split(' '));
const scripts = new Set(['EVAL', 'EVALSHA']);
export function countingRedis(send, counter) {
  return (args) => {
    const command = args[0].toUpperCase();
    if (!reads.has(command)) for (let i = scripts.has(command) ? 2 : 1; i < args.length; i += 1) counter.bytes += bytes(args[i]);
    return send(args);
  };
}
function countPort(store, counter, method, scope) {
  counter.commits ??= 0;
  counter.latency ??= createHistogram();
  return forward(store, {
    async [method](...args) {
      const start = performance.now();
      counter.commits += 1;
      if (scope) for (const write of args[0]) {
        if (!write.serialized) continue;
        const { inputs, snapshot, appends } = write.serialized;
        for (const value of inputs) counter.bytes += bytes(value);
        counter.bytes += bytes(snapshot);
        for (const value of appends) counter.bytes += bytes(value);
      }
      try { return await store[method](...args); }
      finally { counter.latency.record((performance.now() - start) * 1000); }
    },
  });
}
export const countingStatePort = (store, counter) => countPort(store, counter, 'put', false);
export const countingScopePort = (store, counter) => countPort(store, counter, 'commit', true);

export async function openStore(build, spec = { store: 'memory' }) {
  const counter = { bytes: 0, commits: 0, latency: createHistogram() };
  let adapter;
  let durability;
  let cleanup = async () => {};
  const prefix = `bench_${process.pid}_${randomBytes(5).toString('hex')}_`;
  try {
    if (spec.store === 'memory') {
      adapter = build.core.memoryStore();
      durability = { store: 'memory' };
    } else if (spec.store === 'sqlite') {
      if (spec.worker && build.port !== 'scope') throw new Error('worker mode needs the scope store port');
      const { DatabaseSync } = await import('node:sqlite');
      const dir = await mkdtemp(join(os.tmpdir(), 'ab-'));
      let db;
      cleanup = async () => { db?.close(); await rm(dir, { recursive: true, force: true }); };
      const file = join(dir, 'store.db');
      db = new DatabaseSync(file);
      const journal = spec.journalMode ?? 'WAL';
      const sync = spec.synchronous ?? 'NORMAL';
      if (!['WAL', 'DELETE'].includes(journal) || !['OFF', 'NORMAL', 'FULL'].includes(sync)) throw new Error('invalid SQLite mode');
      db.exec(`PRAGMA journal_mode=${journal}; PRAGMA synchronous=${sync}`);
      const { sqliteStore } = await build.module('sqlite');
      adapter = spec.worker ? sqliteStore({ file, worker: true, synchronous: sync }, { prefix }) : sqliteStore(build.port === 'state' ? countingSqlite(db, counter) : db, { prefix });
      const journalMode = String(Object.values(db.prepare('PRAGMA journal_mode').get())[0]).toUpperCase();
      const level = Number(Object.values(db.prepare('PRAGMA synchronous').get())[0]);
      const synchronous = spec.worker ? sync : ['OFF', 'NORMAL', 'FULL'][level];
      if (!synchronous) throw new Error('unsupported SQLite synchronous level');
      durability = { store: 'sqlite', journalMode, synchronous, worker: !!spec.worker };
    } else if (spec.store === 'postgres') {
      const { default: pg } = await import('pg');
      const url = spec.url ?? process.env.ATTEND_POSTGRES_URL;
      if (!url) throw new Error('ATTEND_POSTGRES_URL is required');
      if (spec.synchronousCommit && !['on', 'off', 'local', 'remote_write', 'remote_apply'].includes(spec.synchronousCommit)) throw new Error('invalid synchronous_commit');
      const pool = new pg.Pool({ connectionString: url, max: 4, options: spec.synchronousCommit ? `-c synchronous_commit=${spec.synchronousCommit}` : undefined });
      cleanup = async () => {
        try {
          const { rows } = await pool.query('SELECT tablename FROM pg_tables WHERE schemaname = current_schema()');
          for (const { tablename } of rows) if (tablename.startsWith(prefix)) await pool.query(`DROP TABLE "${tablename}" CASCADE`);
        } finally { await pool.end(); }
      };
      const module = await build.module('postgres');
      await module.migrate(pool, { prefix });
      const { rows } = await pool.query('SHOW synchronous_commit');
      durability = { store: 'postgres', synchronousCommit: rows[0].synchronous_commit };
      adapter = module.postgresStore(build.port === 'state' ? countingPostgres(pool, counter) : pool, { prefix });
    } else if (spec.store === 'redis') {
      const { createClient } = await import('redis');
      const url = spec.url ?? process.env.ATTEND_REDIS_URL;
      if (!url) throw new Error('ATTEND_REDIS_URL is required');
      const client = createClient({ url, socket: { reconnectStrategy: false, connectTimeout: 5000 } });
      client.on('error', () => {});
      await client.connect();
      const send = (args) => client.sendCommand(args);
      // The decision log waits with XREAD BLOCK on a connection of its own, as attend serve does.
      const blocking = client.duplicate();
      blocking.on('error', () => {});
      const keyPrefix = prefix.replaceAll('_', ':');
      cleanup = async () => {
        if (blocking.isOpen) blocking.destroy();
        try {
          let cursor = '0';
          do {
            const result = await send(['SCAN', cursor, 'MATCH', `${keyPrefix}*`, 'COUNT', '1000']);
            cursor = String(result[0]);
            if (result[1].length) await send(['UNLINK', ...result[1]]);
          } while (cursor !== '0');
        } finally { await client.quit(); }
      };
      await blocking.connect();
      const config = async (key) => {
        const reply = await send(['CONFIG', 'GET', key]);
        return String(reply instanceof Map ? reply.get(key) : Array.isArray(reply) ? reply[1] : reply[key]);
      };
      durability = { store: 'redis', appendonly: await config('appendonly') === 'yes', appendfsync: await config('appendfsync') };
      adapter = (await build.module('redis')).redisStore(build.port === 'state' ? countingRedis(send, counter) : send, { prefix: keyPrefix, blockingSend: (args) => blocking.sendCommand(args) });
    } else throw new Error(`unknown store: ${spec.store}`);
    const store = spec.store === 'memory' ? adapter : build.port === 'scope' ? countingScopePort(adapter, counter) : countingStatePort(adapter, counter);
    let closed = false;
    return {
      store, durability,
      stats() {
        const values = counter.latency.percentiles();
        return { commits: counter.commits, bytes: counter.bytes, commitLatencyMs: Object.fromEntries(Object.entries(values).map(([key, value]) => [key, value / 1000])) };
      },
      resetStats() { counter.bytes = 0; counter.commits = 0; counter.latency = createHistogram(); },
      async close() {
        if (closed) return;
        closed = true;
        try { await adapter.close?.(); } finally { await cleanup(); }
      },
    };
  } catch (error) {
    try { await adapter?.close?.(); } finally { await cleanup(); }
    throw error;
  }
}
