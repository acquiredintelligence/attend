import { randomUUID } from 'node:crypto';
import pg from 'pg';
import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import { migrate, postgresLog, postgresStore } from '../src/postgres.js';
import { initialRecord } from '../src/step.js';
import { crashSuite, logConformance, storeConformance } from '../src/testing.js';
import type { Event, ScopeRecord } from '../src/types.js';

const url = process.env.ATTEND_POSTGRES_URL ?? 'postgres://postgres:attend@127.0.0.1:15432/attend';
const schema = `attend_test_${randomUUID().replaceAll('-', '').slice(0, 12)}`;
const config = { connectionString: url, options: `-c search_path=${schema}`, connectionTimeoutMillis: 5000 };
let pool: pg.Pool;
const clients: pg.Client[] = [];

beforeAll(async () => {
  const admin = new pg.Client({ connectionString: url, connectionTimeoutMillis: 5000 });
  await admin.connect();
  await admin.query(`CREATE SCHEMA ${schema}`);
  await admin.end();
  pool = new pg.Pool({ ...config, max: 20 });
});

afterAll(async () => {
  await Promise.all(clients.map((c) => c.end()));
  await pool?.query(`DROP SCHEMA ${schema} CASCADE`);
  await pool?.end();
});

const connect = async () => {
  const client = new pg.Client(config);
  clients.push(client);
  await client.connect();
  return client;
};

let tables = 0;
const fresh = async () => {
  const prefix = `t${tables++}_`;
  await migrate(pool, { prefix });
  return prefix;
};

describe('postgresStore conformance', () => {
  const make = async () => postgresStore(pool, { prefix: await fresh() });
  for (const c of storeConformance(make)) test(c.name, c.run);
  for (const c of crashSuite(make)) test(c.name, c.run, 120_000);
});

describe('postgresLog conformance', () => {
  for (const c of logConformance(async () => postgresLog(pool, { prefix: await fresh(), pollMs: 10 }), { canBlock: true })) test(c.name, c.run, 10_000);
});

const record = (scope: string, version: number, dueAt: number | null): ScopeRecord => ({ ...initialRecord(scope), version, dueAt });
const event = (id: string): Event => ({ id, at: 0, name: 'n', data: { id } });

describe('postgresStore across connections', () => {
  test('sixteen puts at one version from separate connections give exactly one ok', async () => {
    const prefix = await fresh();
    const stores = await Promise.all(Array.from({ length: 16 }, async () => postgresStore(await connect(), { prefix })));
    expect(await stores[0]?.put(record('s', 1, null), 0)).toBe('ok');
    const results = await Promise.all(stores.map((store, i) => store.put(record('s', 2, i), 1)));
    expect(results.filter((r) => r === 'ok')).toEqual(['ok']);
    expect(await stores[15]?.get('s')).toEqual(record('s', 2, results.indexOf('ok')));
  });

  test('sixteen creates of one scope from separate connections give exactly one ok', async () => {
    const prefix = await fresh();
    const stores = await Promise.all(Array.from({ length: 16 }, async () => postgresStore(await connect(), { prefix })));
    const results = await Promise.all(stores.map((store, i) => store.put(record('s', 1, i), 0)));
    expect(results.filter((r) => r === 'ok')).toEqual(['ok']);
    expect(await stores[0]?.get('s')).toEqual(record('s', 1, results.indexOf('ok')));
  });

  test('a record survives a new store instance on a new connection, and migrate again keeps it', async () => {
    const prefix = await fresh();
    const first = await connect();
    const r: ScopeRecord = { ...record('s', 3, 50), cursor: 'c9', seen: ['a'] };
    await postgresStore(first, { prefix }).put(r, 0);
    await first.end();
    const second = await connect();
    await migrate(second, { prefix });
    const store = postgresStore(second, { prefix });
    expect(await store.get('s')).toEqual(r);
    expect(await store.due(50, 10)).toEqual(['s']);
    expect(await store.put(record('s', 4, null), 3)).toBe('ok');
  });

  test('a record whose event text holds a NUL round-trips', async () => {
    const store = postgresStore(pool, { prefix: await fresh() });
    const r: ScopeRecord = {
      ...record('s', 1, null),
      buffer: { entries: [{ id: 'a', at: 0, name: 'out', data: { raw: 'x\u0000y' }, text: 'nul \u0000 here', kind: 'consequence', seenAt: 0 }], dropped: 0, gap: false, forcedAt: null },
    };
    expect(await store.put(r, 0)).toBe('ok');
    expect(await store.get('s')).toEqual(r);
  });

  test('a fractional dueAt is due exactly when now reaches it', async () => {
    const store = postgresStore(pool, { prefix: await fresh() });
    expect(await store.put(record('s', 1, 100.5), 0)).toBe('ok');
    expect(await store.due(100.4, 10)).toEqual([]);
    expect(await store.due(100.5, 10)).toEqual(['s']);
  });

  test('eight concurrent migrations of one prefix all succeed', async () => {
    const prefix = `t${tables++}_`;
    const connections = await Promise.all(Array.from({ length: 8 }, () => connect()));
    await Promise.all(connections.map((c) => migrate(c, { prefix })));
    expect(await postgresStore(pool, { prefix }).put(record('s', 1, null), 0)).toBe('ok');
  });

  test('a prefix that is not a plain identifier is refused', async () => {
    await expect(migrate(pool, { prefix: 'x; DROP TABLE y; --' })).rejects.toThrow('attend/postgres: prefix must be letters, digits and underscores');
  });
});

describe('postgresLog across connections', () => {
  test('eight concurrent appenders lose nothing and duplicate nothing for a tailing reader', async () => {
    const prefix = await fresh();
    const appenders = await Promise.all(Array.from({ length: 8 }, async () => postgresLog(await connect(), { prefix })));
    const reader = postgresLog(await connect(), { prefix });
    const ids = (a: number) => Array.from({ length: 25 }, (_, b) => [0, 1, 2, 3].map((i) => `a${a}-b${b}-${i}`)).flat();
    let finished = false;
    const seen: string[] = [];
    const tail = (async () => {
      for (let after: string | null = null; ; ) {
        const settled = finished;
        const page = await reader.read('s', after, { limit: 7 });
        for (const entry of page) seen.push(entry.event.id);
        after = page.at(-1)?.position ?? after;
        if (page.length === 0 && settled) return;
      }
    })();
    await Promise.all(
      appenders.map(async (log, a) => {
        const mine = ids(a);
        for (let b = 0; b < mine.length; b += 4) await log.append('s', mine.slice(b, b + 4).map(event));
      }),
    );
    finished = true;
    await tail;
    expect(seen).toHaveLength(800);
    expect(new Set(seen).size).toBe(800);
    for (let a = 0; a < 8; a += 1) expect(seen.filter((id) => id.startsWith(`a${a}-`))).toEqual(ids(a));
  });

  test('an event whose text holds a NUL round-trips', async () => {
    const log = postgresLog(pool, { prefix: await fresh() });
    const e: Event = { id: 'a', at: 1, name: 'out', data: { raw: 'x\u0000y' }, text: 'nul \u0000 here' };
    await log.append('s', [e]);
    expect(await log.read('s', null, { limit: 10 })).toEqual([{ position: expect.stringMatching(/^\d{20}$/), event: e }]);
  });
});
