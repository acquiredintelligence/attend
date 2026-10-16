import { randomUUID } from 'node:crypto';
import pg from 'pg';
import { afterAll, describe, expect, test } from 'vitest';
import { migrate, postgresLog, postgresStore } from '../src/postgres.js';
import { createAttend } from '../src/index.js';
import { crashSuite, multiScopeCrashSuite, fakeClock, recordingSink, scriptedJudge, logConformance } from '../src/testing.js';

const url = process.env.ATTEND_POSTGRES_URL ?? 'postgres://postgres:attend@127.0.0.1:15432/attend';
const schema = `attend_p6b_${randomUUID().replaceAll('-', '').slice(0, 12)}`;
const config = { connectionString: url, options: `-c search_path=${schema}`, connectionTimeoutMillis: 5000 };
const clients: pg.Client[] = [];
const stores: ReturnType<typeof postgresStore>[] = [];

let ready: Promise<pg.Pool> | undefined;
const pool = () =>
  (ready ??= (async () => {
    const admin = new pg.Client({ connectionString: url, connectionTimeoutMillis: 5000 });
    try {
      await admin.connect();
      await admin.query(`CREATE SCHEMA ${schema}`);
    } finally { await admin.end(); }
    return new pg.Pool({ ...config, max: 4 });
  })());

afterAll(async () => {
  for (const store of stores) await store.close?.();
  await Promise.all(clients.map((c) => c.end()));
  const opened = await ready?.catch(() => undefined);
  await opened?.query(`DROP SCHEMA ${schema} CASCADE`);
  await opened?.end();
});

const connect = async () => {
  await pool();
  const client = new pg.Client(config);
  clients.push(client);
  await client.connect();
  return client;
};

let tables = 0;
const fresh = async () => {
  // NOTIFY channels are shared across schemas.
  const prefix = `${schema}_${tables++}_`;
  await migrate(await pool(), { prefix });
  return prefix;
};

const instance = async (prefix: string) => {
  const store = postgresStore(await pool(), { prefix });
  stores.push(store);
  return store;
};
const make = async () => instance(await fresh());

describe('public postgresStore', () => {
  for (const c of [...crashSuite(make), ...multiScopeCrashSuite(make)]) test(c.name, c.run, 300000);

  test('public store exposes load instead of the removed StateStore API', async () => {
    const store = await make();
    expect(typeof store.load).toBe('function');
    expect('get' in store).toBe(false);
    expect('put' in store).toBe(false);
    expect(await store.load('missing')).toBeNull();
  });

  test('createAttend delivers and recovers without deciding the same event again', async () => {
    const prefix = await fresh();
    const clock = fakeClock(1000);
    const judge = scriptedJudge([{ route: 'queue' }]);
    const sink = recordingSink();
    const config = { clock, judge, sink, kind: () => 'closing' as const, deadLetter: 'log' as const };
    const event = { id: 'a', at: 0, name: 'close', data: {} };
    const expected = [{
      id: 'cf8923dfae84f930', scope: 'scope', route: 'queue',
      probabilities: { interrupt: 0, queue: 1, note: 0, ask: 0, skip: 0, wait: 0 },
      predicates: {}, confidence: null,
      window: {
        id: 'cf8923dfae84f930', events: [{ ...event, kind: 'closing', seenAt: 1000 }],
        reason: 'forced', waits: 0, dropped: 0, gap: false, settledAt: 1000,
      },
      at: 1000,
    }];
    const first = createAttend({ ...config, store: await instance(prefix) });
    const controller = new AbortController();
    const done = first.run({ signal: controller.signal });
    try {
      await first.push('scope', [event]);
      await first.flush('scope');
      await clock.advance(0);
      await expect.poll(() => first.inspect('scope')).toMatchObject({
        batch: null, outbox: { decisions: [], deadLetters: [] },
        recent: [{ id: 'cf8923dfae84f930', route: 'queue', at: 1000 }],
      });
      expect(sink.delivered).toEqual(expected);
    } finally {
      controller.abort();
      await done;
    }
    const second = createAttend({ ...config, store: await instance(prefix) });
    const restart = new AbortController();
    const recovered = second.run({ signal: restart.signal });
    try {
      expect(await second.inspect('scope')).toEqual(await first.inspect('scope'));
      expect(await second.inspect('scope')).toMatchObject({
        seen: ['a'], batch: null, dueAt: null,
        recent: [{ id: 'cf8923dfae84f930', route: 'queue', at: 1000 }],
      });
      await second.push('scope', [event]);
      await second.flush('scope');
      await clock.advance(0);
      await expect.poll(() => second.inspect('scope')).toMatchObject({
        batch: null, buffer: { entries: [] }, outbox: { decisions: [], deadLetters: [] },
      });
      expect(judge.calls.map((call) => call.window.id)).toEqual(['cf8923dfae84f930']);
      expect(sink.delivered).toEqual(expected);
    } finally {
      restart.abort();
      await recovered;
    }
  }, 30000);
});

describe('postgresLog conformance', () => {
  for (const c of logConformance(async () => postgresLog(await pool(), { prefix: await fresh(), pollMs: 10 }), { canBlock: true })) test(c.name, c.run, 10_000);

  test('a prefix that is not a lowercase identifier is refused', async () => {
    await expect(migrate(await pool(), { prefix: 'x; DROP TABLE y; --' })).rejects.toThrow('attend/postgres: prefix must be lowercase letters, digits and underscores');
    const client = await pool();
    expect(() => postgresLog(client, { prefix: 'Attend_' })).toThrow('attend/postgres: prefix must be lowercase letters, digits and underscores, got "Attend_"');
  });
});

const event = (id: string) => ({ id, at: 0, name: 'n', data: { id } });

describe('postgresLog across connections', () => {
  test('concurrent migrate creates the new store tables and preserves EventLog entries', async () => {
    const prefix = await fresh();
    const client = await pool();
    const log = postgresLog(client, { prefix });
    await log.append('s', [event('a')]);
    await Promise.all(Array.from({ length: 8 }, () => migrate(client, { prefix })));
    const { rows } = await client.query(
      'SELECT tablename FROM pg_tables WHERE schemaname = $1 AND left(tablename, length($2)) = $2 ORDER BY tablename',
      [schema, prefix],
    );
    expect(rows).toEqual([
      { tablename: `${prefix}decisions` },
      { tablename: `${prefix}inputs` },
      { tablename: `${prefix}log` },
      { tablename: `${prefix}meta` },
      { tablename: `${prefix}scopes` },
      { tablename: `${prefix}snapshots` },
    ]);
    expect(await log.read('s', null, { limit: 10 })).toEqual([
      { position: '00000000000000000001', event: event('a') },
    ]);
  });

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
  }, 30_000);

  test('an event whose text holds a NUL round-trips', async () => {
    const log = postgresLog(await pool(), { prefix: await fresh() });
    const e = { id: 'a', at: 1, name: 'out', data: { raw: 'x\u0000y' }, text: 'nul \u0000 here' };
    await log.append('s', [e]);
    expect(await log.read('s', null, { limit: 10 })).toEqual([{ position: expect.stringMatching(/^\d{20}$/), event: e }]);
  });
});
