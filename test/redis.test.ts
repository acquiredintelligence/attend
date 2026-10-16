import { randomUUID } from 'node:crypto';
import { createClient, RESP_TYPES } from 'redis';
import { afterAll, describe, expect, test } from 'vitest';
import { redisLog, redisStore, type RedisSend } from '../src/redis.js';
import { initialRecord } from '../src/step.js';
import { crashSuite, logConformance, storeConformance } from '../src/testing.js';
import type { Event, ScopeRecord } from '../src/types.js';
import { aliasingCases } from './support/aliasing.js';

const url = process.env.ATTEND_REDIS_URL ?? 'redis://127.0.0.1:16379';
const run = `attend-test:${randomUUID().slice(0, 8)}:`;
const closers: (() => Promise<void>)[] = [];

/** A fresh connection. `resp` and `mapping` pick the reply shapes the adapter has to parse. */
async function connect(resp: 2 | 3 = 3, mapping: 'default' | 'buffers' | 'maps' | 'flat' = 'default'): Promise<RedisSend> {
  const client = createClient({ url, RESP: resp, socket: { reconnectStrategy: false, connectTimeout: 5000 } });
  // Failures also reject the pending command; this listener only keeps the emitter from throwing.
  client.on('error', () => undefined);
  await client.connect();
  closers.push(() => client.close());
  const typeMapping = {
    default: {},
    buffers: { [RESP_TYPES.BLOB_STRING]: Buffer, [RESP_TYPES.SIMPLE_STRING]: Buffer },
    maps: { [RESP_TYPES.MAP]: Map },
    flat: { [RESP_TYPES.MAP]: Array },
  }[mapping];
  return (args) => client.sendCommand(args, { typeMapping });
}

/** Connected on first use, inside a test, so an unreachable server fails every test rather than skipping them. */
let main: Promise<RedisSend> | undefined;
const shared = () => (main ??= connect());

afterAll(async () => {
  const send = await main?.catch(() => undefined);
  const keys = await send?.(['KEYS', `${run}*`]);
  if (send && Array.isArray(keys) && keys.length > 0) await send(['UNLINK', ...keys.map(String)]);
  await Promise.all(closers.map((close) => close()));
});

let prefixes = 0;
const fresh = () => `${run}${prefixes++}:`;

describe('redisStore conformance', () => {
  const make = async () => redisStore(await shared(), { prefix: fresh() });
  for (const c of storeConformance(make)) test(c.name, c.run);
  for (const c of crashSuite(make)) test(c.name, c.run, 120_000);
  for (const c of aliasingCases(make)) test(c.name, c.run);
});

describe('redisStore conformance over RESP2 with Buffer replies', () => {
  for (const c of storeConformance(async () => redisStore(await connect(2, 'buffers'), { prefix: fresh() }))) test(c.name, c.run);
});

describe('redisLog conformance without a blocking connection', () => {
  for (const c of logConformance(async () => redisLog(await shared(), { prefix: fresh() }), { canBlock: false })) test(c.name, c.run);
});

describe('redisLog conformance with a blocking connection', () => {
  for (const c of logConformance(async () => redisLog(await shared(), { prefix: fresh(), blockingSend: await connect() }), { canBlock: true })) test(c.name, c.run, 10_000);
});

describe('redisLog conformance over RESP2 with Buffer replies', () => {
  for (const c of logConformance(async () => redisLog(await connect(2, 'buffers'), { prefix: fresh(), blockingSend: await connect(2, 'buffers') }), { canBlock: true }))
    test(c.name, c.run, 10_000);
});

const record = (scope: string, version: number, dueAt: number | null): ScopeRecord => ({ ...initialRecord(scope), version, dueAt });
const event = (id: string): Event => ({ id, at: 0, name: 'n', data: { id } });

describe('redisStore across connections', () => {
  test('sixteen puts at one version from separate connections give exactly one ok', async () => {
    const prefix = fresh();
    const stores = await Promise.all(Array.from({ length: 16 }, async () => redisStore(await connect(), { prefix })));
    expect(await stores[0]?.put(record('s', 1, null), 0)).toBe('ok');
    const results = await Promise.all(stores.map((store, i) => store.put(record('s', 2, i), 1)));
    expect(results.filter((r) => r === 'ok')).toEqual(['ok']);
    expect(await stores[15]?.get('s')).toEqual(record('s', 2, results.indexOf('ok')));
  });

  test('a record survives a new store instance on a new connection', async () => {
    const prefix = fresh();
    const r: ScopeRecord = { ...record('s', 3, 50), cursors: { 'log:s': 'c9' }, seen: ['a'] };
    await redisStore(await connect(), { prefix }).put(r, 0);
    const store = redisStore(await connect(), { prefix });
    expect(await store.get('s')).toEqual(r);
    expect(await store.due(50, 10)).toEqual(['s']);
    expect(await store.put(record('s', 4, null), 3)).toBe('ok');
  });

  test('a fractional dueAt is due exactly when now reaches it', async () => {
    const store = redisStore(await shared(), { prefix: fresh() });
    await store.put(record('s', 1, 100.5), 0);
    expect(await store.due(100.4, 10)).toEqual([]);
    expect(await store.due(100.5, 10)).toEqual(['s']);
  });
});

describe('redisLog across connections', () => {
  test('eight concurrent appenders lose nothing and duplicate nothing for a tailing reader', async () => {
    const prefix = fresh();
    const appenders = await Promise.all(Array.from({ length: 8 }, async () => redisLog(await connect(), { prefix })));
    const reader = redisLog(await connect(), { prefix });
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

  test('positions of a dozen entries appended at once sort as strings in read order', async () => {
    const log = redisLog(await shared(), { prefix: fresh() });
    await log.append('s', 'abcdefghijkl'.split('').map(event));
    const entries = await log.read('s', null, { limit: 20 });
    const positions = entries.map((e) => e.position);
    expect(entries.map((e) => e.event.id).join('')).toBe('abcdefghijkl');
    expect([...positions].sort()).toEqual(positions);
    expect((await log.read('s', positions[9] ?? null, { limit: 20 })).map((e) => e.event.id)).toEqual(['k', 'l']);
  });

  test('a blocked read wakes for an append whatever shape the client decodes the XREAD map into', async () => {
    for (const mapping of ['default', 'maps', 'flat'] as const) {
      const prefix = fresh();
      const log = redisLog(await shared(), { prefix, blockingSend: await connect(3, mapping) });
      const pending = log.read('s', null, { limit: 10, blockMs: 5000 });
      await new Promise((resolve) => setTimeout(resolve, 20));
      await redisLog(await shared(), { prefix }).append('s', [event(mapping)]);
      expect((await pending).map((e) => e.event.id)).toEqual([mapping]);
    }
  });
});
