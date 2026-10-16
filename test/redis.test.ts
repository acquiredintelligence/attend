import { randomUUID } from 'node:crypto';
import { createClient, RESP_TYPES } from 'redis';
import { afterAll, describe, expect, test } from 'vitest';
import { redisLog, redisStore, type RedisSend } from '../src/redis.js';
import { createAttend } from '../src/index.js';
import { crashSuite, multiScopeCrashSuite, fakeClock, recordingSink, scriptedJudge, logConformance } from '../src/testing.js';

const url = process.env.ATTEND_REDIS_URL ?? 'redis://127.0.0.1:16379';
const run = `attend-p6b:{${randomUUID().replaceAll('-', '').slice(0, 12)}}:`;
const closers: (() => Promise<void>)[] = [];

async function connect(resp: 2 | 3 = 3, mapping: 'default' | 'buffers' | 'maps' | 'flat' = 'default'): Promise<RedisSend> {
  const client = createClient({ url, RESP: resp, socket: { reconnectStrategy: false, connectTimeout: 5000 } });
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

describe('public redisStore', () => {
  const make = async () => redisStore(await shared(), { prefix: fresh() });
  for (const c of [...crashSuite(make), ...multiScopeCrashSuite(make)]) test(c.name, c.run, 300000);

  test('public store exposes load instead of the removed StateStore API', async () => {
    const store = await make();
    expect(typeof store.load).toBe('function');
    expect('get' in store).toBe(false);
    expect('put' in store).toBe(false);
    expect(await store.load('missing')).toBeNull();
  });

  test('createAttend delivers and recovers without deciding the same event again', async () => {
    const prefix = fresh();
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
    const first = createAttend({ ...config, store: redisStore(await shared(), { prefix }) });
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
    const second = createAttend({ ...config, store: redisStore(await connect(), { prefix }) });
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

const event = (id: string) => ({ id, at: 0, name: 'n', data: { id } });

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
