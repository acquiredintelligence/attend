import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { afterAll, describe, expect, test } from 'vitest';
import { createAttend } from '../src/index.js';
import { sqliteLog, sqliteStore } from '../src/sqlite.js';
import { crashSuite, multiScopeCrashSuite, fakeClock, recordingSink, scriptedJudge, logConformance } from '../src/testing.js';

const dir = mkdtempSync(join(tmpdir(), 'attend-p6b-'));
const opened: DatabaseSync[] = [];
const stores: ReturnType<typeof sqliteStore>[] = [];
const open = (path: string) => {
  const db = new DatabaseSync(path);
  db.exec('PRAGMA journal_mode=WAL');
  opened.push(db);
  return db;
};
afterAll(async () => {
  for (const store of stores) await store.close?.();
  for (const db of opened) if (db.isOpen) db.close();
  rmSync(dir, { recursive: true, force: true });
}, 120000);

let tables = 0;
const targets: [string, () => { db: DatabaseSync; prefix: string }][] = [
  [':memory:', () => ({ db: open(':memory:'), prefix: 'attend_' })],
  ['a file, one table prefix per case', () => ({ db: open(join(dir, 'shared.db')), prefix: `case${tables++}_` })],
];

for (const [where, make] of targets) {
  describe(`sqliteLog in ${where}`, () => {
    for (const c of logConformance(() => {
      const { db, prefix } = make();
      return sqliteLog(db, { prefix });
    }, { canBlock: false })) test(c.name, c.run);
  });
}

let files = 0;
for (const mode of ['in-thread WAL', 'worker WAL']) describe(`public sqliteStore ${mode}`, () => {
  const instance = (path: string) => {
    const store = mode === 'worker WAL' ? sqliteStore({ file: path, worker: true }) : sqliteStore(open(path));
    stores.push(store);
    return store;
  };
  const make = () => instance(join(dir, `store${files++}.db`));
  for (const c of [...crashSuite(make), ...multiScopeCrashSuite(make)]) test(c.name, c.run, 300000);

  test('public store exposes load instead of the removed StateStore API', async () => {
    const store = make();
    expect(typeof store.load).toBe('function');
    expect('get' in store).toBe(false);
    expect('put' in store).toBe(false);
    expect(await store.load('missing')).toBeNull();
  });

  test('createAttend delivers and recovers without deciding the same event again', async () => {
    const path = join(dir, `smoke${files++}.db`);
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
    const first = createAttend({ ...config, store: instance(path) });
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
    const second = createAttend({ ...config, store: instance(path) });
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

const event = (id: string) => ({ id, at: 0, name: 'n', data: { id } });

describe('sqliteLog over one file', () => {
  test('entries survive a new log instance and positions keep increasing', async () => {
    const path = join(dir, 'log-reopen.db');
    const first = open(path);
    await sqliteLog(first).append('s', [event('a'), event('b')]);
    first.close();
    const log = sqliteLog(open(path));
    await log.append('s', [event('c')]);
    expect(await log.read('s', null, { limit: 10 })).toEqual([
      { position: '00000000000000000001', event: event('a') },
      { position: '00000000000000000002', event: event('b') },
      { position: '00000000000000000003', event: event('c') },
    ]);
  });

  test('eight appenders on separate connections lose nothing and duplicate nothing for a tailing reader', async () => {
    const path = join(dir, 'tail.db');
    const appenders = Array.from({ length: 8 }, () => sqliteLog(open(path)));
    const reader = sqliteLog(open(path));
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
        await new Promise((resolve) => setImmediate(resolve));
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
});
