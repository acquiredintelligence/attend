import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { afterAll, describe, expect, test } from 'vitest';
import { sqliteLog, sqliteStore } from '../src/sqlite.js';
import { initialRecord } from '../src/step.js';
import { logConformance, storeConformance } from '../src/testing.js';
import type { Event, ScopeRecord } from '../src/types.js';

const dir = mkdtempSync(join(tmpdir(), 'attend-sqlite-'));
const opened: DatabaseSync[] = [];
const open = (path: string) => {
  const db = new DatabaseSync(path);
  opened.push(db);
  return db;
};
afterAll(() => {
  for (const db of opened) if (db.isOpen) db.close();
  rmSync(dir, { recursive: true, force: true });
});

let tables = 0;
const targets: [string, () => { db: DatabaseSync; prefix: string }][] = [
  [':memory:', () => ({ db: open(':memory:'), prefix: 'attend_' })],
  ['a file, one table prefix per case', () => ({ db: open(join(dir, 'shared.db')), prefix: `case${tables++}_` })],
];

for (const [where, make] of targets) {
  describe(`sqliteStore in ${where}`, () => {
    const store = () => {
      const { db, prefix } = make();
      return sqliteStore(db, { prefix });
    };
    for (const c of storeConformance(store)) test(c.name, c.run);
  });

  describe(`sqliteLog in ${where}`, () => {
    for (const c of logConformance(() => {
      const { db, prefix } = make();
      return sqliteLog(db, { prefix });
    }, { canBlock: false })) test(c.name, c.run);
  });
}

const record = (scope: string, version: number, dueAt: number | null): ScopeRecord => ({ ...initialRecord(scope), version, dueAt });
const event = (id: string): Event => ({ id, at: 0, name: 'n', data: { id } });

describe('sqliteStore over one file', () => {
  test('sixteen puts at one version from separate connections give exactly one ok', async () => {
    const path = join(dir, 'race.db');
    const stores = Array.from({ length: 16 }, () => sqliteStore(open(path)));
    expect(await stores[0]?.put(record('s', 1, null), 0)).toBe('ok');
    const results = await Promise.all(stores.map((store, i) => store.put(record('s', 2, i), 1)));
    expect(results.filter((r) => r === 'ok')).toEqual(['ok']);
    expect(await stores[15]?.get('s')).toEqual(record('s', 2, results.indexOf('ok')));
  });

  test('a record survives a new store instance over the same file', async () => {
    const path = join(dir, 'reopen.db');
    const first = open(path);
    const r: ScopeRecord = { ...record('s', 3, 50), cursors: { 'log:s': 'c9' }, seen: ['a'] };
    await sqliteStore(first).put(r, 0);
    first.close();
    const store = sqliteStore(open(path));
    expect(await store.get('s')).toEqual(r);
    expect(await store.due(50, 10)).toEqual(['s']);
    expect(await store.put(record('s', 4, null), 3)).toBe('ok');
  });

  test('a fractional dueAt is due exactly when now reaches it', async () => {
    const store = sqliteStore(open(':memory:'));
    await store.put(record('s', 1, 100.5), 0);
    expect(await store.due(100.4, 10)).toEqual([]);
    expect(await store.due(100.5, 10)).toEqual(['s']);
  });

  test('a prefix that is not a plain identifier is refused', () => {
    expect(() => sqliteStore(open(':memory:'), { prefix: 'x; DROP TABLE y; --' })).toThrow('attend/sqlite: prefix must be letters, digits and underscores');
  });
});

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
