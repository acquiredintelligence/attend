import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { afterAll, describe, expect, test } from 'vitest';
import { DEFAULT_POLICY } from '../../src/core/types.js';
import type { DecisionAppend, LoggedInput, ScopeRecord } from '../../src/core/types.js';
import { sqliteStore } from '../../src/store/sqlite.js';
import type { SqliteDatabase, SqliteStoreOptions } from '../../src/store/sqlite.js';
import type { ScopeStore, ScopeWrite, Snapshot } from '../../src/store/types.js';
import { decisionLogConformance, scopeStoreConformance } from '../../src/testing/conformance.js';

const dir = mkdtempSync(join(tmpdir(), 'attend-sqlite-store-'));
const handles: DatabaseSync[] = [];
const workers: ScopeStore[] = [];
let sequence = 0;
const file = () => join(dir, `${sequence++}.db`);

function open(path: string, journal: 'WAL' | 'DELETE' = 'WAL'): DatabaseSync {
  const db = new DatabaseSync(path);
  handles.push(db);
  db.exec(`PRAGMA journal_mode = ${journal}; PRAGMA synchronous = FULL; PRAGMA busy_timeout = 5000`);
  return db;
}

function worker(path: string, synchronous: 'NORMAL' | 'FULL' = 'FULL', options?: SqliteStoreOptions): ScopeStore {
  const store = sqliteStore({ file: path, worker: true, synchronous }, options);
  workers.push(store);
  return store;
}

afterAll(async () => {
  await Promise.all(workers.map((store) => store.close?.()));
  for (const db of handles) if (db.isOpen) db.close();
  rmSync(dir, { recursive: true, force: true });
});

/** bun:sqlite answers a missing row with null where node:sqlite answers undefined. */
function nullRows(db: DatabaseSync): SqliteDatabase {
  return {
    exec(sql) { db.exec(sql); },
    prepare(sql) {
      const statement = db.prepare(sql);
      return {
        run: (...params) => statement.run(...params),
        get: (...params) => statement.get(...params) ?? null,
        all: (...params) => statement.all(...params),
      };
    },
  };
}

const modes: { name: string; make: () => ScopeStore; concurrent?: () => Promise<[ScopeStore, ScopeStore]> }[] = [
  { name: 'in-thread memory', make: () => sqliteStore(open(':memory:')) },
  { name: 'in-thread memory, a driver that answers a missing row with null', make: () => sqliteStore(nullRows(open(':memory:'))) },
  ...(['WAL', 'DELETE'] as const).map((journal) => ({
    name: `in-thread file ${journal} FULL`,
    make: () => sqliteStore(open(file(), journal)),
    async concurrent(): Promise<[ScopeStore, ScopeStore]> {
      const path = file();
      return [sqliteStore(open(path, journal)), sqliteStore(open(path, journal))];
    },
  })),
  ...(['NORMAL', 'FULL'] as const).map((synchronous) => ({
    name: `worker WAL ${synchronous}`,
    make: () => worker(file(), synchronous),
    async concurrent(): Promise<[ScopeStore, ScopeStore]> {
      const path = file();
      return [worker(path, synchronous), worker(path, synchronous)];
    },
  })),
];

for (const mode of modes) {
  describe(mode.name, () => {
    for (const c of scopeStoreConformance(mode.make)) test(c.name, c.run, 30_000);
    for (const c of decisionLogConformance(mode.make, mode.concurrent ? { concurrent: mode.concurrent } : undefined)) test(c.name, c.run, 30_000);
  });
}

const input = (now = 1000): LoggedInput => ({ input: { kind: 'flush' }, now });
const record = (scope: string, version: number): ScopeRecord => ({
  scope, version, cursors: {}, seen: [], buffer: { entries: [], dropped: 0, gap: false, forcedAt: null },
  batch: null, outbox: { decisions: [], deadLetters: [] }, recent: [], dueAt: null,
});
const snapshot = (scope: string, version: number): Snapshot => ({ version, record: record(scope, version), policy: DEFAULT_POLICY, reducer: 1 });
const append = (id: string, scope = 's', at = 1000): DecisionAppend => ({
  kind: 'deadLetter', id, scope, at, route: null,
  body: {
    id, scope, at, stage: 'judge', decision: null, message: 'failed', permanent: false, attempts: 1,
    window: { id: `window:${id}`, events: [], reason: 'flush', waits: 0, dropped: 0, gap: false, settledAt: at },
  },
});
const write = (scope = 's', expectedVersion = 0, extra: Partial<ScopeWrite> = {}): ScopeWrite => ({
  scope, expectedVersion, inputs: [input()], snapshot: null, appends: [], ...extra,
});

const persistenceModes: { name: string; make: (path: string, options?: SqliteStoreOptions) => ScopeStore }[] = [
  { name: 'in-thread', make: (path, options) => sqliteStore(open(path), options) },
  { name: 'worker', make: (path, options) => worker(path, 'FULL', options) },
];

test('in-thread commits in one turn share exactly one transaction', async () => {
  const db = open(':memory:');
  const statements: string[] = [];
  const wrapped: SqliteDatabase = {
    exec(sql) { statements.push(sql); db.exec(sql); },
    prepare(sql) { return db.prepare(sql); },
  };
  const store = sqliteStore(wrapped);
  statements.length = 0;
  const commits = ['a', 'b', 'c'].map((scope) => store.commit([write(scope)]));
  expect(await Promise.all(commits)).toEqual([['ok'], ['ok'], ['ok']]);
  expect(statements.filter((sql) => sql === 'BEGIN IMMEDIATE')).toEqual(['BEGIN IMMEDIATE']);
  expect(statements.filter((sql) => sql === 'COMMIT')).toEqual(['COMMIT']);
  for (const scope of ['a', 'b', 'c']) expect(await store.load(scope)).toEqual({ version: 1, snapshot: null, tail: [input()] });
  expect(store.close).toBeUndefined();
  expect(db.isOpen).toBe(true);
});

for (const mode of persistenceModes) {
  describe(`${mode.name} focused behavior`, () => {
    test('a failing group rejects alone and rolls back its scope and decision', async () => {
      const store = mode.make(file());
      const good = store.commit([write('good')]);
      const bad = store.commit([write('rolled-back'), write('bad', 0, { appends: [append('bad', 'bad', Number.NaN)] })]);
      const results = await Promise.allSettled([good, bad]);
      expect(results[0]).toEqual({ status: 'fulfilled', value: ['ok'] });
      expect(results[1]?.status).toBe('rejected');
      expect(await store.load('good')).toEqual({ version: 1, snapshot: null, tail: [input()] });
      expect(await store.load('bad')).toBeNull();
      expect(await store.load('rolled-back')).toBeNull();
      expect(await store.log.find('deadLetter', 'bad')).toBeNull();
      expect(await store.log.read({ after: null, limit: 10 })).toEqual({ rows: [], checked: null, more: false });
    });

    test('binds supplied serialized strings and encodes writes without them', async () => {
      const store = mode.make(file());
      const storedSnapshot = snapshot('s', 2);
      const body = append('encoded');
      const serializedBody = { ...body.body, message: 'serialized body' };
      expect(await store.commit([write('s', 0, {
        serialized: { inputs: [JSON.stringify(input(2000))], snapshot: null, appends: [] },
      })])).toEqual(['ok']);
      expect(await store.load('s')).toEqual({ version: 1, snapshot: null, tail: [input(2000)] });
      expect(await store.commit([write('s', 1, {
        snapshot: storedSnapshot, appends: [body],
        serialized: {
          inputs: [JSON.stringify(input())], snapshot: JSON.stringify({ ...storedSnapshot.record, seen: ['serialized'] }),
          appends: [JSON.stringify(serializedBody)],
        },
      })])).toEqual(['ok']);
      expect(await store.load('s')).toEqual({ version: 2, snapshot: { ...storedSnapshot, record: { ...storedSnapshot.record, seen: ['serialized'] } }, tail: [] });
      expect(await store.log.find('deadLetter', 'encoded')).toEqual({ ...body, body: serializedBody, position: '00000000000000000001' });
      const fallback = snapshot('fallback', 1);
      const row = append('fallback', 'fallback');
      expect(await store.commit([write('fallback', 0, { snapshot: fallback, appends: [row] })])).toEqual(['ok']);
      expect(await store.commit([write('fallback', 1, { inputs: [input(3000)] })])).toEqual(['ok']);
      expect(await store.load('fallback')).toEqual({ version: 2, snapshot: fallback, tail: [input(3000)] });
      expect(await store.log.find('deadLetter', 'fallback')).toEqual({ ...row, position: '00000000000000000002' });
    });

    test('scope, snapshot, tail, due, decisions and epoch survive a new instance', async () => {
      const path = file();
      const first = mode.make(path);
      const snap = snapshot('s', 1);
      const row = append('durable');
      expect(await first.commit([write('s', 0, { snapshot: snap, appends: [row], due: 1234.5 })])).toEqual(['ok']);
      expect(await first.commit([write('s', 1, { inputs: [input(2000)] })])).toEqual(['ok']);
      const epoch = await first.log.epoch();
      expect(typeof epoch).toBe('string');
      expect(epoch.length).toBeGreaterThan(0);
      await first.close?.();
      if (mode.name === 'in-thread') handles.at(-1)?.close();
      const second = mode.make(path);
      expect(second.durable).toBe(true);
      expect(await second.load('s')).toEqual({ version: 2, snapshot: snap, tail: [input(2000)] });
      expect(await second.due(1234.4, 10)).toEqual([]);
      expect(await second.due(1234.5, 10)).toEqual([{ due: 1234.5, scope: 's' }]);
      expect(await second.log.find('deadLetter', 'durable')).toEqual({ ...row, position: '00000000000000000001' });
      expect(await second.log.epoch()).toBe(epoch);
    });

    test('retain keeps the newest two rows and a commit without appends does not trim', async () => {
      const path = file();
      const store = mode.make(path, { retain: { maxRows: 2 } });
      for (let i = 0; i < 5; i++) expect(await store.commit([write('s', i, { appends: [append(`${i}`)] })])).toEqual(['ok']);
      expect((await store.log.read({ after: null, limit: 10 })).rows.map((row) => row.id)).toEqual(['3', '4']);
      const other = mode.make(path);
      expect(await other.commit([write('other', 0, { appends: [append('5', 'other'), append('6', 'other')] })])).toEqual(['ok']);
      expect(await store.commit([write('s', 5)])).toEqual(['ok']);
      expect((await store.log.read({ after: null, limit: 10 })).rows.map((row) => row.id)).toEqual(['3', '4', '5', '6']);
      expect(await store.commit([write('s', 6, { appends: [append('4')] })])).toEqual(['ok']);
      expect((await store.log.read({ after: null, limit: 10 })).rows.map((row) => row.id)).toEqual(['3', '4', '5', '6']);
    });

    test('wait sees another connection commit within one second', async () => {
      const path = file();
      const first = mode.make(path);
      const second = mode.make(path);
      expect(await first.commit([write('s', 0, { appends: [append('first')] })])).toEqual(['ok']);
      const head = (await first.log.bounds()).last;
      expect(head).toBe('00000000000000000001');
      const abort = new AbortController();
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        const waiting = first.log.wait(head, 5000, abort.signal).then(() => 'woke');
        expect(await second.commit([write('s', 1, { appends: [append('second')] })])).toEqual(['ok']);
        expect(await Promise.race([waiting, new Promise<string>((resolve) => { timer = setTimeout(() => resolve('timed out'), 1000); })])).toBe('woke');
      } finally {
        clearTimeout(timer);
        abort.abort();
      }
    });

    test('a prefix that is not a plain identifier throws synchronously', () => {
      expect(() => mode.make(file(), { prefix: 'x; DROP TABLE y' })).toThrow('attend/sqlite: prefix must be letters, digits and underscores');
    });
  });
}

test('worker close drains requests and calls after close reject', async () => {
  const store = worker(file());
  const committed = store.commit([write()]);
  const loaded = store.load('missing');
  const closed = store.close?.();
  expect(await committed).toEqual(['ok']);
  expect(await loaded).toBeNull();
  expect(await closed).toBeUndefined();
  await expect(store.load('s')).rejects.toThrow('attend/sqlite: the worker stopped');
  await expect(store.commit([write('t')])).rejects.toThrow('attend/sqlite: the worker stopped');
  expect(await store.commit([])).toEqual([]);
  await expect(store.log.wait(null, 1000, new AbortController().signal)).resolves.toBeUndefined();
});

test('worker uses WAL by default', async () => {
  const path = file();
  const store = sqliteStore({ file: path, worker: true });
  workers.push(store);
  expect(await store.commit([write()])).toEqual(['ok']);
  const db = new DatabaseSync(path);
  handles.push(db);
  expect(db.prepare('PRAGMA journal_mode').get()).toEqual({ journal_mode: 'wal' });
});


test('a failed transaction rejects every queued group without applying changes', async () => {
  for (const failure of ['BEGIN IMMEDIATE', 'ROLLBACK TO attend_group', 'COMMIT']) {
    const db = open(':memory:');
    let armed = false;
    const wrapped: SqliteDatabase = {
      exec(sql) {
        if (armed && sql === failure) throw new Error(`injected ${failure}`);
        db.exec(sql);
      },
      prepare(sql) { return db.prepare(sql); },
    };
    const store = sqliteStore(wrapped);
    armed = true;
    const results = await Promise.allSettled([
      store.commit([write('good')]),
      store.commit([write('bad', 0, { appends: [append('bad', 'bad', Number.NaN)] })]),
    ]);
    expect(results).toEqual([
      { status: 'rejected', reason: new Error(`injected ${failure}`) },
      { status: 'rejected', reason: new Error(`injected ${failure}`) },
    ]);
    expect(await store.load('good')).toBeNull();
    expect(await store.load('bad')).toBeNull();
    expect(await store.log.read({ after: null, limit: 10 })).toEqual({ rows: [], checked: null, more: false });
    armed = false;
    expect(await store.commit([write('good')])).toEqual(['ok']);
    expect(await store.load('good')).toEqual({ version: 1, snapshot: null, tail: [input()] });
  }
});

test('worker startup failure rejects pending and later calls while wait resolves', async () => {
  const store = sqliteStore({ file: join(dir, 'missing', 'store.db'), worker: true });
  const results = await Promise.allSettled([store.load('s'), store.commit([write()])]);
  expect(results.map((result) => result.status)).toEqual(['rejected', 'rejected']);
  await expect(store.load('s')).rejects.toThrow();
  await expect(store.log.wait(null, 1000, new AbortController().signal)).resolves.toBeUndefined();
  expect(await store.close?.()).toBeUndefined();
});
