import { Worker } from 'node:worker_threads';
import { boundary } from '../internal/adapter.js';
import type { DeliveredRoute } from '../core/types.js';
import { openTables, queueCommits } from './sqlite-worker.js';
import type { EncodedWrite, Operations, RawRow } from './sqlite-worker.js';
import type { DecisionRow, Retention, ScopeStore, ScopeWrite, StoredScope } from './types.js';

/** A positional SQLite binding. */
export type SqliteValue = string | number | bigint | null;

/** Prepared statements supplied by the application's SQLite driver. */
export interface SqliteStatement {
  run(...params: SqliteValue[]): { changes: number | bigint };
  get(...params: SqliteValue[]): unknown;
  all(...params: SqliteValue[]): unknown[];
}

/** The synchronous SQLite handle used by the in-thread store. */
export interface SqliteDatabase {
  exec(sql: string): void;
  prepare(sql: string): SqliteStatement;
}

export interface SqliteStoreOptions {
  /** Prepended to the table names. Letters, digits and underscores. Default 'attend_'. */
  prefix?: string;
  /** Trims the decision log by this inside every transaction that appends rows. */
  retain?: Retention;
}

/**
 * Given a handle: in-thread, one transaction per macrotask turn, under the app's own pragmas
 * (`journal_mode`, `synchronous`, `busy_timeout`). Given a file: a worker thread owns the database
 * in WAL mode, `synchronous` FULL unless set, so commits are asynchronous and groups form under
 * load. Either way the tables are created if they are absent.
 */
export function sqliteStore(db: SqliteDatabase | { file: string; worker: true; synchronous?: 'NORMAL' | 'FULL' }, options?: SqliteStoreOptions): ScopeStore {
  const prefix = parse.tablePrefix(options?.prefix);
  const transport = 'worker' in db ? workerTransport(db, prefix, options?.retain) : localTransport(db, prefix, options?.retain);
  const engine = transport.engine;
  const waiters = new Set<() => void>();
  const store: ScopeStore = {
    durable: true,
    async load(scope) {
      const raw = await engine.load(scope);
      if (raw === null) return null;
      const snapshot = raw.snapshot;
      const stored: StoredScope = {
        version: Number(raw.version),
        snapshot: snapshot === null ? null : {
          version: Number(parse.column(snapshot, 'version')),
          policy: JSON.parse(parse.text(parse.column(snapshot, 'policy'))),
          reducer: Number(parse.column(snapshot, 'reducer')),
          record: parse.parseRecord(parse.column(snapshot, 'record'), scope),
        },
        tail: raw.tail.map((input) => JSON.parse(parse.text(input))),
      };
      return stored;
    },
    async commit(writes) {
      const scopes = new Set<string>();
      for (const write of writes) {
        if (scopes.has(write.scope)) throw new Error(`attend/sqlite: a group names scope "${write.scope}" twice`);
        scopes.add(write.scope);
      }
      if (writes.length === 0) return [];
      const result = await engine.commit(writes.map(encode));
      if (result.appended) for (const wake of waiters) wake();
      return result.outcomes;
    },
    async due(now, limit, after) {
      return (await engine.due(now, limit, after)).map((row) => ({
        due: Number(parse.column(row, 'due')), scope: parse.text(parse.column(row, 'scope')),
      }));
    },
    async remove(scope) { await engine.remove(scope); },
    log: {
      async epoch() { return parse.text(await engine.epoch()); },
      async bounds() {
        const raw = await engine.bounds();
        return { first: position(parse.column(raw, 'first')), last: position(parse.column(raw, 'last')) };
      },
      async read(query) {
        const raw = await engine.read({ ...query, after: query.after === null ? null : Number(parse.parsePosition(query.after)) });
        return { rows: raw.rows.map(decodeRow), checked: position(raw.checked), more: raw.more };
      },
      async find(kind, id) {
        const row = await engine.find(kind, id);
        return row === null ? null : decodeRow(row);
      },
      wait(after, blockMs, signal) {
        if (signal.aborted) return Promise.resolve();
        return new Promise<void>((resolve) => {
          let finished = false;
          let checking = false;
          const finish = () => {
            if (finished) return;
            finished = true;
            clearTimeout(timeout);
            clearInterval(poll);
            signal.removeEventListener('abort', finish);
            waiters.delete(check);
            resolve();
          };
          const check = () => {
            if (finished || checking) return;
            checking = true;
            void store.log.bounds().then(({ last }) => {
              if (last !== null && (after === null || store.log.compare(last, after) > 0)) finish();
            }).catch(finish).finally(() => { checking = false; });
          };
          const timeout = setTimeout(finish, blockMs);
          const poll = setInterval(check, 100);
          signal.addEventListener('abort', finish, { once: true });
          waiters.add(check);
          check();
        });
      },
      async trim(retention) { await engine.trim(retention); },
      compare(a, b) { return Math.sign(Number(parse.parsePosition(a)) - Number(parse.parsePosition(b))); },
    },
  };
  if (transport.close) store.close = transport.close;
  return store;
}

const parse = boundary('attend/sqlite');

type AsyncOperations = { [K in Exclude<keyof Operations, 'close'>]: (...args: Parameters<Operations[K]>) => Promise<ReturnType<Operations[K]>> };
type Transport = { engine: AsyncOperations; close?: () => Promise<void> };

function position(value: unknown): string | null {
  return value === null ? null : parse.formatPosition(value);
}

function encode(write: ScopeWrite): EncodedWrite {
  return {
    scope: write.scope, expectedVersion: write.expectedVersion,
    inputs: write.serialized?.inputs ?? write.inputs.map((input) => JSON.stringify(input)),
    snapshot: write.snapshot === null ? null : {
      version: write.snapshot.version, policy: JSON.stringify(write.snapshot.policy), reducer: write.snapshot.reducer,
      record: write.serialized?.snapshot ?? JSON.stringify(write.snapshot.record),
    },
    setDue: write.due !== undefined, due: write.due ?? null,
    appends: write.appends.map((row, i) => ({
      kind: row.kind, id: row.id, scope: row.scope, at: row.at, route: row.route,
      body: write.serialized?.appends[i] ?? JSON.stringify(row.body),
    })),
  };
}

function route(value: unknown): DeliveredRoute {
  const result = parse.text(value);
  if (result === 'interrupt' || result === 'queue' || result === 'note' || result === 'ask') return result;
  throw new Error(`attend/sqlite: unknown route "${result}"`);
}

function decodeRow(raw: RawRow): DecisionRow {
  const base = { position: parse.formatPosition(raw.position), scope: parse.text(raw.scope), id: parse.text(raw.id), at: Number(raw.at) };
  const kind = parse.text(raw.kind);
  const body = JSON.parse(parse.text(raw.body));
  if (kind === 'decision') return { ...base, kind, route: route(raw.route), body };
  if (kind === 'deadLetter') return { ...base, kind, route: raw.route === null ? null : route(raw.route), body };
  throw new Error(`attend/sqlite: unknown decision kind "${kind}"`);
}

function localTransport(db: SqliteDatabase, prefix: string, retain: Retention | undefined): Transport {
  const engine = openTables(db, prefix);
  return { engine: {
    commit: queueCommits(engine, retain),
    async load(...args) { return engine.load(...args); },
    async due(...args) { return engine.due(...args); },
    async remove(...args) { return engine.remove(...args); },
    async bounds() { return engine.bounds(); },
    async read(...args) { return engine.read(...args); },
    async find(...args) { return engine.find(...args); },
    async trim(...args) { return engine.trim(...args); },
    async epoch() { return engine.epoch(); },
  } };
}

function workerTransport(db: { file: string; synchronous?: 'NORMAL' | 'FULL' }, prefix: string, retain: Retention | undefined): Transport {
  if (db.synchronous !== undefined && db.synchronous !== 'NORMAL' && db.synchronous !== 'FULL') {
    throw new Error('attend/sqlite: synchronous must be NORMAL or FULL');
  }
  // Tests load this module from source.
  const url = new URL(import.meta.url.endsWith('.ts') ? './sqlite-worker.ts' : './sqlite-worker.js', import.meta.url);
  const worker = new Worker(url, { workerData: { attendSqlite: { file: db.file, prefix, synchronous: db.synchronous ?? 'FULL', retain } } });
  const pending = new Map<number, { resolve: (value: unknown) => void; reject: (error: Error) => void }>();
  let nextId = 0;
  let stopped: Error | undefined;
  let closing: Promise<void> | undefined;
  const fail = (error: Error) => {
    stopped = error;
    for (const request of pending.values()) request.reject(error);
    pending.clear();
    worker.unref();
  };
  const exited = new Promise<void>((resolve) => {
    worker.once('exit', () => {
      fail(stopped ?? new Error('attend/sqlite: the worker stopped'));
      resolve();
    });
  });
  worker.on('error', fail);
  worker.on('message', (reply: { id: number; value?: unknown; error?: string }) => {
    const request = pending.get(reply.id);
    if (!request) return;
    pending.delete(reply.id);
    if (reply.error !== undefined) request.reject(new Error(reply.error));
    else request.resolve(reply.value);
    if (pending.size === 0 && !closing) worker.unref();
  });
  worker.unref();
  function send<K extends keyof Operations>(op: K, args: Parameters<Operations[K]>): Promise<ReturnType<Operations[K]>> {
    if (stopped) return Promise.reject(stopped);
    if (closing && op !== 'close') return Promise.reject(new Error('attend/sqlite: the worker stopped'));
    return new Promise((resolve, reject) => {
      const id = nextId++;
      pending.set(id, { resolve: (value) => resolve(value as ReturnType<Operations[K]>), reject });
      worker.ref();
      try { worker.postMessage({ id, op, args }); } catch (error) {
        pending.delete(id);
        if (pending.size === 0 && !closing) worker.unref();
        reject(error);
      }
    });
  }
  return {
    engine: {
      commit: (...args) => send('commit', args),
      load: (...args) => send('load', args),
      due: (...args) => send('due', args),
      remove: (...args) => send('remove', args),
      bounds: () => send('bounds', []),
      read: (...args) => send('read', args),
      find: (...args) => send('find', args),
      trim: (...args) => send('trim', args),
      epoch: () => send('epoch', []),
    },
    close() {
      closing ??= (async () => {
        if (!stopped) await send('close', []);
        await exited;
      })();
      return closing;
    },
  };
}
