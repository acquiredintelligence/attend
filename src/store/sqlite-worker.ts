import { isMainThread, parentPort, workerData } from 'node:worker_threads';
import type { SqliteDatabase, SqliteValue } from './sqlite.js';
import type { CommitOutcome, DecisionQuery, DueKey, Retention } from './types.js';

/** A write whose JSON was encoded before entering the commit queue. */
export interface EncodedWrite {
  scope: string;
  expectedVersion: number;
  inputs: string[];
  snapshot: { version: number; policy: string; reducer: number; record: string } | null;
  setDue: boolean;
  due: number | null;
  appends: { kind: string; id: string; scope: string; at: number; route: string | null; body: string }[];
}

/** A scope with driver values and unparsed JSON. */
export interface RawScope {
  version: unknown;
  snapshot: unknown;
  tail: unknown[];
}

/** A decision with its numeric position and unparsed body. */
export interface RawRow {
  position: number;
  scope: unknown;
  kind: unknown;
  id: unknown;
  at: unknown;
  route: unknown;
  body: unknown;
}

/** A page whose cursor is still a numeric rowid. */
export interface RawPage {
  rows: RawRow[];
  checked: number | null;
  more: boolean;
}

/** A query with its cursor decoded before transport. */
export type EncodedQuery = Omit<DecisionQuery, 'after'> & { after: number | null };

/** Outcomes and whether this group inserted any decision rows. */
export interface CommitResult {
  outcomes: CommitOutcome[];
  appended: boolean;
}

type GroupResult = { value: CommitResult } | { error: unknown };

function column(row: unknown, name: string): unknown {
  if (typeof row !== 'object' || row === null) throw new Error('attend/sqlite: expected a row');
  return Reflect.get(row, name);
}

function rawRow(row: unknown): RawRow {
  return {
    position: Number(column(row, 'position')), scope: column(row, 'scope'), kind: column(row, 'kind'),
    id: column(row, 'id'), at: column(row, 'at'), route: column(row, 'route'), body: column(row, 'body'),
  };
}

/** No row is `undefined` from node:sqlite and `null` from bun:sqlite. */
function absent(row: unknown): boolean {
  return row === undefined || row === null;
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/** Creates or adopts the tables and returns the synchronous SQL engine. */
export function openTables(db: SqliteDatabase, prefix: string) {
  const scopes = `${prefix}scopes`;
  const inputs = `${prefix}inputs`;
  const snapshots = `${prefix}snapshots`;
  const decisions = `${prefix}decisions`;
  const rollback = () => {
    try { db.exec('ROLLBACK'); } catch { /* BEGIN may itself have failed. */ }
  };
  const transaction = <T>(run: () => T): T => {
    try {
      db.exec('BEGIN IMMEDIATE');
      const result = run();
      db.exec('COMMIT');
      return result;
    } catch (error) {
      rollback();
      throw error;
    }
  };
  db.exec(`
    CREATE TABLE IF NOT EXISTS ${scopes} (scope TEXT PRIMARY KEY, version INTEGER NOT NULL, due REAL);
    CREATE INDEX IF NOT EXISTS ${prefix}scopes_due ON ${scopes} (due, scope) WHERE due IS NOT NULL;
    CREATE TABLE IF NOT EXISTS ${inputs} (scope TEXT NOT NULL, version INTEGER NOT NULL, input TEXT NOT NULL, PRIMARY KEY (scope, version)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS ${snapshots} (scope TEXT PRIMARY KEY, version INTEGER NOT NULL, policy TEXT NOT NULL, reducer INTEGER NOT NULL, record TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS ${decisions} (position INTEGER PRIMARY KEY AUTOINCREMENT, scope TEXT NOT NULL, kind TEXT NOT NULL, id TEXT NOT NULL, at REAL NOT NULL, route TEXT, body TEXT NOT NULL, UNIQUE (kind, id));
    CREATE INDEX IF NOT EXISTS ${prefix}decisions_scope ON ${decisions} (scope, position);
    CREATE INDEX IF NOT EXISTS ${prefix}decisions_at ON ${decisions} (at);
    CREATE TABLE IF NOT EXISTS ${prefix}meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
  `);
  db.prepare(`INSERT INTO ${prefix}meta (key, value) VALUES ('epoch', ?) ON CONFLICT (key) DO NOTHING`).run(crypto.randomUUID());
  const epoch = column(db.prepare(`SELECT value FROM ${prefix}meta WHERE key = 'epoch'`).get(), 'value');
  const selectHead = db.prepare(`SELECT max(position) AS head FROM ${decisions}`);
  const head = () => {
    const value = column(selectHead.get(), 'head');
    return value === null ? null : Number(value);
  };
  const newest = db.prepare(`SELECT position FROM ${decisions} ORDER BY position DESC LIMIT 1 OFFSET ?`);
  const firstSince = db.prepare(`SELECT position FROM ${decisions} WHERE at >= ? ORDER BY position LIMIT 1`);
  const dropBelow = db.prepare(`DELETE FROM ${decisions} WHERE position < ?`);
  const trim = (retention: Retention) => {
    const last = head();
    if (last === null) return;
    const floors: number[] = [];
    if (retention.maxRows !== undefined) {
      const row = newest.get(Math.max(retention.maxRows - 1, 0));
      if (!absent(row)) floors.push(Number(column(row, 'position')));
    }
    if (retention.olderThanMs !== undefined) {
      const row = firstSince.get(Date.now() - retention.olderThanMs);
      floors.push(absent(row) ? last : Number(column(row, 'position')));
    }
    if (floors.length) dropBelow.run(Math.min(last, Math.max(...floors)));
  };
  const create = db.prepare(`INSERT INTO ${scopes} (scope, version, due) VALUES (?, ?, ?) ON CONFLICT (scope) DO NOTHING`);
  const advance = db.prepare(`UPDATE ${scopes} SET version = ? WHERE scope = ? AND version = ?`);
  const advanceDue = db.prepare(`UPDATE ${scopes} SET version = ?, due = ? WHERE scope = ? AND version = ?`);
  const insertInput = db.prepare(`INSERT INTO ${inputs} (scope, version, input) VALUES (?, ?, ?)`);
  const upsertSnapshot = db.prepare(`INSERT INTO ${snapshots} (scope, version, policy, reducer, record) VALUES (?, ?, ?, ?, ?) ON CONFLICT (scope) DO UPDATE SET version = excluded.version, policy = excluded.policy, reducer = excluded.reducer, record = excluded.record`);
  const trimInputs = db.prepare(`DELETE FROM ${inputs} WHERE scope = ? AND version <= ?`);
  const append = db.prepare(`INSERT INTO ${decisions} (scope, kind, id, at, route, body) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (kind, id) DO NOTHING`);
  const selectScope = db.prepare(`SELECT version FROM ${scopes} WHERE scope = ?`);
  const selectSnapshot = db.prepare(`SELECT version, policy, reducer, record FROM ${snapshots} WHERE scope = ?`);
  const selectTail = db.prepare(`SELECT input FROM ${inputs} WHERE scope = ? ORDER BY version`);
  const selectDue = db.prepare(`SELECT due, scope FROM ${scopes} WHERE due <= ? ORDER BY due, scope LIMIT ?`);
  const selectDueAfter = db.prepare(`SELECT due, scope FROM ${scopes} WHERE due <= ? AND (due, scope) > (?, ?) ORDER BY due, scope LIMIT ?`);
  const removals = [scopes, inputs, snapshots].map((table) => db.prepare(`DELETE FROM ${table} WHERE scope = ?`));
  const selectBounds = db.prepare(`SELECT (SELECT min(position) FROM ${decisions}) AS first, (SELECT max(position) FROM ${decisions}) AS last`);
  const scan = (scoped: boolean) => db.prepare(`SELECT position, scope, kind, id, at, route,
    CASE WHEN (? IS NULL OR kind IN (SELECT value FROM json_each(?)))
      AND (? IS NULL OR route IN (SELECT value FROM json_each(?)))
      AND (? IS NULL OR at >= ?)
      AND (? IS NULL OR substr(scope, 1, length(?)) = ?) THEN body END AS body
    FROM ${decisions} WHERE ${scoped ? 'scope = ? AND ' : ''}position > ? AND position <= ? ORDER BY position LIMIT ?`);
  const scanAll = scan(false);
  const scanScope = scan(true);
  const selectRow = db.prepare(`SELECT position, scope, kind, id, at, route, body FROM ${decisions} WHERE kind = ? AND id = ?`);
  const write = (writes: EncodedWrite[]): CommitResult => {
    let appended = false;
    const outcomes = writes.map((w): CommitOutcome => {
      const final = w.expectedVersion + w.inputs.length;
      const result = w.expectedVersion === 0
        ? create.run(w.scope, final, w.due)
        : w.setDue ? advanceDue.run(final, w.due, w.scope, w.expectedVersion) : advance.run(final, w.scope, w.expectedVersion);
      if (Number(result.changes) === 0) return 'conflict';
      for (const [i, input] of w.inputs.entries()) {
        const version = w.expectedVersion + 1 + i;
        if (w.snapshot === null || version > w.snapshot.version) insertInput.run(w.scope, version, input);
      }
      if (w.snapshot !== null) {
        upsertSnapshot.run(w.scope, w.snapshot.version, w.snapshot.policy, w.snapshot.reducer, w.snapshot.record);
        trimInputs.run(w.scope, w.snapshot.version);
      }
      for (const row of w.appends) {
        if (Number(append.run(row.scope, row.kind, row.id, row.at, row.route, row.body).changes) > 0) appended = true;
      }
      return 'ok';
    });
    return { outcomes, appended };
  };
  return {
    epoch: () => epoch,
    load(scope: string): RawScope | null {
      db.exec('SAVEPOINT attend_load');
      try {
        const row = selectScope.get(scope);
        if (absent(row)) return null;
        const snapshot = selectSnapshot.get(scope);
        return {
          version: column(row, 'version'),
          snapshot: absent(snapshot) ? null : snapshot,
          tail: selectTail.all(scope).map((r) => column(r, 'input')),
        };
      } finally {
        db.exec('RELEASE attend_load');
      }
    },
    commit(groups: EncodedWrite[][], retention?: Retention): GroupResult[] {
      return transaction(() => {
        const results = groups.map((group): GroupResult => {
          db.exec('SAVEPOINT attend_group');
          let value: CommitResult;
          try {
            value = write(group);
          } catch (error) {
            db.exec('ROLLBACK TO attend_group');
            db.exec('RELEASE attend_group');
            return { error };
          }
          db.exec('RELEASE attend_group');
          return { value };
        });
        if (retention && results.some((r) => 'value' in r && r.value.appended)) trim(retention);
        return results;
      });
    },
    due(now: number, limit: number, after?: DueKey) {
      return after ? selectDueAfter.all(now, after.due, after.scope, limit) : selectDue.all(now, limit);
    },
    remove(scope: string) {
      transaction(() => {
        for (const removal of removals) removal.run(scope);
      });
    },
    bounds() {
      return selectBounds.get();
    },
    read(query: EncodedQuery): RawPage {
      const last = head();
      const scanLimit = query.scanLimit ?? 4 * query.limit;
      const filtered = query.kinds !== undefined || query.routes !== undefined || query.sinceAt !== undefined || !!query.scopePrefix;
      const budget = filtered ? scanLimit : Math.min(query.limit, scanLimit);
      const kinds = query.kinds === undefined ? null : JSON.stringify(query.kinds);
      const routes = query.routes === undefined ? null : JSON.stringify(query.routes);
      const sinceAt = query.sinceAt ?? null;
      const scopePrefix = query.scopePrefix || null;
      const filters: SqliteValue[] = [kinds, kinds, routes, routes, sinceAt, sinceAt, scopePrefix, scopePrefix, scopePrefix];
      const range: SqliteValue[] = [query.after ?? 0, last ?? 0, budget + 1];
      const rows = query.scope === undefined ? scanAll.all(...filters, ...range) : scanScope.all(...filters, query.scope, ...range);
      const matched: RawRow[] = [];
      let examined = 0;
      let checked = query.after;
      for (const row of rows) {
        if (examined === scanLimit || matched.length === query.limit) return { rows: matched, checked, more: true };
        examined++;
        const raw = rawRow(row);
        checked = raw.position;
        if (raw.body !== null) matched.push(raw);
      }
      return { rows: matched, checked: last === null ? query.after : Math.max(last, query.after ?? 0), more: false };
    },
    find(kind: string, id: string): RawRow | null {
      const row = selectRow.get(kind, id);
      return absent(row) ? null : rawRow(row);
    },
    trim(retention: Retention) { transaction(() => trim(retention)); },
  };
}

/** The synchronous operations shared by the two transports. */
export type Engine = ReturnType<typeof openTables>;

/** Coalesces every group queued before the next immediate into one transaction. */
export function queueCommits(engine: Engine, retention?: Retention) {
  let queued: { writes: EncodedWrite[]; resolve: (value: CommitResult) => void; reject: (error: unknown) => void }[] = [];
  return (writes: EncodedWrite[]): Promise<CommitResult> => new Promise((resolve, reject) => {
    queued.push({ writes, resolve, reject });
    if (queued.length !== 1) return;
    setImmediate(() => {
      const batch = queued;
      queued = [];
      try {
        const results = engine.commit(batch.map((group) => group.writes), retention);
        for (const [i, result] of results.entries()) {
          const group = batch[i];
          if (!group) continue;
          if ('error' in result) group.reject(result.error);
          else group.resolve(result.value);
        }
      } catch (error) {
        for (const group of batch) group.reject(error);
      }
    });
  });
}

/**
 * Two connections switching one new file to WAL at once: the loser gets SQLITE_BUSY at once, without
 * waiting out busy_timeout. So retry, for as long as busy_timeout would have waited.
 */
function useWal(db: SqliteDatabase): void {
  for (let attempt = 1; ; attempt += 1) {
    try {
      db.exec('PRAGMA journal_mode = WAL');
      return;
    } catch (error) {
      if (attempt === 50 || Reflect.get(Object(error), 'errcode') !== 5) throw error;
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 100);
    }
  }
}

/** Operations sent across the worker port. */
export interface Operations extends Omit<Engine, 'commit'> {
  commit(writes: EncodedWrite[]): CommitResult;
  close(): void;
}

/** A request ties its operation name to its positional arguments. */
export type Request = { [K in keyof Operations]: { id: number; op: K; args: Parameters<Operations[K]> } }[keyof Operations];

if (!isMainThread && parentPort && workerData?.attendSqlite) {
  const port = parentPort;
  const { file, prefix, synchronous, retain } = workerData.attendSqlite;
  const { DatabaseSync } = await import('node:sqlite');
  const db = new DatabaseSync(file);
  db.exec('PRAGMA busy_timeout = 5000');
  useWal(db);
  db.exec(`PRAGMA synchronous = ${synchronous}`);
  const engine = openTables(db, prefix);
  const commit = queueCommits(engine, retain);
  const pending = new Set<Promise<void>>();
  port.on('message', (request: Request) => {
    const run = async () => {
      try {
        let value: unknown;
        switch (request.op) {
          case 'commit': value = await commit(...request.args); break;
          case 'load': value = engine.load(...request.args); break;
          case 'due': value = engine.due(...request.args); break;
          case 'remove': value = engine.remove(...request.args); break;
          case 'bounds': value = engine.bounds(...request.args); break;
          case 'read': value = engine.read(...request.args); break;
          case 'find': value = engine.find(...request.args); break;
          case 'trim': value = engine.trim(...request.args); break;
          case 'epoch': value = engine.epoch(...request.args); break;
          case 'close': await Promise.all(pending); db.close(); break;
        }
        port.postMessage({ id: request.id, value });
        if (request.op === 'close') port.close();
      } catch (error) {
        port.postMessage({ id: request.id, error: message(error) });
      }
    };
    const task = run();
    pending.add(task);
    void task.finally(() => pending.delete(task));
  });
}
