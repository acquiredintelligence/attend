/**
 * `attend/sqlite`: a `StateStore` and an `EventLog` over one SQLite database. Takes any
 * object shaped like `node:sqlite`'s `DatabaseSync` (Bun's `Database` and better-sqlite3
 * fit too), so importing this module loads no driver. Each adapter creates its table and
 * index on construction if they are absent, so a second instance over the same file adopts
 * what the first wrote.
 */
import { boundary } from './internal/adapter.js';
import type { EventLog, LogEntry, StateStore } from './types.js';

const { column, formatPosition, parseEvent, parsePosition, parseRecord, tablePrefix, text } = boundary('attend/sqlite');

export type SqliteValue = string | number | bigint | null;

export interface SqliteStatement {
  run(...params: SqliteValue[]): { changes: number | bigint };
  get(...params: SqliteValue[]): unknown;
  all(...params: SqliteValue[]): unknown[];
}

export interface SqliteDatabase {
  exec(sql: string): void;
  prepare(sql: string): SqliteStatement;
}

export interface SqliteOptions {
  /** Prepended to the table names. Letters, digits and underscores. Default 'attend_'. */
  prefix?: string;
}

export function sqliteStore(db: SqliteDatabase, options?: SqliteOptions): StateStore {
  const table = `${tablePrefix(options?.prefix)}records`;
  db.exec(`CREATE TABLE IF NOT EXISTS ${table} (scope TEXT PRIMARY KEY, version INTEGER NOT NULL, due_at REAL, record TEXT NOT NULL);
    CREATE INDEX IF NOT EXISTS ${table}_due ON ${table} (due_at) WHERE due_at IS NOT NULL;`);
  const select = db.prepare(`SELECT record FROM ${table} WHERE scope = ?`);
  const insert = db.prepare(`INSERT INTO ${table} (scope, version, due_at, record) VALUES (?, ?, ?, ?) ON CONFLICT (scope) DO NOTHING`);
  const update = db.prepare(`UPDATE ${table} SET version = ?, due_at = ?, record = ? WHERE scope = ? AND version = ?`);
  const due = db.prepare(`SELECT scope FROM ${table} WHERE due_at <= ? ORDER BY due_at LIMIT ?`);
  const remove = db.prepare(`DELETE FROM ${table} WHERE scope = ?`);
  return {
    async get(scope) {
      const row = select.get(scope);
      return row === undefined ? null : parseRecord(column(row, 'record'), scope);
    },
    async put(record, expectedVersion) {
      const json = JSON.stringify(record);
      const { changes } =
        expectedVersion === 0
          ? insert.run(record.scope, record.version, record.dueAt, json)
          : update.run(record.version, record.dueAt, json, record.scope, expectedVersion);
      return Number(changes) === 1 ? 'ok' : 'conflict';
    },
    async due(now, limit) {
      return due.all(now, limit).map((row) => text(column(row, 'scope')));
    },
    async remove(scope) {
      remove.run(scope);
    },
  };
}

/**
 * Positions are the rowid, which SQLite assigns under its single write lock, so they follow
 * commit order. AUTOINCREMENT keeps a deleted tail's ids from being handed out again. `read`
 * cannot wait: with `blockMs` it returns at once and `logSource` paces the loop.
 */
export function sqliteLog(db: SqliteDatabase, options?: SqliteOptions): EventLog {
  const table = `${tablePrefix(options?.prefix)}log`;
  db.exec(`CREATE TABLE IF NOT EXISTS ${table} (position INTEGER PRIMARY KEY AUTOINCREMENT, scope TEXT NOT NULL, event TEXT NOT NULL);
    CREATE INDEX IF NOT EXISTS ${table}_scope ON ${table} (scope, position);`);
  const insert = db.prepare(`INSERT INTO ${table} (scope, event) VALUES (?, ?)`);
  const select = db.prepare(`SELECT position, event FROM ${table} WHERE scope = ? AND position > ? ORDER BY position LIMIT ?`);
  return {
    async append(scope, events) {
      // A savepoint, unlike BEGIN, nests inside a transaction the caller already holds on this handle.
      db.exec('SAVEPOINT attend_append');
      try {
        for (const event of events) insert.run(scope, JSON.stringify(event));
        db.exec('RELEASE attend_append');
      } catch (error) {
        db.exec('ROLLBACK TO attend_append; RELEASE attend_append');
        throw error;
      }
    },
    async read(scope, after, { limit }) {
      return select.all(scope, after === null ? 0 : Number(parsePosition(after)), limit).map(
        (row): LogEntry => ({ position: formatPosition(column(row, 'position')), event: parseEvent(column(row, 'event')) }),
      );
    },
  };
}
