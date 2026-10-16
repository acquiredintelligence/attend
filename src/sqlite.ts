/**
 * `attend/sqlite`: a ScopeStore and an EventLog over SQLite, without loading a driver.
 * The store supports a synchronous database handle or a file owned by a worker thread.
 * Both adapters create their tables and indexes when needed.
 */
import { boundary } from './internal/adapter.js';
import type { EventLog, LogEntry } from './types.js';
import type { SqliteDatabase } from './store/sqlite.js';

export { sqliteStore } from './store/sqlite.js';
export type { SqliteStoreOptions, SqliteDatabase, SqliteStatement, SqliteValue } from './store/sqlite.js';

const { column, formatPosition, parseEvent, parsePosition, tablePrefix } = boundary('attend/sqlite');

export interface SqliteOptions {
  prefix?: string;
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
