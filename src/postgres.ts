/**
 * `attend/postgres`: a ScopeStore and an EventLog over a client with `query(text, values)`.
 * Run `migrate()` before either adapter. It calls the store migration, then creates the
 * separate EventLog table and index that `postgresLog` still needs.
 */
import { boundary } from './internal/adapter.js';
import type { EventLog, LogEntry } from './types.js';
import { migrate as migrateStore } from './store/postgres.js';
import type { PostgresClient } from './store/postgres.js';

export { postgresStore } from './store/postgres.js';
export type { PostgresClient, PostgresListener, PostgresStoreOptions } from './store/postgres.js';

const { column, formatPosition, parseEvent, parsePosition, tablePrefix } = boundary('attend/postgres');

export interface PostgresOptions {
  /**
   * Prepended to the table names. Lowercase letters, digits and underscores, since Postgres
   * folds unquoted names to lowercase and the append lock is keyed by the name as written.
   * Default 'attend_'. To put the tables in another schema, set the connection's `search_path`.
   */
  prefix?: string;
}

export async function migrate(client: PostgresClient, options?: PostgresOptions): Promise<void> {
  await migrateStore(client, options);
  const log = `${tablePrefix(options?.prefix, { lowercase: true })}log`;
  await client.query(`SELECT pg_advisory_xact_lock(hashtext('attend.migrate'));
    CREATE TABLE IF NOT EXISTS ${log} (position bigserial PRIMARY KEY, scope text NOT NULL, event json NOT NULL);
    CREATE INDEX IF NOT EXISTS ${log}_scope ON ${log} (scope, position);`);
}

/**
 * Positions come from one bigserial, so a reader could see position 11 commit before 10 and
 * skip 10 for good. Each append therefore takes a transaction-scoped advisory lock on its
 * scope before drawing any position, in the same statement: appends to one scope draw and
 * commit one at a time, and the lock is released only after the commit is visible.
 *
 * `read` with `blockMs` polls every `pollMs` (default 100) until an entry arrives or the time is up.
 */
export function postgresLog(client: PostgresClient, options?: PostgresOptions & { pollMs?: number }): EventLog {
  const log = `${tablePrefix(options?.prefix, { lowercase: true })}log`;
  const pollMs = options?.pollMs ?? 100;
  const page = async (scope: string, after: string | null, limit: number): Promise<LogEntry[]> => {
    const { rows } = await client.query(`SELECT position, event FROM ${log} WHERE scope = $1 AND position > $2 ORDER BY position LIMIT $3`, [
      scope,
      after === null ? '0' : parsePosition(after),
      limit,
    ]);
    return rows.map((row) => ({ position: formatPosition(column(row, 'position')), event: parseEvent(column(row, 'event')) }));
  };
  return {
    async append(scope, events) {
      if (events.length === 0) return;
      await client.query(
        `WITH turn AS (SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2)))
         INSERT INTO ${log} (scope, event)
         SELECT $2, e.event::json FROM turn, unnest($3::text[]) WITH ORDINALITY AS e(event, n) ORDER BY e.n`,
        [log, scope, events.map((event) => JSON.stringify(event))],
      );
    },
    async read(scope, after, { limit, blockMs }) {
      const deadline = Date.now() + (blockMs ?? 0);
      for (;;) {
        const entries = await page(scope, after, limit);
        const left = deadline - Date.now();
        if (entries.length > 0 || left <= 0) return entries;
        await new Promise((resolve) => setTimeout(resolve, Math.min(pollMs, left)));
      }
    },
  };
}
