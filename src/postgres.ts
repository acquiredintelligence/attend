/**
 * `attend/postgres`: a `StateStore` and an `EventLog` over Postgres. Takes any client with
 * `query(text, values)`, such as a `pg` `Pool` or `Client`, so importing this module loads no
 * driver. Run `migrate()` once before using either adapter; neither runs DDL itself.
 */
import type { Event, EventLog, LogEntry, ScopeRecord, StateStore } from './types.js';

export interface PostgresClient {
  query(text: string, values?: unknown[]): Promise<{ rows: unknown[]; rowCount: number | null }>;
}

export interface PostgresOptions {
  /**
   * Prepended to the table names. Lowercase letters, digits and underscores, since Postgres
   * folds unquoted names to lowercase and the append lock is keyed by the name as written.
   * Default 'attend_'. To put the tables in another schema, set the connection's `search_path`.
   */
  prefix?: string;
}

/**
 * Creates the record and log tables and their indexes if they are absent. Idempotent and safe
 * to run from several processes at once. Run it at deploy or startup with a role that may
 * create tables; the adapters need only SELECT, INSERT, UPDATE and DELETE.
 *
 * Records and events are `json`, not `jsonb`: jsonb rejects the escape `\u0000`, so one event
 * whose text held a NUL would make every later put for its scope fail. `due_at` is a double so
 * a fractional `dueAt` is stored exactly.
 */
export async function migrate(client: PostgresClient, options?: PostgresOptions): Promise<void> {
  const { records, log } = tables(options);
  // One simple query runs as one transaction, so the lock serialises concurrent migrations; IF NOT EXISTS alone races on the catalog.
  await client.query(`SELECT pg_advisory_xact_lock(hashtext('attend.migrate'));
    CREATE TABLE IF NOT EXISTS ${records} (scope text PRIMARY KEY, version bigint NOT NULL, due_at double precision, record json NOT NULL);
    CREATE INDEX IF NOT EXISTS ${records}_due ON ${records} (due_at) WHERE due_at IS NOT NULL;
    CREATE TABLE IF NOT EXISTS ${log} (position bigserial PRIMARY KEY, scope text NOT NULL, event json NOT NULL);
    CREATE INDEX IF NOT EXISTS ${log}_scope ON ${log} (scope, position);`);
}

export function postgresStore(client: PostgresClient, options?: PostgresOptions): StateStore {
  const { records } = tables(options);
  return {
    async get(scope) {
      const { rows } = await client.query(`SELECT record FROM ${records} WHERE scope = $1`, [scope]);
      return rows.length === 0 ? null : parseRecord(column(rows[0], 'record'), scope);
    },
    async put(record, expectedVersion) {
      const json = JSON.stringify(record);
      const { rowCount } =
        expectedVersion === 0
          ? await client.query(`INSERT INTO ${records} (scope, version, due_at, record) VALUES ($1, $2, $3, $4) ON CONFLICT (scope) DO NOTHING`, [record.scope, record.version, record.dueAt, json])
          : await client.query(`UPDATE ${records} SET version = $2, due_at = $3, record = $4 WHERE scope = $1 AND version = $5`, [record.scope, record.version, record.dueAt, json, expectedVersion]);
      return rowCount === 1 ? 'ok' : 'conflict';
    },
    async due(now, limit) {
      const { rows } = await client.query(`SELECT scope FROM ${records} WHERE due_at <= $1 ORDER BY due_at LIMIT $2`, [now, limit]);
      return rows.map((row) => text(column(row, 'scope')));
    },
    async remove(scope) {
      await client.query(`DELETE FROM ${records} WHERE scope = $1`, [scope]);
    },
  };
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
  const { log } = tables(options);
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

function tables(options: PostgresOptions | undefined): { records: string; log: string } {
  const prefix = options?.prefix ?? 'attend_';
  if (!/^[a-z_][a-z0-9_]*$/.test(prefix)) throw new Error(`attend/postgres: prefix must be lowercase letters, digits and underscores, got "${prefix}"`);
  return { records: `${prefix}records`, log: `${prefix}log` };
}

function column(row: unknown, name: string): unknown {
  if (typeof row !== 'object' || row === null) throw new Error(`attend/postgres: expected a row, got ${typeof row}`);
  return Reflect.get(row, name);
}

function text(value: unknown): string {
  if (typeof value !== 'string') throw new Error(`attend/postgres: expected text, got ${typeof value}`);
  return value;
}

/** `pg` hands json columns back parsed; a driver that does not hands back text. */
function json(value: unknown): unknown {
  return typeof value === 'string' ? JSON.parse(value) : value;
}

/** Twenty digits, so positions compare as strings the way they compare as numbers. */
function formatPosition(value: unknown): string {
  const digits = typeof value === 'number' || typeof value === 'bigint' ? String(value) : text(value);
  if (!/^\d{1,20}$/.test(digits)) throw new Error(`attend/postgres: expected an integer position, got "${digits}"`);
  return digits.padStart(20, '0');
}

function parsePosition(position: string): string {
  if (!/^\d{1,20}$/.test(position)) throw new Error(`attend/postgres: not a position from this log: "${position}"`);
  return position;
}

/** The adapter wrote this JSON from a `ScopeRecord`; past the identity checks, the shape is its own. */
function parseRecord(value: unknown, scope: string): ScopeRecord {
  const parsed = json(value);
  if (typeof parsed !== 'object' || parsed === null || Reflect.get(parsed, 'scope') !== scope || typeof Reflect.get(parsed, 'version') !== 'number') {
    throw new Error(`attend/postgres: corrupt record for scope "${scope}"`);
  }
  return parsed as ScopeRecord;
}

function parseEvent(value: unknown): Event {
  const parsed = json(value);
  const field = (name: string): unknown => (typeof parsed === 'object' && parsed !== null ? Reflect.get(parsed, name) : undefined);
  const data = field('data');
  if (typeof field('id') !== 'string' || typeof field('name') !== 'string' || typeof field('at') !== 'number' || typeof data !== 'object' || data === null) {
    throw new Error('attend/postgres: corrupt log entry');
  }
  return parsed as Event;
}
