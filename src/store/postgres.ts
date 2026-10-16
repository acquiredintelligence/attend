import { boundary } from '../internal/adapter.js';
import type { DecisionLog, DecisionRow, Retention, ScopeStore, ScopeWrite, Snapshot, StoredScope } from './types.js';

const { column, parseRecord, tablePrefix, text } = boundary('attend/postgres');

export interface PostgresClient {
  query(text: string, values?: unknown[]): Promise<{ rows: unknown[]; rowCount: number | null }>;
}

/** A dedicated connection for LISTEN, so subscribers in other processes wake on new decisions. */
export interface PostgresListener {
  listen(channel: string, onNotify: () => void): Promise<() => Promise<void>>;
}

export interface PostgresStoreOptions {
  prefix?: string;
  retain?: Retention;
  listener?: PostgresListener;
  /** Opt-in `SET LOCAL synchronous_commit = off` per group: can lose the last commits, never corrupts. */
  asyncCommit?: boolean;
}

/** Creates the scopes, inputs, snapshots, decisions and meta tables. Idempotent. Needs PostgreSQL 13 or later. */
export async function migrate(client: PostgresClient, options?: { prefix?: string }): Promise<void> {
  const p = tablePrefix(options?.prefix, { lowercase: true });
  // fillfactor 50 leaves room on every page of scope rows, so version bumps stay HOT once thousands of scopes fill the table.
  await client.query(`SELECT pg_advisory_xact_lock(hashtext('attend.migrate'));
    CREATE TABLE IF NOT EXISTS ${p}scopes (scope text COLLATE "C" PRIMARY KEY, version bigint NOT NULL, due double precision) WITH (fillfactor = 50);
    CREATE INDEX IF NOT EXISTS ${p}scopes_due ON ${p}scopes (due, scope) WHERE due IS NOT NULL;
    CREATE TABLE IF NOT EXISTS ${p}inputs (scope text COLLATE "C" NOT NULL, version bigint NOT NULL, input json NOT NULL, PRIMARY KEY (scope, version));
    CREATE TABLE IF NOT EXISTS ${p}snapshots (scope text COLLATE "C" PRIMARY KEY, version bigint NOT NULL, policy json NOT NULL, reducer int NOT NULL, record json NOT NULL);
    CREATE TABLE IF NOT EXISTS ${p}decisions (
      txid xid8 NOT NULL DEFAULT pg_current_xact_id(), n int NOT NULL, scope text COLLATE "C" NOT NULL,
      kind text NOT NULL, id text NOT NULL, route text, at double precision NOT NULL, body json NOT NULL,
      PRIMARY KEY (txid, n), UNIQUE (kind, id));
    CREATE INDEX IF NOT EXISTS ${p}decisions_scope ON ${p}decisions (scope, txid, n);
    CREATE INDEX IF NOT EXISTS ${p}decisions_at ON ${p}decisions (at);
    CREATE TABLE IF NOT EXISTS ${p}meta (key text PRIMARY KEY, value text NOT NULL);
    INSERT INTO ${p}meta (key, value) VALUES ('epoch', gen_random_uuid()::text) ON CONFLICT DO NOTHING;
    CREATE OR REPLACE FUNCTION ${p}decisions_notify() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF EXISTS (SELECT 1 FROM added) THEN PERFORM pg_notify('${p}decisions', ''); END IF;
      RETURN NULL;
    END $$;
    DO $$ BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = '${p}decisions'::regclass AND tgname = '${p}decisions_notify') THEN
        CREATE TRIGGER ${p}decisions_notify AFTER INSERT ON ${p}decisions REFERENCING NEW TABLE AS added
          FOR EACH STATEMENT EXECUTE FUNCTION ${p}decisions_notify();
      END IF;
    END $$;
    CREATE OR REPLACE FUNCTION ${p}remove(target text) RETURNS void LANGUAGE plpgsql AS $$
    BEGIN
      DELETE FROM ${p}scopes WHERE scope = target;
      DELETE FROM ${p}inputs WHERE scope = target;
      DELETE FROM ${p}snapshots WHERE scope = target;
    END $$;`);
}

export function postgresStore(client: PostgresClient, options?: PostgresStoreOptions): ScopeStore {
  const p = tablePrefix(options?.prefix, { lowercase: true });
  const horizon = 'h AS MATERIALIZED (SELECT pg_snapshot_xmin(pg_current_snapshot()) AS xid)';
  const position = "txid::text || '.' || n::text";
  const head = `SELECT ${position} FROM ${p}decisions, h WHERE txid < h.xid ORDER BY txid DESC, n DESC LIMIT 1`;
  let ownTxid: bigint | undefined;
  let visible = 0n;
  let epoch: Promise<string> | undefined;
  let closed = false;
  let listening: Promise<(() => Promise<void>) | undefined> | undefined;
  let retained: Promise<void> | undefined;
  let lastTrim = -Infinity;
  const wakeups = new Set<() => void>();
  const wake = () => { for (const notify of wakeups) notify(); };

  // The horizon is cluster-wide, so this store's newest rows can sit above it for a moment; waiting lets it read its own writes.
  async function settle(): Promise<void> {
    const target = ownTxid;
    if (target === undefined || target < visible) return;
    const deadline = Date.now() + 2000;
    for (let delay = 1; !closed; delay = Math.min(delay * 2, 50)) {
      const { rows } = await client.query('SELECT pg_snapshot_xmin(pg_current_snapshot())::text AS xid');
      const horizon = BigInt(text(column(rows[0], 'xid')));
      if (horizon > visible) visible = horizon;
      if (horizon > target) return;
      if (Date.now() >= deadline) {
        if (target >= visible) visible = target + 1n;
        return;
      }
      await new Promise<void>((resolve) => setTimeout(resolve, delay));
    }
  }

  function listen(): Promise<(() => Promise<void>) | undefined> {
    return listening ??= options?.listener?.listen(`${p}decisions`, wake).catch(() => undefined) ?? Promise.resolve(undefined);
  }

  const log: DecisionLog = {
    epoch() {
      return epoch ??= client.query(`SELECT value FROM ${p}meta WHERE key = 'epoch'`).then(
        ({ rows }) => text(column(rows[0], 'value')),
        (error: unknown) => { epoch = undefined; throw error; },
      );
    },
    compare,
    async bounds() {
      await settle();
      const { rows } = await client.query(`WITH ${horizon} SELECT
        (SELECT ${position} FROM ${p}decisions, h WHERE txid < h.xid ORDER BY txid, n LIMIT 1) AS first,
        (${head}) AS last`);
      return { first: nullableText(column(rows[0], 'first')), last: nullableText(column(rows[0], 'last')) };
    },
    async find(kind, id) {
      await settle();
      const { rows } = await client.query(`WITH ${horizon} SELECT d.*, ${position} AS position FROM ${p}decisions d, h
        WHERE kind = $1 AND id = $2 AND txid < h.xid`, [kind, id]);
      return rows.length === 0 ? null : decisionRow(rows[0]);
    },
    async read(query) {
      const after = query.after === null ? null : parsePosition(query.after);
      await settle();
      const values: unknown[] = [];
      const bind = (value: unknown) => { values.push(value); return `$${values.length}`; };
      const where = ['txid < h.xid'];
      if (after) where.push(`(txid, n) > (${bind(after.txid)}::xid8, ${bind(after.n)}::int)`);
      if (query.scope !== undefined) where.push(`scope = ${bind(query.scope)}`);
      if (query.scopePrefix !== undefined) where.push(`starts_with(scope, ${bind(query.scopePrefix)})`);
      const hits: string[] = [];
      if (query.kinds !== undefined) hits.push(`kind = ANY(${bind(query.kinds)}::text[])`);
      if (query.routes !== undefined) hits.push(`route = ANY(${bind(query.routes)}::text[])`);
      if (query.sinceAt !== undefined) hits.push(`at >= ${bind(query.sinceAt)}`);
      const scanLimit = query.scanLimit ?? 4 * query.limit;
      const candidates = bind(scanLimit + 1);
      const limit = bind(query.limit);
      const { rows } = await client.query(`WITH ${horizon}, candidates AS MATERIALIZED (
        SELECT d.*, COALESCE(${hits.join(' AND ') || 'true'}, false) AS hit FROM ${p}decisions d, h
        WHERE ${where.join(' AND ')} ORDER BY txid, n LIMIT ${candidates}
      ), ranked AS (
        SELECT *, count(*) FILTER (WHERE hit) OVER (ORDER BY txid, n) AS hits FROM candidates
      ), head AS (SELECT (${head}) AS head)
      SELECT head.head, ${position} AS position, scope, kind, id, route, at, hit,
        CASE WHEN hit AND hits <= ${limit} THEN body END AS body
      FROM head LEFT JOIN ranked ON true ORDER BY txid, n`, values);
      const seenHead = nullableText(column(rows[0], 'head'));
      const candidatesRows = rows.filter((row) => column(row, 'position') !== null);
      const result: DecisionRow[] = [];
      let examined = 0;
      let checked = query.after;
      for (const row of candidatesRows) {
        if (examined >= scanLimit || result.length >= query.limit) break;
        examined += 1;
        checked = text(column(row, 'position'));
        if (column(row, 'hit') === true) result.push(decisionRow(row));
      }
      const more = examined < candidatesRows.length;
      if (!more && seenHead !== null && (checked === null || compare(seenHead, checked) > 0)) checked = seenHead;
      return { rows: result, checked, more };
    },
    async wait(after, blockMs, signal) {
      if (signal.aborted || closed) return;
      let timer: ReturnType<typeof setTimeout> | undefined;
      let resume: (() => void) | undefined;
      let notified = false;
      const notify = () => { notified = true; resume?.(); };
      const abort = () => resume?.();
      wakeups.add(notify);
      signal.addEventListener('abort', abort);
      try {
        if (after !== null) parsePosition(after);
        const registration = listen();
        let subscribed = false;
        void registration.then((stop) => { subscribed = stop !== undefined; notify(); });
        const deadline = Date.now() + blockMs;
        let delay = 5;
        while (!closed && !signal.aborted) {
          notified = false;
          const { rows } = await client.query(`WITH ${horizon} SELECT (${head}) AS head,
            COALESCE((SELECT txid FROM ${p}decisions ORDER BY txid DESC, n DESC LIMIT 1) >= (SELECT xid FROM h), false) AS pending`);
          const last = nullableText(column(rows[0], 'head'));
          if (last !== null && (after === null || compare(last, after) > 0)) return;
          const left = deadline - Date.now();
          if (left <= 0 || closed || signal.aborted) return;
          if (notified) { delay = 5; continue; }
          const pending = column(rows[0], 'pending') === true;
          const poll = pending ? delay : subscribed ? left : 100;
          await new Promise<void>((resolve) => {
            resume = resolve;
            timer = setTimeout(resolve, Math.min(left, poll));
          });
          clearTimeout(timer);
          resume = undefined;
          delay = notified ? 5 : Math.min(delay * 2, 250);
        }
      } catch {
        return;
      } finally {
        clearTimeout(timer);
        wakeups.delete(notify);
        signal.removeEventListener('abort', abort);
      }
    },
    async trim(retention) {
      await settle();
      const values: unknown[] = [];
      const bind = (value: unknown) => { values.push(value); return `$${values.length}`; };
      const ctes = [];
      const floors = [];
      if (retention.maxRows !== undefined) {
        ctes.push(`count_floor AS (SELECT txid, n FROM visible ORDER BY txid DESC, n DESC OFFSET ${bind(Math.max(0, retention.maxRows - 1))} LIMIT 1)`);
        floors.push('SELECT * FROM count_floor', 'SELECT * FROM oldest WHERE NOT EXISTS (SELECT 1 FROM count_floor)');
      }
      if (retention.olderThanMs !== undefined) {
        ctes.push(`age_floor AS (SELECT txid, n FROM visible WHERE at >= ${bind(Date.now() - retention.olderThanMs)} ORDER BY txid, n LIMIT 1)`);
        floors.push('SELECT * FROM age_floor', 'SELECT * FROM head WHERE NOT EXISTS (SELECT 1 FROM age_floor)');
      }
      if (ctes.length === 0) return;
      await client.query(`WITH ${horizon}, visible AS NOT MATERIALIZED (
        SELECT txid, n, at FROM ${p}decisions, h WHERE txid < h.xid
      ), head AS (SELECT txid, n FROM visible ORDER BY txid DESC, n DESC LIMIT 1),
      oldest AS (SELECT txid, n FROM visible ORDER BY txid, n LIMIT 1),
      ${ctes.join(', ')}, floor AS (
        ${floors.join(' UNION ALL ')} ORDER BY txid DESC, n DESC LIMIT 1
      ) DELETE FROM ${p}decisions d USING head, floor
      WHERE (d.txid, d.n) < (head.txid, head.n) AND (d.txid, d.n) < (floor.txid, floor.n)`, values);
    },
  };

  return {
    durable: true,
    log,
    async load(scope) {
      const { rows } = await client.query(`SELECT s.version, sn.version AS snapshot_version, sn.policy, sn.reducer, sn.record,
        (SELECT json_agg(input ORDER BY version) FROM ${p}inputs WHERE scope = s.scope AND version > COALESCE(sn.version, 0)) AS tail
        FROM ${p}scopes s LEFT JOIN ${p}snapshots sn USING (scope) WHERE s.scope = $1`, [scope]);
      if (rows.length === 0) return null;
      const row = rows[0];
      return {
        version: number(column(row, 'version')),
        snapshot: column(row, 'snapshot_version') === null ? null : {
          version: number(column(row, 'snapshot_version')),
          policy: json(column(row, 'policy')) as Snapshot['policy'],
          reducer: number(column(row, 'reducer')),
          record: parseRecord(column(row, 'record'), scope) as Snapshot['record'],
        },
        tail: column(row, 'tail') === null ? [] : json(column(row, 'tail')) as StoredScope['tail'],
      };
    },
    async commit(writes) {
      if (new Set(writes.map((write) => write.scope)).size !== writes.length) throw new Error('attend/postgres: duplicate scope in group');
      if (writes.length === 0) return [];
      const sorted = [...writes].sort((a, b) => a.scope < b.scope ? -1 : a.scope > b.scope ? 1 : 0);
      const { sql, values, appends } = commitStatement(p, sorted, writes, options?.asyncCommit === true);
      const { rows } = await client.query(sql, values);
      const ok = new Set(rows.map((row) => text(column(row, 'scope'))));
      const txid = appends && rows.length > 0 ? nullableText(column(rows[0], 'txid')) : null;
      if (txid !== null) {
        const committed = BigInt(txid);
        if (ownTxid === undefined || committed > ownTxid) ownTxid = committed;
        if (options?.retain && !closed && !retained && Date.now() - lastTrim >= 60_000) {
          lastTrim = Date.now();
          // A failed background trim is retried by the next eligible commit.
          retained = log.trim(options.retain).catch(() => {}).finally(() => { retained = undefined; });
        }
      }
      return writes.map((write) => ok.has(write.scope) ? 'ok' : 'conflict');
    },
    async due(now, limit, after) {
      const values: unknown[] = [now, limit];
      if (after) values.push(after.due, after.scope);
      const { rows } = await client.query(`SELECT due, scope FROM ${p}scopes WHERE due <= $1
        ${after ? 'AND (due, scope) > ($3, $4)' : ''} ORDER BY due, scope LIMIT $2`, values);
      return rows.map((row) => ({ due: number(column(row, 'due')), scope: text(column(row, 'scope')) }));
    },
    async remove(scope) {
      await client.query(`SELECT ${p}remove($1)`, [scope]);
    },
    async close() {
      closed = true;
      wake();
      const stop = await listening;
      await stop?.();
      await retained;
    },
  };
}

function commitStatement(p: string, sorted: readonly ScopeWrite[], writes: readonly ScopeWrite[], asyncCommit: boolean) {
  const values: unknown[] = [];
  const bind = (value: unknown) => { values.push(value); return `$${values.length}`; };
  const pieces = (serialized: string[]) => bind(`[${serialized.join(',')}]`);
  const scopes = bind(sorted.map((w) => w.scope));
  const expected = bind(sorted.map((w) => w.expectedVersion));
  const versions = bind(sorted.map((w) => w.expectedVersion + w.inputs.length));
  const due = bind(sorted.map((w) => w.due ?? null));
  const setDue = bind(sorted.map((w) => w.due !== undefined));
  const ctes = asyncCommit ? ["cfg AS MATERIALIZED (SELECT set_config('synchronous_commit', 'off', true))"] : [];
  ctes.push(`w AS (SELECT w.* FROM unnest(${scopes}::text[], ${expected}::bigint[], ${versions}::bigint[], ${due}::float8[], ${setDue}::bool[]) w(scope, expected, version, due, set_due) ${asyncCommit ? 'CROSS JOIN cfg' : ''})`,
    `lk AS MATERIALIZED (SELECT scope FROM ${p}scopes WHERE scope = ANY(${scopes}::text[]) ORDER BY scope FOR NO KEY UPDATE)`,
    `cas AS (UPDATE ${p}scopes s SET version = w.version, due = CASE WHEN w.set_due THEN w.due ELSE s.due END
      FROM w JOIN lk USING (scope) WHERE s.scope = w.scope AND s.version = w.expected AND w.expected > 0 RETURNING s.scope)`,
    `ins AS (INSERT INTO ${p}scopes (scope, version, due) SELECT scope, version, due FROM w WHERE expected = 0
      ORDER BY scope COLLATE "C" ON CONFLICT (scope) DO NOTHING RETURNING scope)`,
    'ok AS (SELECT scope FROM cas UNION ALL SELECT scope FROM ins)');
  const inputs = sorted.flatMap((w) => w.snapshot === null ? w.inputs.map((input, i) => ({ scope: w.scope, version: w.expectedVersion + i + 1, json: w.serialized?.inputs[i] ?? JSON.stringify(input) })) : []);
  if (inputs.length > 0) ctes.push(`inp AS (INSERT INTO ${p}inputs (scope, version, input)
    SELECT i.scope, i.version, j.value FROM unnest(${bind(inputs.map((i) => i.scope))}::text[], ${bind(inputs.map((i) => i.version))}::bigint[]) WITH ORDINALITY i(scope, version, ord)
    JOIN json_array_elements(${pieces(inputs.map((i) => i.json))}::json) WITH ORDINALITY j(value, ord) USING (ord) JOIN ok USING (scope))`);
  const snapshots = sorted.flatMap((w) => w.snapshot === null ? [] : [{ scope: w.scope, version: w.snapshot.version, snapshot: w.snapshot, record: w.serialized?.snapshot ?? JSON.stringify(w.snapshot.record) }]);
  if (snapshots.length > 0) {
    const snapshotScopes = bind(snapshots.map((s) => s.scope));
    const snapshotVersions = bind(snapshots.map((s) => s.version));
    ctes.push(`snp AS (INSERT INTO ${p}snapshots (scope, version, policy, reducer, record)
      SELECT s.scope, s.version, policy.value, s.reducer, record.value
      FROM unnest(${snapshotScopes}::text[], ${snapshotVersions}::bigint[], ${bind(snapshots.map((s) => s.snapshot.reducer))}::int[]) WITH ORDINALITY s(scope, version, reducer, ord)
      JOIN json_array_elements(${pieces(snapshots.map((s) => JSON.stringify(s.snapshot.policy)))}::json) WITH ORDINALITY policy(value, ord) USING (ord)
      JOIN json_array_elements(${pieces(snapshots.map((s) => s.record))}::json) WITH ORDINALITY record(value, ord) USING (ord) JOIN ok USING (scope)
      ON CONFLICT (scope) DO UPDATE SET version = EXCLUDED.version, policy = EXCLUDED.policy, reducer = EXCLUDED.reducer, record = EXCLUDED.record)`,
    `trm AS (DELETE FROM ${p}inputs d USING unnest(${snapshotScopes}::text[], ${snapshotVersions}::bigint[]) t(scope, version) JOIN ok USING (scope)
      WHERE d.scope = t.scope AND d.version <= t.version)`);
  }
  const appends = writes.flatMap((w) => w.appends.map((row, i) => ({ row, scope: w.scope, json: w.serialized?.appends[i] ?? JSON.stringify(row.body) })));
  if (appends.length > 0) ctes.push(`dec AS (INSERT INTO ${p}decisions (n, scope, kind, id, route, at, body)
    SELECT r.ord::int, r.scope, r.kind, r.id, r.route, r.at, j.value
    FROM unnest(${bind(appends.map((a) => a.scope))}::text[], ${bind(appends.map((a) => a.row.kind))}::text[], ${bind(appends.map((a) => a.row.id))}::text[], ${bind(appends.map((a) => a.row.route))}::text[], ${bind(appends.map((a) => a.row.at))}::float8[]) WITH ORDINALITY r(scope, kind, id, route, at, ord)
    JOIN json_array_elements(${pieces(appends.map((a) => a.json))}::json) WITH ORDINALITY j(value, ord) USING (ord) JOIN ok USING (scope)
    ORDER BY r.ord ON CONFLICT (kind, id) DO NOTHING RETURNING txid)`);
  return { sql: `WITH ${ctes.join(',\n')} SELECT scope${appends.length > 0 ? ', (SELECT txid::text FROM dec LIMIT 1) AS txid' : ''} FROM ok`, values, appends: appends.length > 0 };
}

function parsePosition(value: string): { txid: string; n: string } {
  const match = /^(\d{1,20})\.(\d{1,10})$/.exec(value);
  if (!match?.[1] || !match[2]) throw new Error(`attend/postgres: not a position from this log: "${value}"`);
  return { txid: match[1], n: match[2] };
}

function compare(a: string, b: string): number {
  const left = parsePosition(a);
  const right = parsePosition(b);
  const x = BigInt(left.txid);
  const y = BigInt(right.txid);
  return x < y ? -1 : x > y ? 1 : Number(left.n) - Number(right.n);
}

function nullableText(value: unknown): string | null {
  return value === null ? null : text(value);
}

function number(value: unknown): number {
  const result = typeof value === 'number' ? value : Number(text(value));
  if (!Number.isFinite(result)) throw new Error('attend/postgres: expected a finite number');
  return result;
}

function json(value: unknown): unknown {
  return typeof value === 'string' || value instanceof Uint8Array ? JSON.parse(text(value)) : value;
}

function decisionRow(row: unknown): DecisionRow {
  const kind = text(column(row, 'kind'));
  const route = nullableText(column(row, 'route'));
  if (route !== null && route !== 'interrupt' && route !== 'queue' && route !== 'note' && route !== 'ask') throw new Error('attend/postgres: invalid route');
  const common = { position: text(column(row, 'position')), id: text(column(row, 'id')), scope: text(column(row, 'scope')), at: number(column(row, 'at')) };
  const body = json(column(row, 'body'));
  if (typeof body !== 'object' || body === null) throw new Error('attend/postgres: invalid decision body');
  if (kind === 'decision' && route !== null) return { ...common, kind, route, body: body as Extract<DecisionRow, { kind: 'decision' }>['body'] };
  if (kind === 'deadLetter') return { ...common, kind, route, body: body as Extract<DecisionRow, { kind: 'deadLetter' }>['body'] };
  throw new Error('attend/postgres: invalid decision kind');
}
