"""``attend.store.postgres``: a ScopeStore over PostgreSQL 13 or later, on asyncpg.

A port of ``src/store/postgres.ts`` with the same tables, columns, statements and JSON, so a Python
process and a TypeScript process can share one database. One statement commits a group of scopes'
writes together; decisions are ordered by ``(txid, n)`` with an ``xid8`` horizon, so a reader never
sees a row appear below a position it has read. Run ``migrate`` at deploy with a role that can
create tables; the store needs only SELECT, INSERT, UPDATE and DELETE.

asyncpg was chosen over psycopg 3 for its asyncio-native pool, its binary protocol and prepared
statement cache (the group commit is one large statement), and ``add_listener`` for LISTEN.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import math
import re
import time
from collections.abc import Callable, Sequence
from typing import Any, Protocol, cast

from .._json import stringify
from ..signal import AbortSignal
from .types import (
    CommitOutcome,
    DecisionKind,
    DecisionLog,
    DecisionPage,
    DecisionQuery,
    DecisionRow,
    DueKey,
    LogBounds,
    Retention,
    ScopeStore,
    ScopeWrite,
    Snapshot,
    StoredScope,
)

_PREFIX = re.compile(r"[a-z_][a-z0-9_]*")
_POSITION = re.compile(r"([0-9]{1,20})\.([0-9]{1,10})")


class PostgresClient(Protocol):
    """What the store needs from an ``asyncpg.Pool`` (or a connection): ``fetch`` and ``execute``."""

    async def fetch(self, query: str, *args: Any) -> list[Any]: ...

    async def execute(self, query: str, *args: Any) -> str: ...


class PostgresListener(Protocol):
    """A dedicated ``asyncpg.Connection``, not a pooled one, since LISTEN belongs to the session."""

    async def add_listener(self, channel: str, callback: Callable[..., Any]) -> None: ...

    async def remove_listener(self, channel: str, callback: Callable[..., Any]) -> None: ...


def _table_prefix(prefix: str) -> str:
    if not _PREFIX.fullmatch(prefix):
        raise ValueError(f'attend/postgres: prefix must be lowercase letters, digits and underscores, got "{prefix}"')
    return prefix


async def migrate(client: PostgresClient, *, prefix: str = "attend_") -> None:
    """Creates the scopes, inputs, snapshots, decisions and meta tables. Idempotent. Needs PostgreSQL 13 or later."""
    p = _table_prefix(prefix)
    # fillfactor 50 leaves room on every page of scope rows, so version bumps stay HOT once thousands of scopes fill the table.
    await client.execute(f"""SELECT pg_advisory_xact_lock(hashtext('attend.migrate'));
    CREATE TABLE IF NOT EXISTS {p}scopes (scope text COLLATE "C" PRIMARY KEY, version bigint NOT NULL, due double precision) WITH (fillfactor = 50);
    CREATE INDEX IF NOT EXISTS {p}scopes_due ON {p}scopes (due, scope) WHERE due IS NOT NULL;
    CREATE TABLE IF NOT EXISTS {p}inputs (scope text COLLATE "C" NOT NULL, version bigint NOT NULL, input json NOT NULL, PRIMARY KEY (scope, version));
    CREATE TABLE IF NOT EXISTS {p}snapshots (scope text COLLATE "C" PRIMARY KEY, version bigint NOT NULL, policy json NOT NULL, reducer int NOT NULL, record json NOT NULL);
    CREATE TABLE IF NOT EXISTS {p}decisions (
      txid xid8 NOT NULL DEFAULT pg_current_xact_id(), n int NOT NULL, scope text COLLATE "C" NOT NULL,
      kind text NOT NULL, id text NOT NULL, route text, at double precision NOT NULL, body json NOT NULL,
      PRIMARY KEY (txid, n), UNIQUE (kind, id));
    CREATE INDEX IF NOT EXISTS {p}decisions_scope ON {p}decisions (scope, txid, n);
    CREATE INDEX IF NOT EXISTS {p}decisions_at ON {p}decisions (at);
    CREATE TABLE IF NOT EXISTS {p}meta (key text PRIMARY KEY, value text NOT NULL);
    INSERT INTO {p}meta (key, value) VALUES ('epoch', gen_random_uuid()::text) ON CONFLICT DO NOTHING;
    CREATE OR REPLACE FUNCTION {p}decisions_notify() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF EXISTS (SELECT 1 FROM added) THEN PERFORM pg_notify('{p}decisions', ''); END IF;
      RETURN NULL;
    END $$;
    DO $$ BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = '{p}decisions'::regclass AND tgname = '{p}decisions_notify') THEN
        CREATE TRIGGER {p}decisions_notify AFTER INSERT ON {p}decisions REFERENCING NEW TABLE AS added
          FOR EACH STATEMENT EXECUTE FUNCTION {p}decisions_notify();
      END IF;
    END $$;
    CREATE OR REPLACE FUNCTION {p}remove(target text) RETURNS void LANGUAGE plpgsql AS $$
    BEGIN
      DELETE FROM {p}scopes WHERE scope = target;
      DELETE FROM {p}inputs WHERE scope = target;
      DELETE FROM {p}snapshots WHERE scope = target;
    END $$;""")


def _parse_position(value: str) -> tuple[str, int]:
    match = _POSITION.fullmatch(value)
    if match is None:
        raise ValueError(f'attend/postgres: not a position from this log: "{value}"')
    return match.group(1), int(match.group(2))


def _compare(a: str, b: str) -> int:
    left, right = _parse_position(a), _parse_position(b)
    x, y = int(left[0]), int(right[0])
    return -1 if x < y else 1 if x > y else left[1] - right[1]


def _number(value: object) -> float:
    if isinstance(value, bool):
        raise ValueError("attend/postgres: expected a finite number")
    result = value if isinstance(value, (int, float)) else float(str(value))
    if not math.isfinite(result):
        raise ValueError("attend/postgres: expected a finite number")
    return int(result) if isinstance(result, float) and result.is_integer() else result


def _json(value: object) -> Any:
    if isinstance(value, (str, bytes, bytearray)):
        return json.loads(value)
    return value


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _decision_row(row: Any) -> DecisionRow:
    kind = row["kind"]
    route = row["route"]
    if route is not None and route not in ("interrupt", "queue", "note", "ask"):
        raise ValueError("attend/postgres: invalid route")
    body = _json(row["body"])
    if not isinstance(body, dict):
        raise ValueError("attend/postgres: invalid decision body")
    if (kind == "decision" and route is not None) or kind == "deadLetter":
        return cast(DecisionRow, {"position": row["position"], "id": row["id"], "scope": row["scope"], "at": _number(row["at"]), "kind": kind, "route": route, "body": body})
    raise ValueError("attend/postgres: invalid decision kind")


class _Binder:
    def __init__(self) -> None:
        self.values: list[Any] = []

    def __call__(self, value: Any) -> str:
        self.values.append(value)
        return f"${len(self.values)}"


def _commit_statement(p: str, ordered: Sequence[ScopeWrite], writes: Sequence[ScopeWrite], async_commit: bool) -> tuple[str, list[Any], bool]:
    bind = _Binder()

    def pieces(serialized: list[str]) -> str:
        return bind("[" + ",".join(serialized) + "]")

    scopes = bind([w["scope"] for w in ordered])
    expected = bind([w["expectedVersion"] for w in ordered])
    versions = bind([w["expectedVersion"] + len(w["inputs"]) for w in ordered])
    due = bind([None if w.get("due") is None else float(cast(float, w.get("due"))) for w in ordered])
    set_due = bind(["due" in w for w in ordered])
    ctes = ["cfg AS MATERIALIZED (SELECT set_config('synchronous_commit', 'off', true))"] if async_commit else []
    ctes.append(
        f"w AS (SELECT w.* FROM unnest({scopes}::text[], {expected}::bigint[], {versions}::bigint[], {due}::float8[], {set_due}::bool[]) w(scope, expected, version, due, set_due) {'CROSS JOIN cfg' if async_commit else ''})"
    )
    ctes.append(f"lk AS MATERIALIZED (SELECT scope FROM {p}scopes WHERE scope = ANY({scopes}::text[]) ORDER BY scope FOR NO KEY UPDATE)")
    ctes.append(f"""cas AS (UPDATE {p}scopes s SET version = w.version, due = CASE WHEN w.set_due THEN w.due ELSE s.due END
      FROM w JOIN lk USING (scope) WHERE s.scope = w.scope AND s.version = w.expected AND w.expected > 0 RETURNING s.scope)""")
    ctes.append(f"""ins AS (INSERT INTO {p}scopes (scope, version, due) SELECT scope, version, due FROM w WHERE expected = 0
      ORDER BY scope COLLATE "C" ON CONFLICT (scope) DO NOTHING RETURNING scope)""")
    ctes.append("ok AS (SELECT scope FROM cas UNION ALL SELECT scope FROM ins)")
    inputs: list[tuple[str, int, str]] = []
    for w in ordered:
        if w["snapshot"] is None:
            serialized = w.get("serialized")
            for i, logged in enumerate(w["inputs"]):
                inputs.append((w["scope"], w["expectedVersion"] + i + 1, serialized["inputs"][i] if serialized is not None else stringify(logged)))
    if inputs:
        ctes.append(f"""inp AS (INSERT INTO {p}inputs (scope, version, input)
    SELECT i.scope, i.version, j.value FROM unnest({bind([i[0] for i in inputs])}::text[], {bind([i[1] for i in inputs])}::bigint[]) WITH ORDINALITY i(scope, version, ord)
    JOIN json_array_elements({pieces([i[2] for i in inputs])}::json) WITH ORDINALITY j(value, ord) USING (ord) JOIN ok USING (scope))""")
    snapshots: list[tuple[str, Snapshot, str]] = []
    for w in ordered:
        snapshot = w["snapshot"]
        if snapshot is not None:
            serialized = w.get("serialized")
            record = serialized["snapshot"] if serialized is not None and serialized["snapshot"] is not None else stringify(snapshot["record"])
            snapshots.append((w["scope"], snapshot, record))
    if snapshots:
        snapshot_scopes = bind([s[0] for s in snapshots])
        snapshot_versions = bind([s[1]["version"] for s in snapshots])
        ctes.append(f"""snp AS (INSERT INTO {p}snapshots (scope, version, policy, reducer, record)
      SELECT s.scope, s.version, policy.value, s.reducer, record.value
      FROM unnest({snapshot_scopes}::text[], {snapshot_versions}::bigint[], {bind([s[1]["reducer"] for s in snapshots])}::int[]) WITH ORDINALITY s(scope, version, reducer, ord)
      JOIN json_array_elements({pieces([stringify(s[1]["policy"]) for s in snapshots])}::json) WITH ORDINALITY policy(value, ord) USING (ord)
      JOIN json_array_elements({pieces([s[2] for s in snapshots])}::json) WITH ORDINALITY record(value, ord) USING (ord) JOIN ok USING (scope)
      ON CONFLICT (scope) DO UPDATE SET version = EXCLUDED.version, policy = EXCLUDED.policy, reducer = EXCLUDED.reducer, record = EXCLUDED.record)""")
        ctes.append(f"""trm AS (DELETE FROM {p}inputs d USING unnest({snapshot_scopes}::text[], {snapshot_versions}::bigint[]) t(scope, version) JOIN ok USING (scope)
      WHERE d.scope = t.scope AND d.version <= t.version)""")
    appends: list[tuple[str, str, str, str | None, float, str]] = []
    for w in writes:
        serialized = w.get("serialized")
        for i, row in enumerate(w["appends"]):
            appends.append((w["scope"], row["kind"], row["id"], row["route"], float(row["at"]), serialized["appends"][i] if serialized is not None else stringify(row["body"])))
    if appends:
        ctes.append(f"""dec AS (INSERT INTO {p}decisions (n, scope, kind, id, route, at, body)
    SELECT r.ord::int, r.scope, r.kind, r.id, r.route, r.at, j.value
    FROM unnest({bind([a[0] for a in appends])}::text[], {bind([a[1] for a in appends])}::text[], {bind([a[2] for a in appends])}::text[], {bind([a[3] for a in appends])}::text[], {bind([a[4] for a in appends])}::float8[]) WITH ORDINALITY r(scope, kind, id, route, at, ord)
    JOIN json_array_elements({pieces([a[5] for a in appends])}::json) WITH ORDINALITY j(value, ord) USING (ord) JOIN ok USING (scope)
    ORDER BY r.ord ON CONFLICT (kind, id) DO NOTHING RETURNING txid)""")
    tail = ", (SELECT txid::text FROM dec LIMIT 1) AS txid" if appends else ""
    return f"WITH {',\n'.join(ctes)} SELECT scope{tail} FROM ok", bind.values, bool(appends)


class PostgresStore(ScopeStore):
    durable = True

    def __init__(
        self, client: PostgresClient, *, prefix: str = "attend_", retain: Retention | None = None, listener: PostgresListener | None = None, async_commit: bool = False
    ) -> None:
        p = _table_prefix(prefix)
        self.client = client
        self.prefix = p
        self._retain = retain
        self._listener = listener
        self._async_commit = async_commit
        self.sql_horizon = "h AS MATERIALIZED (SELECT pg_snapshot_xmin(pg_current_snapshot()) AS xid)"
        self.sql_position = "txid::text || '.' || n::text"
        self.sql_head = f"SELECT {self.sql_position} FROM {p}decisions, h WHERE txid < (SELECT xid FROM h) ORDER BY txid DESC, n DESC LIMIT 1"
        self._own_txid: int | None = None
        self._visible = 0
        self.epoch_future: asyncio.Future[str] | None = None
        self.closed = False
        self._listening: asyncio.Future[bool] | None = None
        self._retained: asyncio.Task[None] | None = None
        self._last_trim = -math.inf
        self.wakeups: set[Callable[[], None]] = set()
        self.log = _PostgresLog(self)

    def _wake(self, *_: Any) -> None:
        for notify in list(self.wakeups):
            notify()

    async def settle(self) -> None:
        """The horizon is cluster-wide, so this store's newest rows can sit above it for a moment; waiting lets it read its own writes."""
        target = self._own_txid
        if target is None or target < self._visible:
            return
        deadline = time.monotonic() + 2
        delay = 0.001
        while not self.closed:
            rows = await self.client.fetch("SELECT pg_snapshot_xmin(pg_current_snapshot())::text AS xid")
            horizon = int(rows[0]["xid"])
            self._visible = max(self._visible, horizon)
            if horizon > target:
                return
            if time.monotonic() >= deadline:
                if target >= self._visible:
                    self._visible = target + 1
                return
            await asyncio.sleep(delay)
            delay = min(delay * 2, 0.05)

    def listen(self) -> asyncio.Future[bool]:
        if self._listening is None:

            async def listen() -> bool:
                if self._listener is None:
                    return False
                try:
                    await self._listener.add_listener(f"{self.prefix}decisions", self._wake)
                    return True
                except Exception:
                    return False

            self._listening = asyncio.ensure_future(listen())
        return self._listening

    async def load(self, scope: str) -> StoredScope | None:
        p = self.prefix
        rows = await self.client.fetch(
            f"""SELECT s.version, sn.version AS snapshot_version, sn.policy, sn.reducer, sn.record,
        (SELECT json_agg(input ORDER BY version) FROM {p}inputs WHERE scope = s.scope AND version > COALESCE(sn.version, 0)) AS tail
        FROM {p}scopes s LEFT JOIN {p}snapshots sn USING (scope) WHERE s.scope = $1""",
            scope,
        )
        if not rows:
            return None
        row = rows[0]
        snapshot = None
        if row["snapshot_version"] is not None:
            record: Any = _json(row["record"])
            if not isinstance(record, dict) or cast(dict[str, Any], record).get("scope") != scope or not isinstance(cast(dict[str, Any], record).get("version"), (int, float)):
                raise ValueError(f'attend/postgres: corrupt record for scope "{scope}"')
            snapshot = cast(Snapshot, {"version": int(row["snapshot_version"]), "policy": _json(row["policy"]), "reducer": int(row["reducer"]), "record": record})
        tail = row["tail"]
        return {"version": int(row["version"]), "snapshot": snapshot, "tail": [] if tail is None else _json(tail)}

    async def commit(self, writes: Sequence[ScopeWrite]) -> list[CommitOutcome]:
        if len({w["scope"] for w in writes}) != len(writes):
            raise ValueError("attend/postgres: duplicate scope in group")
        if not writes:
            return []
        ordered = sorted(writes, key=lambda w: w["scope"].encode("utf-16-be", "surrogatepass"))
        sql, values, appends = _commit_statement(self.prefix, ordered, writes, self._async_commit)
        rows = await self.client.fetch(sql, *values)
        ok = {row["scope"] for row in rows}
        txid = rows[0]["txid"] if appends and rows else None
        if txid is not None:
            committed = int(txid)
            if self._own_txid is None or committed > self._own_txid:
                self._own_txid = committed
            if self._retain and not self.closed and self._retained is None and time.monotonic() * 1000 - self._last_trim >= 60_000:
                self._last_trim = time.monotonic() * 1000
                retain = self._retain

                async def trim() -> None:
                    # A failed background trim is retried by the next eligible commit.
                    try:
                        await self.log.trim(retain)
                    except Exception:
                        pass
                    finally:
                        self._retained = None

                self._retained = asyncio.ensure_future(trim())
        return ["ok" if w["scope"] in ok else "conflict" for w in writes]

    async def due(self, now: float, limit: int, after: DueKey | None = None) -> list[DueKey]:
        values: list[Any] = [float(now), limit]
        if after is not None:
            values.extend([float(after["due"]), after["scope"]])
        rows = await self.client.fetch(
            f"SELECT due, scope FROM {self.prefix}scopes WHERE due <= $1::float8 {'AND (due, scope) > ($3::float8, $4::text)' if after is not None else ''} ORDER BY due, scope LIMIT $2::bigint",
            *values,
        )
        return [{"due": _number(row["due"]), "scope": row["scope"]} for row in rows]

    async def remove(self, scope: str) -> None:
        await self.client.execute(f"SELECT {self.prefix}remove($1)", scope)

    async def close(self) -> None:
        self.closed = True
        self._wake()
        if self._listening is not None and await self._listening and self._listener is not None:
            with contextlib.suppress(Exception):
                await self._listener.remove_listener(f"{self.prefix}decisions", self._wake)
        if self._retained is not None:
            await asyncio.gather(self._retained, return_exceptions=True)


class _PostgresLog(DecisionLog):
    def __init__(self, store: PostgresStore) -> None:
        self._s = store

    async def epoch(self) -> str:
        s = self._s
        if s.epoch_future is None:

            async def read() -> str:
                rows = await s.client.fetch(f"SELECT value FROM {s.prefix}meta WHERE key = 'epoch'")
                return str(rows[0]["value"])

            pending = asyncio.ensure_future(read())

            def forget(task: asyncio.Future[str]) -> None:
                if (task.cancelled() or task.exception() is not None) and s.epoch_future is task:
                    s.epoch_future = None

            pending.add_done_callback(forget)
            s.epoch_future = pending
        # Shielded: the read is shared, so one caller's cancellation must not cancel it for the others.
        return await asyncio.shield(s.epoch_future)

    async def bounds(self) -> LogBounds:
        s = self._s
        await s.settle()
        p = s.prefix
        rows = await s.client.fetch(
            f"""WITH {s.sql_horizon} SELECT
        (SELECT {s.sql_position} FROM {p}decisions, h WHERE txid < (SELECT xid FROM h) ORDER BY txid, n LIMIT 1) AS first,
        ({s.sql_head}) AS last"""
        )
        return {"first": rows[0]["first"], "last": rows[0]["last"]}

    async def find(self, kind: DecisionKind, id: str) -> DecisionRow | None:
        s = self._s
        await s.settle()
        rows = await s.client.fetch(
            f"""WITH {s.sql_horizon} SELECT d.scope, d.kind, d.id, d.route, d.at, d.body, {s.sql_position} AS position FROM {s.prefix}decisions d, h
        WHERE kind = $1 AND id = $2 AND txid < (SELECT xid FROM h)""",
            kind,
            id,
        )
        return None if not rows else _decision_row(rows[0])

    async def read(self, query: DecisionQuery) -> DecisionPage:
        s = self._s
        after = None if query["after"] is None else _parse_position(query["after"])
        await s.settle()
        bind = _Binder()
        where = ["txid < (SELECT xid FROM h)"]
        if after is not None:
            where.append(f"(txid, n) > ({bind(after[0])}::text::xid8, {bind(after[1])}::int)")
        if "scope" in query:
            where.append(f"scope = {bind(query['scope'])}")
        hits: list[str] = []
        if "scopePrefix" in query:
            hits.append(f"starts_with(scope, {bind(query['scopePrefix'])})")
        if "kinds" in query:
            hits.append(f"kind = ANY({bind(list(query['kinds']))}::text[])")
        if "routes" in query:
            hits.append(f"route = ANY({bind(list(query['routes']))}::text[])")
        if "sinceAt" in query:
            hits.append(f"at >= {bind(float(query['sinceAt']))}::float8")
        limit = query["limit"]
        scan_limit = query.get("scanLimit", 4 * limit)
        candidates = bind(scan_limit + 1)
        limited = bind(limit)
        p = s.prefix
        rows = await s.client.fetch(
            f"""WITH {s.sql_horizon}, candidates AS MATERIALIZED (
        SELECT d.txid, d.n, d.scope, d.kind, d.id, d.route, d.at, d.body, COALESCE({" AND ".join(hits) or "true"}, false) AS hit FROM {p}decisions d, h
        WHERE {" AND ".join(where)} ORDER BY txid, n LIMIT {candidates}
      ), ranked AS (
        SELECT *, count(*) FILTER (WHERE hit) OVER (ORDER BY txid, n) AS hits FROM candidates
      ), head AS (SELECT ({s.sql_head}) AS head)
      SELECT head.head, {s.sql_position} AS position, scope, kind, id, route, at, hit,
        CASE WHEN hit AND hits <= {limited} THEN body END AS body
      FROM head LEFT JOIN ranked ON true ORDER BY txid, n""",
            *bind.values,
        )
        seen_head = rows[0]["head"]
        found = [row for row in rows if row["position"] is not None]
        result: list[DecisionRow] = []
        examined = 0
        checked = query["after"]
        for row in found:
            if examined >= scan_limit or len(result) >= limit:
                break
            examined += 1
            checked = row["position"]
            if row["hit"] is True:
                result.append(_decision_row(row))
        more = examined < len(found)
        if not more and seen_head is not None and (checked is None or _compare(seen_head, checked) > 0):
            checked = seen_head
        return {"rows": result, "checked": checked, "more": more}

    async def wait(self, after: str | None, block_ms: float, signal: AbortSignal) -> None:
        """Returns once a decision lands after ``after``. With a listener it wakes on NOTIFY; without, it polls every 100 ms."""
        s = self._s
        if signal.aborted or s.closed:
            return
        loop = asyncio.get_running_loop()
        state: dict[str, Any] = {"notified": False, "resume": None}

        def notify() -> None:
            state["notified"] = True
            resume: asyncio.Future[None] | None = state["resume"]
            if resume is not None and not resume.done():
                resume.set_result(None)

        s.wakeups.add(notify)
        remove = signal.add_listener(notify)
        try:
            if after is not None:
                _parse_position(after)
            registration = s.listen()
            subscribed = {"value": False}

            def registered(future: asyncio.Future[bool]) -> None:
                subscribed["value"] = not future.cancelled() and future.exception() is None and future.result()
                notify()

            registration.add_done_callback(registered)
            deadline = time.monotonic() + block_ms / 1000
            delay = 0.005
            p = s.prefix
            while not s.closed and not signal.aborted:
                state["notified"] = False
                rows = await s.client.fetch(
                    f"""WITH {s.sql_horizon} SELECT ({s.sql_head}) AS head,
            COALESCE((SELECT txid FROM {p}decisions ORDER BY txid DESC, n DESC LIMIT 1) >= (SELECT xid FROM h), false) AS pending"""
                )
                last = rows[0]["head"]
                if last is not None and (after is None or _compare(last, after) > 0):
                    return
                left = deadline - time.monotonic()
                if left <= 0 or s.closed or signal.aborted:
                    return
                if state["notified"]:
                    delay = 0.005
                    continue
                poll = delay if rows[0]["pending"] is True else left if subscribed["value"] else 0.1
                resume = loop.create_future()
                state["resume"] = resume
                timer = loop.call_later(min(left, poll), lambda resume=resume: None if resume.done() else resume.set_result(None))
                try:
                    await resume
                finally:
                    timer.cancel()
                    state["resume"] = None
                delay = 0.005 if state["notified"] else min(delay * 2, 0.25)
        except Exception:
            return
        finally:
            s.wakeups.discard(notify)
            remove()

    async def trim(self, retention: Retention) -> None:
        s = self._s
        await s.settle()
        bind = _Binder()
        ctes: list[str] = []
        floors: list[str] = []
        if "maxRows" in retention:
            ctes.append(f"count_floor AS (SELECT txid, n FROM visible ORDER BY txid DESC, n DESC OFFSET {bind(max(0, retention['maxRows'] - 1))}::bigint LIMIT 1)")
            floors.extend(["SELECT * FROM count_floor", "SELECT * FROM oldest WHERE NOT EXISTS (SELECT 1 FROM count_floor)"])
        if "olderThanMs" in retention:
            ctes.append(f"age_floor AS (SELECT txid, n FROM visible WHERE at >= {bind(float(_now_ms() - retention['olderThanMs']))}::float8 ORDER BY txid, n LIMIT 1)")
            floors.extend(["SELECT * FROM age_floor", "SELECT * FROM head WHERE NOT EXISTS (SELECT 1 FROM age_floor)"])
        if not ctes:
            return
        p = s.prefix
        await s.client.execute(
            f"""WITH {s.sql_horizon}, visible AS NOT MATERIALIZED (
        SELECT txid, n, at FROM {p}decisions, h WHERE txid < (SELECT xid FROM h)
      ), head AS (SELECT txid, n FROM visible ORDER BY txid DESC, n DESC LIMIT 1),
      oldest AS (SELECT txid, n FROM visible ORDER BY txid, n LIMIT 1),
      {", ".join(ctes)}, floor AS (
        {" UNION ALL ".join(floors)} ORDER BY txid DESC, n DESC LIMIT 1
      ) DELETE FROM {p}decisions d USING head, floor
      WHERE (d.txid, d.n) < (head.txid, head.n) AND (d.txid, d.n) < (floor.txid, floor.n)""",
            *bind.values,
        )

    def compare(self, a: str, b: str) -> int:
        return _compare(a, b)


def postgres_store(
    client: PostgresClient, *, prefix: str = "attend_", retain: Retention | None = None, listener: PostgresListener | None = None, async_commit: bool = False
) -> PostgresStore:
    """A ScopeStore over ``client``, an ``asyncpg.Pool``. Run ``migrate(client, prefix=...)`` first.

    ``listener`` is a dedicated ``asyncpg.Connection`` that runs LISTEN, so ``log.wait`` wakes on
    each new decision in this process and others; without it, ``log.wait`` polls every 100 ms.
    ``async_commit`` sets ``synchronous_commit = off`` per group: it can lose the last commits on a
    crash of the database, and never corrupts.
    """
    return PostgresStore(client, prefix=prefix, retain=retain, listener=listener, async_commit=async_commit)
