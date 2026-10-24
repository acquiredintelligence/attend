"""``aqi_attend.store.redis``: a ScopeStore over Redis 6.2 or later, on redis-py's asyncio client.

A port of ``src/store/redis.ts`` with the same Lua scripts, keys, fields and JSON, so a Python
process and a TypeScript process can share one Redis. One EVALSHA per group commit validates every
version first, then writes each scope with its version bumped last. On Redis Cluster, put a hash
tag in the prefix, such as ``attend:{app}:``, so that every key one script touches is in one slot.
"""

from __future__ import annotations

import asyncio
import json
import re
import struct
import time
import uuid
from collections.abc import Sequence
from typing import Any, Protocol, cast

from .._json import number, stringify
from ..core.types import LoggedInput, ScopeRecord, SettlePolicy
from ..signal import AbortSignal
from . import _redis_scripts as scripts
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
    StoredScope,
)

_POSITION = re.compile(r"[0-9]{20}-[0-9]{20}")


class RedisClient(Protocol):
    """What the store needs from ``redis.asyncio.Redis``: one raw command at a time."""

    async def execute_command(self, *args: Any, **options: Any) -> Any: ...


def _text(reply: object) -> str:
    if isinstance(reply, str):
        return reply
    if isinstance(reply, (bytes, bytearray)):
        return reply.decode("utf-8")
    raise TypeError(f"attend/redis: expected text, got {type(reply).__name__}")


def _no_script(error: BaseException) -> bool:
    """A NOSCRIPT reply. redis-py strips the code from the message and raises ``NoScriptError``; other clients keep it in the text."""
    return type(error).__name__ == "NoScriptError" or "NOSCRIPT" in str(error)


def _nullable_text(reply: object) -> str | None:
    return None if reply is None or reply is False else _text(reply)


def _numeric(reply: object) -> float:
    if isinstance(reply, bool):
        raise TypeError("attend/redis: expected a number")
    if isinstance(reply, (int, float)):
        return reply
    value = float(_text(reply))
    return int(value) if value.is_integer() else value


def _list(reply: object) -> list[Any]:
    if isinstance(reply, list):
        return cast(list[Any], reply)
    if isinstance(reply, tuple):
        return list(cast(tuple[Any, ...], reply))
    raise TypeError(f"attend/redis: expected an array reply, got {type(reply).__name__}")


def due_hex(value: float) -> str:
    """A due value as 16 hex characters that sort as the number does."""
    if value != value or value in (float("inf"), float("-inf")):
        raise ValueError("attend/redis: due must be finite")
    (bits,) = struct.unpack(">Q", struct.pack(">d", 0.0 if value == 0 else float(value)))
    mapped = (~bits & 0xFFFFFFFFFFFFFFFF) if bits & (1 << 63) else bits | (1 << 63)
    return f"{mapped:016x}"


def from_due_hex(hex: str) -> float:
    bits = int(hex, 16)
    raw = bits & ((1 << 63) - 1) if bits & (1 << 63) else ~bits & 0xFFFFFFFFFFFFFFFF
    (value,) = struct.unpack(">d", struct.pack(">Q", raw))
    return int(value) if value.is_integer() else value


def _validate_position(position: str) -> None:
    if not _POSITION.fullmatch(position):
        raise ValueError(f'attend/redis: not a position from this log: "{position}"')


def _positive_integer(value: object, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0 or value > 9007199254740991:
        raise ValueError(f"attend/redis: invalid {name}")


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _retention(value: Retention | None) -> list[str]:
    retain: Retention = value or {}
    rows = retain.get("maxRows")
    age = retain.get("olderThanMs")
    if rows is not None and (isinstance(rows, bool) or rows < 0 or not float(rows).is_integer()):
        raise ValueError("attend/redis: invalid maxRows")
    if age is not None and (age != age or age < 0 or age == float("inf")):
        raise ValueError("attend/redis: invalid olderThanMs")
    return [str(rows if rows is not None else -1), "" if age is None else number(_now_ms() - age)]


def _prepare(write: ScopeWrite) -> list[str]:
    expected = write["expectedVersion"]
    inputs = write["inputs"]
    if expected < 0 or expected + len(inputs) > 9007199254740991:
        raise ValueError("attend/redis: invalid version")
    snapshot = write["snapshot"]
    if snapshot is not None and snapshot["version"] != expected + len(inputs):
        raise ValueError("attend/redis: invalid snapshot version")
    serialized = write.get("serialized")
    if serialized is not None and (len(serialized["inputs"]) != len(inputs) or len(serialized["appends"]) != len(write["appends"])):
        raise ValueError("attend/redis: invalid serialized write")
    due = "keep" if "due" not in write else "" if write["due"] is None else due_hex(write["due"])
    args = [write["scope"], str(expected), str(len(inputs)), "0" if snapshot is None else "1", due, str(len(write["appends"]))]
    args.extend(serialized["inputs"] if serialized is not None else [stringify(logged) for logged in inputs])
    if snapshot is not None:
        record = serialized["snapshot"] if serialized is not None else stringify(snapshot["record"])
        if record is None:
            raise ValueError("attend/redis: missing serialized snapshot")
        args.extend([record, str(snapshot["version"]), stringify(snapshot["policy"]), str(snapshot["reducer"])])
    for i, row in enumerate(write["appends"]):
        at = row["at"]
        if at != at or at in (float("inf"), float("-inf")):
            raise ValueError("attend/redis: invalid append time")
        body = serialized["appends"][i] if serialized is not None else stringify(row["body"])
        args.extend([row["kind"], row["id"], row["scope"], number(at), row["route"] or "", body])
    return args


def _json(reply: object) -> dict[str, Any] | None:
    value = json.loads(_text(reply))
    return cast(dict[str, Any], value) if isinstance(value, dict) else None


def _parse_record(reply: object, scope: str) -> ScopeRecord:
    value = _json(reply)
    if value is None or value.get("scope") != scope or isinstance(value.get("version"), bool) or not isinstance(value.get("version"), (int, float)):
        raise ValueError("attend/redis: corrupt scope")
    return cast(ScopeRecord, value)


def _parse_policy(reply: object) -> SettlePolicy:
    value = _json(reply)
    if value is None or not isinstance(value.get("closingGraceMs"), (int, float)):
        raise ValueError("attend/redis: corrupt policy")
    return cast(SettlePolicy, value)


def _parse_input(reply: object) -> LoggedInput:
    value = _json(reply)
    if value is None or not isinstance(value.get("now"), (int, float)) or not isinstance(value.get("input"), dict):
        raise ValueError("attend/redis: corrupt input")
    return cast(LoggedInput, value)


def _parse_row(reply: object) -> DecisionRow:
    id, raw = _list(reply)
    values = _list(raw)
    fields = {_text(values[i]): values[i + 1] for i in range(0, len(values), 2)}
    kind = _text(fields.get("kind"))
    body = _json(fields.get("body"))
    if kind not in ("decision", "deadLetter") or body is None or body.get("id") != _text(fields.get("id")):
        raise ValueError("attend/redis: corrupt decision")
    ms, _, seq = _text(id).partition("-")
    if not ms or not seq:
        raise ValueError("attend/redis: corrupt position")
    return cast(
        DecisionRow,
        {
            "kind": kind,
            "id": _text(fields.get("id")),
            "scope": _text(fields.get("scope")),
            "at": _numeric(fields.get("at")),
            "route": _text(fields.get("route")) or None,
            "body": body,
            "position": f"{ms.zfill(20)}-{seq.zfill(20)}",
        },
    )


class RedisStore(ScopeStore):
    """The Redis ScopeStore. Durable only as far as your Redis persistence settings."""

    durable = True

    def __init__(self, client: RedisClient, *, prefix: str = "attend:", retain: Retention | None = None, blocking: RedisClient | None = None) -> None:
        self._client = client
        self._prefix = prefix
        self._retain = retain
        self._blocking = blocking
        self._shas: dict[str, asyncio.Future[str]] = {}
        self._due_key = f"{prefix}due-index"
        self._stream = f"{prefix}decisions"
        self._decided = f"{prefix}decided"
        self.log = _RedisLog(self)

    def _scope_key(self, scope: str) -> str:
        return f"{self._prefix}scope:{scope}"

    def _inputs_key(self, scope: str) -> str:
        return f"{self._prefix}inputs:{scope}"

    def _index_key(self, scope: str) -> str:
        return f"{self._prefix}by-scope:{scope}"

    def _load_script(self, source: str) -> asyncio.Future[str]:
        async def load() -> str:
            return _text(await self._client.execute_command("SCRIPT", "LOAD", source))

        pending = asyncio.ensure_future(load())
        self._shas[source] = pending

        def forget(task: asyncio.Future[str]) -> None:
            if (task.cancelled() or task.exception() is not None) and self._shas.get(source) is task:
                del self._shas[source]

        pending.add_done_callback(forget)
        return pending

    async def run(self, source: str, keys: Sequence[str], args: Sequence[str] = ()) -> Any:
        # Shielded: the load is shared, so one caller's cancellation must not cancel it for the others.
        sha = await asyncio.shield(self._shas.get(source) or self._load_script(source))
        try:
            return await self._client.execute_command("EVALSHA", sha, str(len(keys)), *keys, *args)
        except Exception as error:
            if not _no_script(error):
                raise
            sha = await asyncio.shield(self._load_script(source))
            return await self._client.execute_command("EVALSHA", sha, str(len(keys)), *keys, *args)

    async def load(self, scope: str) -> StoredScope | None:
        reply = await self.run(scripts.LOAD, [self._scope_key(scope), self._inputs_key(scope)])
        if reply is None or reply is False:
            return None
        version, snapshot, snapshot_version, policy, reducer, tail = _list(reply)
        return {
            "version": int(_numeric(version)),
            "snapshot": None
            if snapshot is None or snapshot is False
            else {
                "version": int(_numeric(snapshot_version)),
                "record": _parse_record(snapshot, scope),
                "policy": _parse_policy(policy),
                "reducer": int(_numeric(reducer)),
            },
            "tail": [_parse_input(item) for item in _list(tail)],
        }

    async def commit(self, writes: Sequence[ScopeWrite]) -> list[CommitOutcome]:
        if not writes:
            return []
        prepared = [arg for write in writes for arg in _prepare(write)]
        keys = [self._due_key, self._stream, self._decided]
        for write in writes:
            keys.extend([self._scope_key(write["scope"]), self._inputs_key(write["scope"]), *(self._index_key(row["scope"]) for row in write["appends"])])
        reply = await self.run(scripts.COMMIT, keys, [self._prefix, *_retention(self._retain), str(len(writes)), *prepared])
        outcomes: list[CommitOutcome] = []
        for item in _list(reply):
            outcome = _text(item)
            if outcome != "ok" and outcome != "conflict":
                raise ValueError("attend/redis: invalid commit outcome")
            outcomes.append(outcome)
        return outcomes

    async def due(self, now: float, limit: int, after: DueKey | None = None) -> list[DueKey]:
        _positive_integer(limit, "limit")
        lower = f"({due_hex(after['due'])}:{after['scope']}" if after is not None else "-"
        reply = await self.run(scripts.DUE, [self._due_key], [self._prefix, lower, f"({due_hex(now)};", str(limit)])
        result: list[DueKey] = []
        for item in _list(reply):
            member = _text(item)
            result.append({"due": from_due_hex(member[:16]), "scope": member[17:]})
        return result

    async def remove(self, scope: str) -> None:
        await self.run(scripts.REMOVE, [self._scope_key(scope), self._inputs_key(scope), self._due_key], [scope])


class _RedisLog(DecisionLog):
    def __init__(self, store: RedisStore) -> None:
        self._store = store

    async def epoch(self) -> str:
        return _text(await self._store.run(scripts.EPOCH, [f"{self._store._prefix}meta"], [str(uuid.uuid4())]))  # pyright: ignore[reportPrivateUsage]

    async def bounds(self) -> LogBounds:
        first, last = _list(await self._store.run(scripts.BOUNDS, [self._store._stream]))  # pyright: ignore[reportPrivateUsage]
        return {"first": _nullable_text(first), "last": _nullable_text(last)}

    async def read(self, query: DecisionQuery) -> DecisionPage:
        store = self._store
        after = query["after"]
        if after is not None:
            _validate_position(after)
        limit = query["limit"]
        scan_limit = query.get("scanLimit", limit * 4)
        _positive_integer(limit, "limit")
        _positive_integer(scan_limit, "scanLimit")
        scope = query.get("scope")
        prefix = query.get("scopePrefix")
        kinds = query.get("kinds")
        routes = query.get("routes")
        since = query.get("sinceAt")
        args = [
            after or "",
            "0" if scope is None else "1",
            scope or "",
            "0" if prefix is None else "1",
            prefix or "",
            str(-1 if kinds is None else len(kinds)),
            *(kinds or []),
            str(-1 if routes is None else len(routes)),
            *(routes or []),
            "" if since is None else number(since),
            str(limit),
            str(scan_limit),
        ]
        rows, checked, more = _list(await store.run(scripts.READ, [store._stream, store._index_key(scope or "")], args))  # pyright: ignore[reportPrivateUsage]
        return {"rows": [_parse_row(row) for row in _list(rows)], "checked": _nullable_text(checked), "more": _numeric(more) == 1}

    async def find(self, kind: DecisionKind, id: str) -> DecisionRow | None:
        store = self._store
        reply = await store.run(scripts.FIND, [store._stream, store._decided], [f"{kind}:{id}"])  # pyright: ignore[reportPrivateUsage]
        return None if reply is None or reply is False else _parse_row(reply)

    async def wait(self, after: str | None, block_ms: float, signal: AbortSignal) -> None:
        """Returns once a decision lands after ``after``. With ``blocking`` it waits on XREAD BLOCK; without, it polls every 250 ms."""
        try:
            if after is not None:
                _validate_position(after)
        except ValueError:
            return
        if signal.aborted or block_ms <= 0:
            return
        deadline = time.monotonic() + block_ms / 1000
        blocking = self._store._blocking  # pyright: ignore[reportPrivateUsage]
        stream = self._store._stream  # pyright: ignore[reportPrivateUsage]

        async def watch() -> None:
            while time.monotonic() < deadline:
                try:
                    last = (await self.bounds())["last"]
                    if last is not None and (after is None or last > after):
                        return
                    left = deadline - time.monotonic()
                    if blocking is not None:
                        block = max(1, min(int(left * 1000 + 0.999), 1000))
                        reply = await blocking.execute_command("XREAD", "BLOCK", str(block), "COUNT", "1", "STREAMS", stream, after or "0-0")
                        if reply:
                            return
                    else:
                        await asyncio.sleep(max(0.0, min(left, 0.25)))
                except Exception:
                    await asyncio.sleep(max(0.0, min(deadline - time.monotonic(), 0.25)))

        task = asyncio.ensure_future(watch())
        aborted = asyncio.ensure_future(signal.wait())
        try:
            await asyncio.wait({task, aborted}, timeout=max(0.0, deadline - time.monotonic()), return_when=asyncio.FIRST_COMPLETED)
        finally:
            aborted.cancel()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, aborted, return_exceptions=True)

    async def trim(self, retention: Retention) -> None:
        store = self._store
        args = _retention(retention)
        while True:
            dropped = _numeric(await store.run(scripts.TRIM, [store._stream, store._decided], [store._prefix, *args]))  # pyright: ignore[reportPrivateUsage]
            if dropped < 256:
                return

    def compare(self, a: str, b: str) -> int:
        return -1 if a < b else 1 if a > b else 0


def redis_store(client: RedisClient, *, prefix: str = "attend:", retain: Retention | None = None, blocking: RedisClient | None = None) -> RedisStore:
    """A ScopeStore over ``client``, a ``redis.asyncio.Redis`` (or anything with ``execute_command``).

    redis-py runs each concurrent command on a pooled connection of its own, and its default pool
    raises once 100 are in use, where node-redis pipelines every command on one connection. attend
    loads every scope it touches at once, so give the client a pool that waits instead:
    ``Redis.from_pool(BlockingConnectionPool.from_url(url, max_connections=50, timeout=None))``.

    ``blocking`` is a client for the decision log's XREAD BLOCK, so a subscriber hears each
    decision at once; without it, ``log.wait`` polls every 250 ms. Each blocked read holds one of
    its connections for up to a second.
    """
    return RedisStore(client, prefix=prefix, retain=retain, blocking=blocking)
