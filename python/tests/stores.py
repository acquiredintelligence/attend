"""Every store the suites run against.

Redis joins when ``ATTEND_REDIS_URL`` is set and Postgres when ``ATTEND_POSTGRES_URL`` is set, as in
the TypeScript suite. Each store a factory makes has a key or table prefix of its own and its own
client, and closing it drops its keys or tables and closes that client.
"""

from __future__ import annotations

import os
import secrets
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, cast

from aqi_attend import memory_store
from aqi_attend.store.types import CommitOutcome, DecisionLog, DueKey, ScopeStore, ScopeWrite, StoredScope

REDIS_URL = os.environ.get("ATTEND_REDIS_URL")
POSTGRES_URL = os.environ.get("ATTEND_POSTGRES_URL")
RUN = secrets.token_hex(4)
_made = 0


class _Owned(ScopeStore):
    """A store plus the cleanup a test owns: its client and its keys or tables."""

    def __init__(self, inner: ScopeStore, cleanup: Callable[[], Awaitable[None]]) -> None:
        self.durable = inner.durable
        self.log: DecisionLog = inner.log
        self.inner = inner
        self._cleanup = cleanup
        self._closed = False

    async def load(self, scope: str) -> StoredScope | None:
        return await self.inner.load(scope)

    async def commit(self, writes: Sequence[ScopeWrite]) -> list[CommitOutcome]:
        return await self.inner.commit(writes)

    async def due(self, now: float, limit: int, after: DueKey | None = None) -> list[DueKey]:
        return await self.inner.due(now, limit, after)

    async def remove(self, scope: str) -> None:
        await self.inner.remove(scope)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self.inner.close()
        await self._cleanup()


def _next(kind: str) -> str:
    global _made
    _made += 1
    return f"{kind}{RUN}{_made}"


async def make_redis(**options: Any) -> _Owned:
    import redis.asyncio as redis

    from aqi_attend.store.redis import redis_store

    assert REDIS_URL is not None
    client: Any = redis.Redis.from_pool(redis.BlockingConnectionPool.from_url(REDIS_URL, max_connections=32, timeout=None))  # pyright: ignore[reportUnknownMemberType]
    blocking: Any = redis.Redis.from_url(REDIS_URL)  # pyright: ignore[reportUnknownMemberType]
    prefix: str = options.pop("prefix", None) or f"attend-py:{{{_next('r')}}}:"

    async def cleanup() -> None:
        keys: list[Any] = [key async for key in client.scan_iter(match=f"{prefix}*", count=500)]
        if keys:
            await client.unlink(*keys)
        await client.aclose()
        await blocking.aclose()

    return _Owned(redis_store(client, prefix=prefix, blocking=blocking, **options), cleanup)


async def redis_pair() -> tuple[ScopeStore, ScopeStore]:
    """Two Redis stores over one prefix, each with its own clients."""
    first = await make_redis()
    prefix: str = first.inner._prefix  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType, reportUnknownVariableType]
    second = await make_redis(prefix=prefix)
    return first, second


async def make_postgres(**options: Any) -> _Owned:
    import asyncpg  # pyright: ignore[reportMissingTypeStubs]

    from aqi_attend.store.postgres import migrate, postgres_store

    assert POSTGRES_URL is not None
    pool = cast(Any, await asyncpg.create_pool(POSTGRES_URL, min_size=1, max_size=4))  # pyright: ignore[reportUnknownMemberType]
    listener = cast(Any, await asyncpg.connect(POSTGRES_URL))  # pyright: ignore[reportUnknownMemberType]
    shared = options.pop("prefix", None)
    prefix: str = shared or f"{_next('p')}_"
    if shared is None:
        await migrate(pool, prefix=prefix)

    async def cleanup() -> None:
        await listener.close()
        if shared is not None:
            await pool.close()
            return
        await pool.execute(
            f"DROP TABLE IF EXISTS {prefix}scopes, {prefix}inputs, {prefix}snapshots, {prefix}decisions, {prefix}meta CASCADE;"
            f"DROP FUNCTION IF EXISTS {prefix}decisions_notify() CASCADE; DROP FUNCTION IF EXISTS {prefix}remove(text) CASCADE;"
        )
        await pool.close()

    return _Owned(postgres_store(pool, prefix=prefix, listener=listener, **options), cleanup)


async def postgres_pair() -> tuple[ScopeStore, ScopeStore]:
    """Two Postgres stores over one set of tables, each with its own pool and listener. The first drops the tables."""
    first = await make_postgres()
    prefix: str = first.inner.prefix  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType, reportUnknownVariableType]
    second = await make_postgres(prefix=prefix)
    return second, first


STORES: list[tuple[str, Callable[[], Awaitable[ScopeStore] | ScopeStore]]] = [("memory", memory_store)]
PAIRS: list[tuple[str, Callable[[], Awaitable[tuple[ScopeStore, ScopeStore]]]]] = []
if REDIS_URL:
    STORES.append(("redis", make_redis))
    PAIRS.append(("redis", redis_pair))
if POSTGRES_URL:
    STORES.append(("postgres", make_postgres))
    PAIRS.append(("postgres", postgres_pair))
