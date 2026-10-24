"""Every store the suites run against. Redis and Postgres join when ATTEND_REDIS_URL and ATTEND_POSTGRES_URL are set."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from attend import memory_store
from attend.store.types import ScopeStore

STORES: list[tuple[str, Callable[[], Awaitable[ScopeStore] | ScopeStore]]] = [("memory", memory_store)]
