"""Reading the decision log in-process, with the same cursors MCP subscribers use."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import TypedDict, cast

from .._json import clone
from ..clock import Clock
from ..core.types import DeliveredRoute
from ..signal import AbortSignal
from ..store.types import DecisionKind, DecisionLog, DecisionQuery, DecisionRow


class DecisionCursorPage(TypedDict):
    """Copied rows and the cursor checked through this page."""

    rows: list[DecisionRow]
    cursor: str
    truncated: bool


def _encode(epoch: str, position: str | None) -> str:
    return f"v1.{epoch}.{position or ''}"


def _decode(cursor: str) -> tuple[str, str | None]:
    separator = cursor.find(".", 3)
    if not cursor.startswith("v1.") or separator <= 3:
        raise TypeError("attend: invalid decision cursor")
    return cursor[3:separator], cursor[separator + 1 :] or None


async def read_decisions(
    log: DecisionLog,
    clock: Clock,
    *,
    signal: AbortSignal,
    cursor: str | None = None,
    max_age_ms: float | None = None,
    page_size: int = 100,
    scope: str | None = None,
    scope_prefix: str | None = None,
    routes: Sequence[DeliveredRoute] | None = None,
    kinds: Sequence[DecisionKind] | None = None,
) -> AsyncIterator[DecisionCursorPage]:
    """Streams copied decision rows, paging at once while the log has more to scan, then waiting on it."""
    if signal.aborted:
        return
    epoch = await log.epoch()
    bounds = await log.bounds()
    decoded = None if cursor is None else _decode(cursor)
    after = bounds["last"] if decoded is None else decoded[1] if decoded[0] == epoch else None
    first = bounds["first"]
    truncated = decoded is not None and (decoded[0] != epoch or (after is not None and first is not None and log.compare(after, first) < 0))
    since_at = None if max_age_ms is None else clock.now() - max_age_ms
    query: DecisionQuery = {"after": after, "limit": page_size}
    if scope is not None:
        query["scope"] = scope
    if scope_prefix is not None:
        query["scopePrefix"] = scope_prefix
    if routes is not None:
        query["routes"] = list(routes)
    if kinds is not None:
        query["kinds"] = list(kinds)
    if decoded is not None and since_at is not None:
        head = await log.read({"after": after, "limit": 1})
        if head["rows"] and head["rows"][0]["at"] < since_at:
            truncated = True
    if since_at is not None:
        query["sinceAt"] = since_at
    initial = True
    while not signal.aborted:
        query["after"] = after
        page = await log.read(query)
        if signal.aborted:
            return
        checked = page["checked"] if page["checked"] is not None else after
        if initial or page["rows"] or checked != after:
            yield {"rows": cast(list[DecisionRow], clone(page["rows"])), "cursor": _encode(epoch, checked), "truncated": truncated}
            initial = False
            truncated = False
        after = checked
        if not page["more"] and not signal.aborted:
            await log.wait(after, 30_000, signal)
