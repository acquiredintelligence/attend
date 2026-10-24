"""The seen ring and the refcounts of every id the scope knows."""

from __future__ import annotations

from collections.abc import Iterable, Sequence

from .types import IdRing, ScopeState


def seen_from(seen: Sequence[str], open: Iterable[str], held: Iterable[str]) -> tuple[IdRing, dict[str, int]]:
    """The snapshot's seen ring and refcounts across seen, open and held ids. O(S + W)."""
    known: dict[str, int] = {}
    for ids in (seen, open, held):
        for id in ids:
            known[id] = known.get(id, 0) + 1
    return IdRing(list(seen), 0, len(seen), len(seen)), known


def seen_ids(ring: IdRing) -> list[str]:
    """The ring's ids, oldest first, as a fresh list for ``ScopeRecord.seen``."""
    if ring.head == 0:
        return ring.ids[: ring.size]
    return [ring.ids[(ring.head + i) % ring.capacity] for i in range(ring.size)]


def seen_admit(state: ScopeState, id: str) -> bool:
    """Admits ``id`` for a batch. False when it is already known, so the caller counts a duplicate.

    Otherwise counts it for the ring and for the open entry the caller appends. Eviction waits for
    ``seen_trim``.
    """
    known = state.known
    if id in known:
        return False
    ring = state.seen
    if ring.size == ring.capacity:
        capacity = max(8, ring.capacity * 2)
        ids = seen_ids(ring)
        ids.extend("" for _ in range(capacity - len(ids)))
        ring.ids = ids
        ring.capacity = capacity
        ring.head = 0
    known[id] = 2
    ring.ids[(ring.head + ring.size) % ring.capacity] = id
    ring.size += 1
    return True


def seen_trim(state: ScopeState, seen_count: int) -> None:
    """After the batch: evicts the oldest ids down to ``seen_count``, releasing each."""
    ring = state.seen
    limit = max(0, seen_count)
    while ring.size > limit:
        seen_release(state.known, ring.ids[ring.head])
        ring.ids[ring.head] = ""
        ring.head = (ring.head + 1) % ring.capacity
        ring.size -= 1


def seen_release(known: dict[str, int], id: str) -> None:
    """An id left the ring, the open window or the held window: decrement its count and delete it at zero."""
    count = known[id]
    if count == 1:
        del known[id]
    else:
        known[id] = count - 1
