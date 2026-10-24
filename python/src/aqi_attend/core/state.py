"""Owned state to and from its snapshot, ``ScopeRecord``."""

from __future__ import annotations

from .ids import fnv_start, fold_id
from .reducer import next_due, same
from .seen import seen_from, seen_ids
from .types import Buffer, Entry, FnvState, HeldBatch, ScopeRecord, ScopeState, SettlePolicy
from .window import estimate_rendered_bytes, open_window, summarize, window_from


def initial_state(scope: str) -> ScopeState:
    """An empty, owned scope at version zero."""
    seen, known = seen_from([], [], [])
    return ScopeState(scope, 0, {}, seen, known, open_window(scope), None, [], [], [], None)


def from_record(record: ScopeRecord, policy: SettlePolicy) -> ScopeState:
    """Builds owned state from a snapshot. Copies every list it will mutate and never mutates ``record``."""
    held = record["batch"]
    scope = record["scope"]
    buffer = record["buffer"]
    seen, known = seen_from(
        record["seen"],
        (entry["id"] for entry in buffer["entries"]),
        (entry["id"] for entry in held["window"]["events"]) if held is not None else (),
    )
    state = ScopeState(
        scope=scope,
        version=record["version"],
        cursors=record["cursors"],
        seen=seen,
        known=known,
        open=window_from(scope, buffer),
        batch=HeldBatch(held["window"], held["phase"], _held_hash(scope, held["window"]["events"])) if held is not None else None,
        decisions=list(record["outbox"]["decisions"]),
        dead_letters=list(record["outbox"]["deadLetters"]),
        recent=record["recent"],
        due_at=None,
    )
    state.due_at = next_due(policy, state)
    return state


def _held_hash(scope: str, events: list[Entry]) -> FnvState:
    hash = fnv_start(scope)
    for entry in events:
        fold_id(hash, entry["id"])
    return hash


def to_record(state: ScopeState) -> ScopeRecord:
    """The state as a fresh ``ScopeRecord``. Shares immutable parts."""
    open = state.open
    buffer: Buffer = {"entries": list(open.entries), "dropped": open.dropped, "gap": open.gap, "forcedAt": open.forced_at}
    if open.ended_at is not None:
        buffer["endedAt"] = open.ended_at
    return {
        "version": state.version,
        "scope": state.scope,
        "cursors": state.cursors,
        "seen": seen_ids(state.seen),
        "buffer": buffer,
        "batch": {"window": state.batch.window, "phase": state.batch.phase} if state.batch is not None else None,
        "outbox": {"decisions": list(state.decisions), "deadLetters": list(state.dead_letters)},
        "recent": state.recent,
        "dueAt": state.due_at,
    }


def quiescent(state: ScopeState) -> bool:
    """True when the scope holds nothing, so it may be evicted from the cache."""
    return not state.open.entries and state.batch is None and not state.decisions and not state.dead_letters and state.due_at is None


def assert_invariants(state: ScopeState, policy: SettlePolicy) -> None:
    """Checks the state's invariants after an apply, in tests. Raises, naming the broken invariant."""
    ring = state.seen
    if ring.size > max(0, policy["seenCount"]) or len(ring.ids) != ring.capacity or ring.head < 0 or (ring.capacity > 0 and ring.head >= ring.capacity):
        raise AssertionError("I2: seen ring")
    expected: dict[str, int] = {}
    for id in seen_ids(ring):
        expected[id] = expected.get(id, 0) + 1
    windows: set[str] = set()
    for entries in (state.open.entries, state.batch.window["events"] if state.batch is not None else []):
        for entry in entries:
            if entry["id"] in windows:
                raise AssertionError("I1: duplicate window id")
            windows.add(entry["id"])
            expected[entry["id"]] = expected.get(entry["id"], 0) + 1
    if expected != state.known:
        raise AssertionError("I1: known ids")
    open = state.open
    summary = summarize(open.entries)
    for name in summary.__slots__:  # pyright: ignore[reportAttributeAccessIssue]
        if not same(getattr(summary, name), getattr(open.summary, name)):
            raise AssertionError(f"I3: {name}")
    if len(open.sizes) != len(open.entries) or sum(open.sizes) != open.bytes:
        raise AssertionError("I4: sizes and bytes")
    if any(size != estimate_rendered_bytes(entry) for size, entry in zip(open.sizes, open.entries, strict=True)):
        raise AssertionError("I4: sizes")
    if open.hash_valid and _held_hash(state.scope, open.entries).value != open.hash.value:
        raise AssertionError("I5: hash")
    if state.batch is not None and state.batch.hash is not None and _held_hash(state.scope, state.batch.window["events"]).value != state.batch.hash.value:
        raise AssertionError("I5: held hash")
    if open.ended_at is not None and not open.entries:
        raise AssertionError("I3: endedAt on an empty window")
    if not same(state.due_at, next_due(policy, state)):
        raise AssertionError("I6: dueAt")
    for lane in (state.decisions, state.dead_letters):
        if any(delivery["phase"]["phase"] == "inFlight" for delivery in lane[1:]):
            raise AssertionError("outbox: more than one in flight in a lane")
    if open.monotone and any(b["seenAt"] < a["seenAt"] for a, b in zip(open.entries, open.entries[1:], strict=False)):
        raise AssertionError("monotone: seenAt decreased")
