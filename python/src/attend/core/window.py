"""The open window: appended in place, pruned and shed by its single writer, with an O(1) deadline."""

from __future__ import annotations

import math

from .._json import stringify, utf8_length
from .ids import fnv_start, fold_id
from .types import Buffer, Entry, Event, FnvState, Kind, OpenWindow, SettlePolicy, SettleReason, TriggerSummary


def is_act(kind: Kind) -> bool:
    """Closing, continuous and passive entries are acts."""
    return kind == "closing" or kind == "continuous" or kind == "passive"


def _sheddable(kind: Kind) -> bool:
    return kind == "consequence" or kind == "context"


def estimate_rendered_bytes(event: Event) -> int:
    """The UTF-8 bytes of name and text (or JSON data), plus 40 bytes of framing."""
    text = event.get("text")
    return utf8_length(event["name"]) + utf8_length(text if text is not None else stringify(event["data"])) + 40


def _summarize_entry(summary: TriggerSummary, entry: Entry, index: int) -> None:
    kind = entry["kind"]
    seen_at = entry["seenAt"]
    if kind == "flush":
        summary.min_flush_at = _min(summary.min_flush_at, seen_at)
    elif kind == "closing":
        summary.last_closing_at = seen_at
    elif kind == "continuous":
        summary.last_stream_at = seen_at
    elif kind == "passive":
        summary.last_passive_at = seen_at
    if summary.first_act_index == -1 and is_act(kind):
        summary.first_act_index = index
        summary.first_act_at = seen_at
    if _sheddable(kind):
        summary.sheddable += 1
        if summary.first_act_index == -1:
            summary.prefix_min_sheddable_at = _min(summary.prefix_min_sheddable_at, seen_at)
    summary.last_seen_at = seen_at


def _min(a: float, b: float) -> float:
    """``Math.min`` of two numbers: NaN when either is."""
    if a != a or b != b:
        return math.nan
    return a if a <= b else b


def summarize(entries: list[Entry]) -> TriggerSummary:
    summary = TriggerSummary()
    for index, entry in enumerate(entries):
        _summarize_entry(summary, entry, index)
    return summary


def open_window(scope: str) -> OpenWindow:
    """An empty open window whose hash starts with ``len:scope``."""
    return OpenWindow(
        entries=[],
        sizes=[],
        bytes=0,
        dropped=0,
        gap=False,
        forced_at=None,
        ended_at=None,
        summary=TriggerSummary(),
        hash=fnv_start(scope),
        hash_valid=True,
        monotone=True,
    )


def window_from(scope: str, buffer: Buffer) -> OpenWindow:
    """Copies a snapshot buffer into new lists, sharing its immutable entries. O(W)."""
    open = open_window(scope)
    for entry in buffer["entries"]:
        window_append(open, entry, estimate_rendered_bytes(entry))
    open.dropped = buffer["dropped"]
    open.gap = buffer["gap"]
    open.forced_at = buffer["forcedAt"]
    open.ended_at = buffer.get("endedAt")
    return open


def window_append(open: OpenWindow, entry: Entry, size: int) -> None:
    """Appends an entry and updates sizes, summary, monotonicity and any valid hash in O(1)."""
    if entry["seenAt"] < open.summary.last_seen_at:
        open.monotone = False
    index = len(open.entries)
    open.entries.append(entry)
    open.sizes.append(size)
    open.bytes += size
    _summarize_entry(open.summary, entry, index)
    if open.hash_valid:
        fold_id(open.hash, entry["id"])


def _prefix_minimum(open: OpenWindow) -> float:
    entries = open.entries
    end = len(entries) if open.summary.first_act_index == -1 else open.summary.first_act_index
    least = math.inf
    for i in range(end):
        entry = entries[i]
        if _sheddable(entry["kind"]):
            if open.monotone:
                return entry["seenAt"]
            least = _min(least, entry["seenAt"])
    return least


def _removed_from(open: OpenWindow, count: int, size: int, prefix_count: int) -> None:
    open.dropped += count
    open.bytes -= size
    summary = open.summary
    summary.sheddable -= count
    if summary.first_act_index != -1:
        summary.first_act_index -= prefix_count
    if prefix_count > 0:
        summary.prefix_min_sheddable_at = _prefix_minimum(open)
    summary.last_seen_at = open.entries[-1]["seenAt"] if open.entries else math.nan
    open.hash_valid = False


def window_prune(open: OpenWindow, max_window_ms: float, now: float) -> list[Entry] | None:
    """Removes stale sheddable entries before the first act, returning them in list order."""
    summary = open.summary
    if summary.prefix_min_sheddable_at + max_window_ms > now:
        return None
    entries = open.entries
    sizes = open.sizes
    end = len(entries) if summary.first_act_index == -1 else summary.first_act_index
    gone: list[Entry] | None = None
    size = 0
    write = 0
    read = 0
    while read < end:
        entry = entries[read]
        stale = entry["seenAt"] + max_window_ms <= now
        if not stale and open.monotone:
            break
        if stale and _sheddable(entry["kind"]):
            if gone is None:
                gone = [entry]
            else:
                gone.append(entry)
            size += sizes[read]
            read += 1
            continue
        if gone is not None:
            entries[write] = entry
            sizes[write] = sizes[read]
        write += 1
        read += 1
    if gone is None:
        return None
    del entries[write:read]
    del sizes[write:read]
    _removed_from(open, len(gone), size, len(gone))
    return gone


def _shed(entries: list[Entry], max_events: int, open: OpenWindow | None) -> list[Entry] | None:
    """Removes the first ``len - max_events`` sheddable entries in one pass, then closes the gap."""
    length = len(entries)
    excess = length - max_events
    if excess <= 0:
        return None
    sizes = None if open is None else open.sizes
    head = length if open is None or open.summary.first_act_index == -1 else open.summary.first_act_index
    gone: list[Entry] | None = None
    size = 0
    before_act = 0
    write = 0
    read = 0
    while read < length and (gone is None or len(gone) < excess):
        entry = entries[read]
        if _sheddable(entry["kind"]):
            if gone is None:
                gone = [entry]
            else:
                gone.append(entry)
            if sizes is not None:
                size += sizes[read]
            if read < head:
                before_act += 1
            read += 1
            continue
        if gone is not None:
            entries[write] = entry
            if sizes is not None:
                sizes[write] = sizes[read]
        write += 1
        read += 1
    if gone is None:
        return None
    del entries[write:read]
    if sizes is not None:
        del sizes[write:read]
    if open is not None:
        _removed_from(open, len(gone), size, before_act)
    return gone


def window_shed(open: OpenWindow, max_events: int) -> list[Entry] | None:
    """Removes the oldest sheddable entries until the window fits, in place."""
    if len(open.entries) <= max_events or open.summary.sheddable == 0:
        return None
    return _shed(open.entries, max_events, open)


def shed_entries(entries: list[Entry], max_events: int) -> list[Entry] | None:
    """Sheds a plain list in place for a merged window, returning removals in order."""
    return _shed(entries, max_events, None)


def window_deadline(policy: SettlePolicy, open: OpenWindow) -> tuple[float, SettleReason] | None:
    """The open window's earliest deadline and its reason, in O(1) from the summary."""
    s = open.summary
    size = len(open.entries)
    at = math.inf
    reason: SettleReason | None = None
    if open.forced_at is not None:
        at = open.forced_at
        reason = "forced"
    if open.ended_at is not None and (reason is None or open.ended_at < at):
        at = open.ended_at
        reason = "closing"
    if size > 0 and size > policy["maxEvents"] and (reason is None or s.last_seen_at < at):
        at = s.last_seen_at
        reason = "overflow"
    if s.min_flush_at != math.inf and (reason is None or s.min_flush_at < at):
        at = s.min_flush_at
        reason = "flush"
    closing = s.last_closing_at + policy["closingGraceMs"]
    if not math.isnan(s.last_closing_at) and (reason is None or closing < at):
        at = closing
        reason = "closing"
    stream = s.last_stream_at + policy["streamGapMs"]
    if not math.isnan(s.last_stream_at) and (reason is None or stream < at):
        at = stream
        reason = "stream_quiet"
    passive = s.last_passive_at + policy["passiveQuietMs"]
    if not math.isnan(s.last_passive_at) and (reason is None or passive < at):
        at = passive
        reason = "passive_quiet"
    age = s.first_act_at + policy["maxWindowMs"]
    if not math.isnan(s.first_act_at) and (reason is None or age < at):
        at = age
        reason = "age"
    max_bytes = policy.get("maxWindowBytes")
    if max_bytes is not None and size > 0 and open.bytes >= max_bytes and (reason is None or s.last_seen_at < at):
        at = s.last_seen_at
        reason = "size"
    return None if reason is None else (at, reason)


def deadline(policy: SettlePolicy, buffer: Buffer) -> tuple[float, SettleReason] | None:
    """Summarizes a snapshot once and computes its deadline without hashing or mutating it."""
    size = 0
    if policy.get("maxWindowBytes") is not None:
        size = sum(estimate_rendered_bytes(entry) for entry in buffer["entries"])
    return window_deadline(
        policy,
        OpenWindow(
            entries=buffer["entries"],
            sizes=[],
            bytes=size,
            dropped=buffer["dropped"],
            gap=buffer["gap"],
            forced_at=buffer["forcedAt"],
            ended_at=buffer.get("endedAt"),
            summary=summarize(buffer["entries"]),
            hash=FnvState(0),
            hash_valid=False,
            monotone=False,
        ),
    )
