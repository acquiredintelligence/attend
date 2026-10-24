"""The state rendering the judges share: compact JSON of the window, facts, recent routes and predicates,
trimmed to a budget and halved once on a context-length rejection. A port of ``src/internal/prompt.ts``;
lengths are UTF-16 code units, as there."""

from __future__ import annotations

import asyncio
import math
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import TypeVar, cast

from .._json import quote, stringify, to_fixed, utf16_length
from ..core.types import Entry, JudgeBudget, JudgeInput, OfferedRoute, Route, Window
from ..signal import AbortSignal
from ._defaults import DEFAULT_CRITERIA
from ._http import is_context_length_rejection, too_long

T = TypeVar("T")

RENDER_SLICE = 64
"""Events rendered before yielding to the event loop."""

BYTES_PER_TOKEN = 4
"""UTF-16 code units per estimated token, matching the reducer's size estimate."""


def resolve_criteria(routes: list[OfferedRoute], criteria: Mapping[Route, str] | None = None) -> list[tuple[Route, str]]:
    """The criterion for each offered route: the input's own, then ``criteria``, then ``DEFAULT_CRITERIA``."""
    out: list[tuple[Route, str]] = []
    for offered in routes:
        route = offered["route"]
        criterion = offered.get("criterion")
        if criterion is None:
            criterion = (criteria or {}).get(route)
        out.append((route, criterion if criterion is not None else DEFAULT_CRITERIA[route]))
    return out


def tokens_of(text: str, bytes_per_token: float = BYTES_PER_TOKEN) -> int:
    """ceil(text.length / bytes_per_token), counting UTF-16 code units."""
    return math.ceil(utf16_length(text) / bytes_per_token)


def render_bytes(budget: JudgeBudget | None) -> float:
    """The window's code-unit allowance after reserved tokens; unbounded without a budget."""
    if budget is None:
        return math.inf
    return (budget["contextTokens"] - budget.get("reservedTokens", 0)) * budget.get("bytesPerToken", BYTES_PER_TOKEN)


@dataclass(slots=True)
class Rendered:
    """A window and its serialized events, shared by trimming and retries."""

    input: JudgeInput
    parts: list[str]


def _ago(input: JudgeInput, at: float) -> str:
    return f"{to_fixed((input['now'] - at) / 1000, 1)}s"


def _frame(input: JudgeInput) -> tuple[str, str]:
    window = input["window"]
    guidance = "" if "guidance" not in input else f'"guidance":{quote(input["guidance"])},'
    head = stringify({"reason": window["reason"], "waits": window["waits"], "dropped": window["dropped"], "gap": window["gap"]})
    tail = stringify(
        {
            "facts": input["facts"],
            "recent": [{"route": verdict["route"], "ago": _ago(input, verdict["at"])} for verdict in input["recent"]],
            "predicates": {predicate["name"]: predicate["question"] for predicate in input["predicates"]},
        }
    )
    return f'{{{guidance}"window":{head[:-1]},"events":[', f"]}},{tail[1:]}"


def _sheddable(kind: str) -> bool:
    return kind == "consequence" or kind == "context"


def _cut(rendered: Rendered, removed: set[int]) -> Rendered:
    if not removed:
        return rendered
    input = rendered.input
    window = input["window"]
    events = [event for i, event in enumerate(window["events"]) if i not in removed]
    cut = cast(JudgeInput, {**input, "window": cast(Window, {**window, "events": events, "dropped": window["dropped"] + len(removed)})})
    return Rendered(cut, [part for i, part in enumerate(rendered.parts) if i not in removed])


def _event_part(input: JudgeInput, event: Entry) -> str:
    if "text" in event:
        return stringify({"kind": event["kind"], "name": event["name"], "text": event["text"], "ago": _ago(input, event["seenAt"])})
    return stringify({"kind": event["kind"], "name": event["name"], "data": event["data"], "ago": _ago(input, event["seenAt"])})


async def render_within(input: JudgeInput, max_bytes: float, signal: AbortSignal) -> Rendered:
    """Renders in 64-event slices, then cuts the oldest sheddable events until the state fits or none remain."""
    signal.throw_if_aborted()
    parts: list[str] = []
    events = input["window"]["events"]
    for index, event in enumerate(events):
        if index > 0 and index % RENDER_SLICE == 0:
            await asyncio.sleep(0)
            signal.throw_if_aborted()
        parts.append(_event_part(input, event))
    head, tail = _frame(input)
    lengths = [utf16_length(part) for part in parts]
    size = sum(lengths) + utf16_length(head) + utf16_length(tail)
    kept = len(parts)
    removed: set[int] = set()
    for index, event in enumerate(events):
        if size + max(0, kept - 1) <= max_bytes:
            break
        if not _sheddable(event["kind"]):
            continue
        dropped = input["window"]["dropped"] + len(removed)
        size += len(str(dropped + 1)) - len(str(dropped)) - lengths[index]
        removed.add(index)
        kept -= 1
    return _cut(Rendered(input, parts), removed)


def state_text(rendered: Rendered) -> str:
    """The compact JSON state from the frame and the cached event parts."""
    head, tail = _frame(rendered.input)
    return head + ",".join(rendered.parts) + tail


def halve(rendered: Rendered) -> Rendered | None:
    """Halves the event parts' length, shedding the oldest consequences and context before acts, keeping flushes and at least one event."""
    lengths = [utf16_length(part) for part in rendered.parts]
    size = sum(lengths)
    target = size // 2
    removed: set[int] = set()
    events = rendered.input["window"]["events"]
    for shedding in (True, False):
        for index, event in enumerate(events):
            if size <= target or len(rendered.parts) - len(removed) <= 1:
                break
            if event["kind"] == "flush" or _sheddable(event["kind"]) != shedding:
                continue
            removed.add(index)
            size -= lengths[index]
    return None if not removed else _cut(rendered, removed)


async def send_halving[T](rendered: Rendered, send: Callable[[Rendered], Awaitable[T]]) -> T:
    """Retries one context-length rejection with a halved window; exhaustion is a retryable error."""
    try:
        return await send(rendered)
    except Exception as error:
        if not is_context_length_rejection(error):
            raise
        halved = halve(rendered)
        if halved is None:
            raise too_long(error, "the window cannot be halved") from error
        try:
            return await send(halved)
        except Exception as again:
            if is_context_length_rejection(again):
                raise too_long(again, "rejected again with the window halved") from again
            raise
