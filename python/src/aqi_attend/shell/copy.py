"""Copies at the boundary: events going in, and windows, decisions and letters going out to app code."""

from __future__ import annotations

import asyncio
import math
from collections.abc import Callable, Mapping, Sequence
from typing import Any, cast

from .._json import clone, utf8_length
from ..core.types import DeadLetter, Decision, Entry, Event, JsonValue, Window

_MAX_DEPTH = 1000
SLICE = 64


def _to_json(value: object, depth: int) -> JsonValue:
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        # JSON writes NaN and the infinities as null, and -0 as 0.
        return value + 0.0 if math.isfinite(value) else None
    if depth >= _MAX_DEPTH:
        raise TypeError(f"attend: event data holds a cycle or nests deeper than {_MAX_DEPTH}")
    if isinstance(value, Mapping):
        mapping = cast(Mapping[object, object], value)
        out: dict[str, JsonValue] = {}
        for key, item in mapping.items():
            if not isinstance(key, str):
                raise TypeError("attend: event data keys must be strings")
            out[key] = _to_json(item, depth + 1)
        return out
    if isinstance(value, (list, tuple)):
        items = cast(Sequence[object], value)
        return [_to_json(item, depth + 1) for item in items]
    raise TypeError(f"attend: event data must be JSON, got {type(value).__name__}")


def copy_event(event: Mapping[str, object], fill: tuple[Callable[[], str], Callable[[], float]] | None = None) -> Event:
    """An event the caller cannot reach: a plain JSON value, the same on every store."""
    if not isinstance(event, Mapping):  # pyright: ignore[reportUnnecessaryIsInstance]
        raise TypeError("attend: an event must be a mapping")
    fields: dict[str, Any] = {}
    for key, value in event.items():
        if not isinstance(key, str):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise TypeError("attend: event keys must be strings")
        if key == "data":
            continue
        fields[key] = _to_json(value, 1)
    data = event.get("data", {} if fill is not None else None)
    if not isinstance(data, Mapping):
        raise TypeError("attend: event data must be a JSON object")
    fields["data"] = _to_json(cast(Mapping[str, object], data), 1)
    if fill is not None:
        if "id" not in fields:
            fields["id"] = fill[0]()
        if "at" not in fields:
            fields["at"] = fill[1]()
    if not isinstance(fields.get("id"), str) or not isinstance(fields.get("name"), str) or ("text" in fields and not isinstance(fields["text"], str)):
        raise TypeError("attend: event id, name and text must be strings")
    return cast(Event, fields)


async def copy_window(window: Window) -> Window:
    """Copies a window, 64 events at a time, yielding to the loop between slices."""
    events: list[Entry] = []
    source = window["events"]
    for start in range(0, len(source), SLICE):
        if start:
            await asyncio.sleep(0)
        events.extend(cast(Entry, clone(entry)) for entry in source[start : start + SLICE])
    return cast(Window, {**window, "events": events})


async def copy_decision(decision: Decision) -> Decision:
    copy = cast(Decision, {**decision, "probabilities": dict(decision["probabilities"]), "predicates": dict(decision["predicates"])})
    copy["window"] = await copy_window(decision["window"])
    return copy


async def copy_letter(letter: DeadLetter) -> DeadLetter:
    window = await copy_window(letter["window"])
    decision = None if letter["decision"] is None else await copy_decision(letter["decision"])
    return cast(DeadLetter, {**letter, "window": window, "decision": decision})


def record_bytes(value: object) -> int:
    """Estimates JSON bytes without serializing, on the memory-store path."""
    if value is None:
        return 4
    if isinstance(value, str):
        return utf8_length(value) + 2
    if isinstance(value, bool):
        return 4 if value else 5
    if isinstance(value, (int, float)):
        from .._json import number

        return len(number(value))
    if isinstance(value, Mapping):
        mapping = cast(Mapping[str, object], value)
        return 2 + sum(utf8_length(key) + 4 + record_bytes(item) for key, item in mapping.items())
    if isinstance(value, (list, tuple)):
        items = cast(Sequence[object], value)
        return 2 + sum(record_bytes(item) + 1 for item in items)
    return 0
