"""The text an agent hears for a decision."""

from __future__ import annotations

from collections.abc import Sequence

from ._json import number, stringify
from .core.types import Decision, DeliveredRoute, Entry
from .fold import EventFold, fold_events

ROUTE_HINT: dict[DeliveredRoute, str] = {
    "interrupt": "Attend to this now, even mid-task.",
    "queue": "Take this up when the current step is done.",
    "note": "Background only. No reply is needed.",
    "ask": "The user was offered a way to call you in. Stay quiet unless they do.",
}


def default_notice(decision: Decision, *, max_lines: int = 20, fold: bool = True) -> str:
    """The window's event lines oldest first, what was left out, and one line on the route.

    A run of events that read the same becomes one line with its count. A run of ``continuous`` events under one name
    is the steps of one act, so it becomes its first and last step with the count. ``fold=False`` gives one line per event.
    """
    window = decision["window"]
    lines = _notice_lines(window["events"], fold)
    shown = lines[-max_lines:]
    older = sum(events for _, events in lines[: len(lines) - len(shown)])
    out: list[str] = []
    if older > 0:
        out.append(f"({number(older)} older events not shown)")
    if window["gap"]:
        out.append("Some events before these were lost.")
    out.extend(text for text, _ in shown)
    if window["dropped"] > 0:
        out.append(f"({number(window['dropped'])} background events dropped from this window)")
    out.append(ROUTE_HINT[decision["route"]])
    return "\n".join(out)


def _notice_lines(entries: Sequence[Entry], fold: bool) -> list[tuple[str, int]]:
    """The notice's event lines, each with how many events it stands for. Context entries are left out."""
    groups = fold_events(
        [entry for entry in entries if entry["kind"] != "context"],
        repeats=lambda first, next: _event_line(first) == _event_line(next),
        continues=lambda first, next: first["kind"] == "continuous" and next["kind"] == "continuous" and first["name"] == next["name"],
        fold=fold,
    )
    return [(_run_line(group), group.count) for group in groups]


def _event_line(entry: Entry) -> str:
    return entry["text"] if "text" in entry else f"{entry['name']} {stringify(entry['data'])}"


def _run_line(run: EventFold[Entry]) -> str:
    first, last = run.first, run.last
    line = _event_line(first)
    if run.mode == "single":
        return line
    if run.mode == "repeat":
        return f"{line} ({number(run.count)} times)"
    end = stringify(last["data"]) if "text" not in first and "text" not in last else _event_line(last)
    return f"{line} → {end} ({number(run.count)} steps)"
