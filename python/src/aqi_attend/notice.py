"""The text an agent hears for a decision."""

from __future__ import annotations

from collections.abc import Sequence

from ._json import number, stringify
from .core.types import Decision, DeliveredRoute, Entry

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
    out: list[tuple[str, int]] = []
    run: list[Entry] = []
    # A run holds while every event reads the same (``same``), or while every event is a step of one act (``steps``).
    same = True
    steps = True
    for entry in entries:
        if entry["kind"] == "context":
            continue
        reads = bool(run) and _event_line(entry) == _event_line(run[0])
        step = bool(run) and entry["kind"] == "continuous" and entry["name"] == run[0]["name"]
        if fold and run and ((same and reads) or (steps and step)):
            same = same and reads
            steps = steps and step
        else:
            if run:
                out.append((_run_line(run, same), len(run)))
            run = []
            same = True
            steps = entry["kind"] == "continuous"
        run.append(entry)
    if run:
        out.append((_run_line(run, same), len(run)))
    return out


def _event_line(entry: Entry) -> str:
    return entry["text"] if "text" in entry else f"{entry['name']} {stringify(entry['data'])}"


def _run_line(run: Sequence[Entry], same: bool) -> str:
    first, last = run[0], run[-1]
    line = _event_line(first)
    if len(run) == 1:
        return line
    if same:
        return f"{line} ({number(len(run))} times)"
    end = stringify(last["data"]) if "text" not in first and "text" not in last else _event_line(last)
    return f"{line} → {end} ({number(len(run))} steps)"
