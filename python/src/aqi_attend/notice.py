"""The text an agent hears for a decision."""

from __future__ import annotations

from ._json import number, stringify
from .core.types import Decision, DeliveredRoute

ROUTE_HINT: dict[DeliveredRoute, str] = {
    "interrupt": "Attend to this now, even mid-task.",
    "queue": "Take this up when the current step is done.",
    "note": "Background only. No reply is needed.",
    "ask": "The user was offered a way to call you in. Stay quiet unless they do.",
}


def default_notice(decision: Decision, *, max_lines: int = 20) -> str:
    """The window's event lines oldest first, what was left out, and one line on the route."""
    window = decision["window"]
    lines = [entry["text"] if "text" in entry else f"{entry['name']} {stringify(entry['data'])}" for entry in window["events"] if entry["kind"] != "context"]
    shown = lines[-max_lines:]
    out: list[str] = []
    if len(lines) > len(shown):
        out.append(f"({number(len(lines) - len(shown))} older events not shown)")
    if window["gap"]:
        out.append("Some events before these were lost.")
    out.extend(shown)
    if window["dropped"] > 0:
        out.append(f"({number(window['dropped'])} background events dropped from this window)")
    out.append(ROUTE_HINT[decision["route"]])
    return "\n".join(out)
