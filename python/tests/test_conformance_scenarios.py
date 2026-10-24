"""The runtime half of the shared conformance suite: every scenario in ``conformance/fixtures/scenarios.json``.

A judge call and a delivery for one scope run at the same time, so which of their results commits
first depends on scheduling, in either language and with any real store. Traces are therefore
compared per lane: the scope's own lane (intake, settling and judging), its decisions lane and
its dead-letter lane, each in order. Everything else is compared exactly.
"""

from __future__ import annotations

from typing import Any

import pytest

from .scenario import run_scenario
from .test_conformance_core import load

SCENARIOS: list[dict[str, Any]] = load("scenarios")["scenarios"]
DELIVERY_INPUTS = {"delivered", "deliveryFailed", "deliveryReleased"}


def lanes(traces: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Splits one scope's traces into its three lanes, keeping their order."""
    out: dict[str, list[dict[str, Any]]] = {"scope": [], "decisions": [], "deadLetters": []}
    lettered: set[str] = set()

    def delivery_lane(id: str) -> str:
        return "deadLetters" if id in lettered else "decisions"

    for trace in traces:
        kind = trace["kind"]
        stage = trace.get("stage")
        if kind in ("delivering", "delivered"):
            lane = delivery_lane(trace["deliveryId"])
        elif kind in ("retrying", "released") and stage in ("sink", "deadLetter"):
            lane = "decisions" if stage == "sink" else "deadLetters"
        elif kind == "stale" and trace["input"] in DELIVERY_INPUTS:
            lane = next((delivery_lane(id) for id in lettered if id in trace["why"]), "decisions")
        elif kind == "deadLettered":
            lane = "decisions" if trace["letter"]["stage"] == "sink" else "scope"
            lettered.add(trace["letter"]["id"])
        else:
            lane = "scope"
        out[lane].append(trace)
    return out


@pytest.mark.parametrize("recorded", SCENARIOS, ids=[s["scenario"]["name"] for s in SCENARIOS])
async def test_scenario(recorded: dict[str, Any]) -> None:
    observed = await run_scenario(recorded["scenario"])
    expected = recorded["expected"]
    for key in ("results", "judgeCalls", "deliveries", "deadLetterCalls", "inspections", "records", "log"):
        assert observed[key] == expected[key], key
    assert sorted(observed["traces"]) == sorted(expected["traces"])
    for scope, traces in expected["traces"].items():
        assert lanes(observed["traces"][scope]) == lanes(traces), scope
