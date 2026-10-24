"""The runtime half of the shared conformance suite: every scenario in ``conformance/fixtures/scenarios.json``.

Everything a scenario observes is compared exactly, traces included, with one exception. When a
judge call and a delivery for one scope are in flight at once, which result commits first depends on
scheduling, in either language and with any real store. For the scenarios listed in ``REORDERED``,
traces are compared per lane instead: the scope's own lane (intake, settling and judging), its
decisions lane and its dead-letter lane, each in order. Those scenarios must really differ in raw
order, so the list cannot outlive the reason for it.
"""

from __future__ import annotations

import re
from typing import Any

import pytest

from .scenario import run_scenario
from .test_conformance_core import load

SCENARIOS: list[dict[str, Any]] = load("scenarios")["scenarios"]
DELIVERY_INPUTS = {"delivered", "deliveryFailed", "deliveryReleased"}
STALE_DELIVERY = re.compile(r"no live delivery attempt -?[0-9]+ for (.*)")

REORDERED = {
    # The release of the held judge call commits its decision, then the next window's judge call and that decision's delivery race.
    "a direct address that lands while the judge is busy is judged as flush with no wait",
    "attend.flush() while the judge is busy settles the act whose deadline passed as forced with no wait",
}


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
            found = STALE_DELIVERY.fullmatch(trace["why"])
            lane = delivery_lane(found.group(1)) if found else "decisions"
        elif kind == "deadLettered":
            lane = "decisions" if trace["letter"]["stage"] == "sink" else "scope"
            lettered.add(trace["letter"]["id"])
        else:
            lane = "scope"
        out[lane].append(trace)
    return out


def test_every_reordered_scenario_exists() -> None:
    assert {s["scenario"]["name"] for s in SCENARIOS} >= REORDERED


@pytest.mark.parametrize("recorded", SCENARIOS, ids=[s["scenario"]["name"] for s in SCENARIOS])
async def test_scenario(recorded: dict[str, Any]) -> None:
    observed = await run_scenario(recorded["scenario"])
    expected = recorded["expected"]
    for key in ("results", "judgeCalls", "deliveries", "deadLetterCalls", "inspections", "records", "log"):
        assert observed[key] == expected[key], key
    if recorded["scenario"]["name"] not in REORDERED:
        assert observed["traces"] == expected["traces"]
        return
    assert observed["traces"] != expected["traces"], "this scenario now matches in raw order: remove it from REORDERED"
    assert sorted(observed["traces"]) == sorted(expected["traces"])
    for scope, traces in expected["traces"].items():
        assert lanes(observed["traces"][scope]) == lanes(traces), scope
