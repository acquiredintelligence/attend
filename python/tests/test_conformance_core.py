"""The pure half of the shared conformance suite: the same JSON fixtures the TypeScript runner checks."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from aqi_attend._json import stable_json, utf8_length
from aqi_attend.core.ids import window_id
from aqi_attend.core.reducer import validate
from aqi_attend.core.step import initial_record, step
from aqi_attend.core.threshold import suggest_threshold, threshold_route
from aqi_attend.notice import default_notice

FIXTURES = Path(__file__).resolve().parents[2] / "conformance" / "fixtures"


def load(name: str) -> Any:
    return json.loads((FIXTURES / f"{name}.json").read_text("utf-8"))


def digest(value: Any) -> str:
    return hashlib.sha256(stable_json(value).encode("utf-8")).hexdigest()[:16]


def plain(value: Any) -> Any:
    """What a JSON round trip makes of ``value``, as the TypeScript runner compares it."""
    return json.loads(stable_json(value))


@pytest.mark.parametrize("seed", [101, 202, 303])
def test_reducer_traces(seed: int) -> None:
    plans = load(f"step-{seed}")["plans"]
    steps = 0
    for plan in plans:
        record = initial_record(plan["scope"])
        for index, expected in enumerate(plan["steps"]):
            after = step(plan["config"], record, expected["input"], expected["now"])
            record = after["record"]
            where = f"{plan['name']} step {index} ({expected['input']['kind']})"
            assert plain(after["effects"]) == expected["effects"], where
            if "record" in expected:
                assert plain(record) == expected["record"], where
            assert digest(record) == expected["digest"], where
            steps += 1
    assert steps > 1000


def test_window_ids_and_utf8_lengths() -> None:
    fixture = load("ids")
    for case in fixture["cases"]:
        assert window_id(case["scope"], case["ids"]) == case["id"], case
    for case in fixture["utf8"]:
        assert utf8_length(case["text"]) == case["length"], case


def test_threshold_validate_and_suggest() -> None:
    fixture = load("threshold")
    for case in fixture["thresholdRoute"]:
        assert plain(threshold_route(case["judgment"], case["offered"], case["threshold"])) == case["result"], case
    for case in fixture["validate"]:
        assert plain(validate(case["judgment"], case["offered"])) == case["result"], case
    for case in fixture["suggestThreshold"]:
        options = case["options"]
        result = suggest_threshold(case["judgments"], duration_ms=options["durationMs"], max_acts_per_hour=options["maxActsPerHour"])
        assert plain(result) == case["result"], case


def test_default_notice() -> None:
    for case in load("notice")["cases"]:
        options = case.get("options")
        text = default_notice(case["decision"], max_lines=options["maxLines"]) if options else default_notice(case["decision"])
        assert text == case["text"]
