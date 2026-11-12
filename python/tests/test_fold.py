"""Both runtimes fold the same neutral host values without changing them."""

import json
from pathlib import Path
from typing import Any

import pytest

from aqi_attend import fold_events

CASES = json.loads((Path(__file__).parents[2] / "conformance/fixtures/fold.json").read_text())


@pytest.mark.parametrize("case", CASES, ids=[c["name"] for c in CASES])
def test_fold(case: dict[str, Any]) -> None:
    items: list[dict[str, Any]] = case["items"]
    before = json.dumps(items)
    groups = fold_events(
        items,
        repeats=lambda a, b: a["group"] == b["group"] and a["value"] == b["value"],
        continues=lambda a, b: a["continuous"] and b["continuous"] and a["group"] == b["group"],
        fold=case.get("fold", True),
    )
    assert [{"first": g.first["id"], "last": g.last["id"], "count": g.count, "mode": g.mode} for g in groups] == case["expected"]
    assert sum(g.count for g in groups) == len(items)
    assert json.dumps(items) == before
    for group in groups:
        assert any(group.first is item for item in items)
        assert any(group.last is item for item in items)
