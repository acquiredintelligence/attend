"""The store conformance suites over every store, as ``test/conformance.test.ts`` runs them over the TypeScript stores."""

from __future__ import annotations

import pytest

from aqi_attend.conformance import decision_log_conformance, scope_store_conformance
from aqi_attend.testing import Case

from .stores import PAIRS, STORES

PAIRED = dict(PAIRS)
CASES: list[tuple[str, Case]] = [(name, case) for name, make in STORES for case in [*scope_store_conformance(make), *decision_log_conformance(make, concurrent=PAIRED.get(name))]]


@pytest.mark.parametrize(("store", "case"), CASES, ids=[f"{name}: {case.name}" for name, case in CASES])
async def test_store_conformance(store: str, case: Case) -> None:
    await case.run()
