"""The pure reducer for tests, evals and replays."""

from __future__ import annotations

from .reducer import apply
from .state import from_record, to_record
from .types import ApplyConfig, ApplyOutput, Input, ScopeRecord, StepConfig, Stepped


def step(config: StepConfig, record: ScopeRecord, input: Input, now: float) -> Stepped:
    """``from_record``, then ``apply``, then ``to_record``. Never mutates ``record``.

    O(W + S) per call, so a runtime that owns its state calls ``apply``.
    """
    policy = config["policy"]
    state = from_record(record, policy)
    out = ApplyOutput()
    apply(ApplyConfig(policy, config.get("decisions", "lane"), config.get("deadLetters", "lane"), True), state, {"input": input, "now": now}, out)
    return {"record": to_record(state), "effects": out.effects}


def initial_record(scope: str) -> ScopeRecord:
    """The empty snapshot at version zero."""
    return {
        "version": 0,
        "scope": scope,
        "cursors": {},
        "seen": [],
        "buffer": {"entries": [], "dropped": 0, "gap": False, "forcedAt": None},
        "batch": None,
        "outbox": {"decisions": [], "deadLetters": []},
        "recent": [],
        "dueAt": None,
    }
