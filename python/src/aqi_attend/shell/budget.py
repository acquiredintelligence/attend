"""The open window's byte limit from a judge's declared budget."""

from __future__ import annotations

import math

from ..core.types import JudgeBudget


def window_bytes_for(budget: JudgeBudget, window_share: float) -> float:
    """(contextTokens - reservedTokens) * windowShare * bytesPerToken, floored."""
    value = (budget["contextTokens"] - budget.get("reservedTokens", 0)) * window_share * budget.get("bytesPerToken", 4)
    return math.floor(value) if math.isfinite(value) else value
