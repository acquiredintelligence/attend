"""The act-threshold rule, and picking a threshold offline from recorded judgments."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from typing import TypedDict

from .types import ROUTES, Judgment, Route


class Thresholded(TypedDict):
    route: Route
    act: float


class ThresholdCandidate(TypedDict):
    threshold: float
    acts: int
    actsPerHour: float


class ThresholdSuggestion(TypedDict):
    threshold: float | None
    candidates: list[ThresholdCandidate]


def threshold_route(judgment: Judgment, offered: Sequence[Route], act_threshold: float) -> Thresholded | None:
    """The route to stage instead of the judge's, with ``act``, or None when the judge's route stands.

    ``act`` is p(interrupt) + p(ask) from the judgment's own probabilities. At or above the
    threshold, the offered interrupt or ask with the higher probability wins, ties to ask. Below
    it, an acting route is replaced by the likeliest offered quiet route, ties preferring skip,
    note, queue, then wait. A judgment with no usable probability is never changed.
    """
    act = _act_probability(judgment)
    if act is None:
        return None
    if act >= act_threshold:
        choices: tuple[Route, ...] = ("ask", "interrupt")
    elif _acting(judgment["route"]):
        choices = ("skip", "note", "queue", "wait")
    else:
        choices = ()
    probabilities = judgment.get("probabilities")
    route: Route | None = None
    best = -1.0
    for choice in choices:
        p = _probability(probabilities.get(choice)) if isinstance(probabilities, Mapping) else None
        p = 0 if p is None else p
        if choice in offered and p > best:
            route = choice
            best = p
    return None if route is None or route == judgment["route"] else {"route": route, "act": act}


def suggest_threshold(judgments: Sequence[Judgment], *, duration_ms: float, max_acts_per_hour: float) -> ThresholdSuggestion:
    """Picks an ``act_threshold`` from the judgments a replay recorded, with no extra judge calls.

    The candidates are every ``act`` above 0 that the judgments reach, capped at 1, plus 1. A
    judgment with no usable probability acts at every threshold when its own route is interrupt or ask.
    """
    if not _finite(duration_ms) or duration_ms <= 0:
        raise ValueError(f"attend: suggest_threshold needs a positive duration_ms, got {duration_ms}")
    if not _finite(max_acts_per_hour) or max_acts_per_hour < 0:
        raise ValueError(f"attend: suggest_threshold needs a max_acts_per_hour of 0 or more, got {max_acts_per_hour}")
    acts: list[float] = []
    fixed = 0
    for judgment in judgments:
        act = _act_probability(judgment)
        if act is None:
            fixed += 1 if _acting(judgment["route"]) else 0
        elif act > 0:
            acts.append(act)
    acts.sort()
    thresholds = list(dict.fromkeys([*(min(act, 1) for act in acts), 1]))
    below = 0
    candidates: list[ThresholdCandidate] = []
    for threshold in thresholds:
        while below < len(acts) and acts[below] < threshold:
            below += 1
        count = fixed + len(acts) - below
        candidates.append({"threshold": threshold, "acts": count, "actsPerHour": (count * 3_600_000) / duration_ms})
    chosen = next((c["threshold"] for c in candidates if c["actsPerHour"] <= max_acts_per_hour), None)
    return {"threshold": chosen, "candidates": candidates}


def _finite(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _probability(value: object) -> float | None:
    return value if _finite(value) and 0 <= value <= 1 else None  # type: ignore[operator, return-value]


def _acting(route: object) -> bool:
    return route == "interrupt" or route == "ask"


def _js_round(value: float) -> float:
    """``Math.round``: the nearest integer, halves toward +inf."""
    floor = math.floor(value)
    return floor + 1 if value - floor >= 0.5 else floor


def _act_probability(judgment: Judgment) -> float | None:
    probabilities = judgment.get("probabilities")
    if not isinstance(probabilities, Mapping) or not any(_probability(probabilities.get(route)) is not None for route in ROUTES):
        return None
    interrupt = _probability(probabilities.get("interrupt")) or 0
    ask = _probability(probabilities.get("ask")) or 0
    # Decimal sums such as 0.7 + 0.1 must meet an exact 0.8 threshold.
    return _js_round((interrupt + ask) * 1e12) / 1e12
