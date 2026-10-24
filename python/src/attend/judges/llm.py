"""``attend.judges.llm``: a judge over any model that can reply in JSON matching a schema. A port of ``src/llm.ts``.

There is no SDK dependency: you pass the one structured-output call as ``complete``. With the
Anthropic SDK::

    import json
    from anthropic import AsyncAnthropic
    from attend import PermanentError
    from attend.judges.llm import llm_judge

    anthropic = AsyncAnthropic()

    async def complete(request):
        content = [{"type": "text", "text": request["prompt"]}]
        content += [{"type": "image", "source": {"type": "base64", "media_type": i["mimeType"], "data": i["data"]}} for i in request["images"]]
        res = await request["signal"].race(anthropic.messages.create(
            model="claude-opus-5-5", max_tokens=1024, system=request["system"],
            messages=[{"role": "user", "content": content}],
            output_config={"effort": "low", "format": {"type": "json_schema", "schema": request["schema"]}},
        ))
        if res.stop_reason == "refusal":
            raise PermanentError("judge refused")
        return json.loads(next(b.text for b in res.content if b.type == "text"))

    judge = llm_judge(complete=complete)

With ``context_tokens`` the judge declares a budget, and a rendered window over it loses its oldest
consequence and context events first. A rejection that names the context length is retried once
with the window halved, so let a vendor SDK's error propagate from ``complete`` with its ``status``.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import Any, TypedDict, cast

from .._json import stringify
from ..core.types import ROUTES, JsonValue, JudgeBudget, JudgeImage, JudgeInput, Judgment, OfferedRoute, Route
from ..signal import AbortSignal
from ._defaults import DEFAULT_CRITERIA, DEFAULT_SYSTEM
from ._prompt import Rendered, render_bytes, render_within, resolve_criteria, send_halving, state_text, tokens_of

__all__ = ["DEFAULT_CRITERIA", "DEFAULT_SYSTEM", "CompleteRequest", "LlmJudge", "llm_judge"]


class CompleteRequest(TypedDict):
    system: str
    prompt: str
    images: list[JudgeImage]
    """From the judge input; empty when it carries none. Place them after the prompt."""
    schema: dict[str, JsonValue]
    """JSON Schema for ``{route, probabilities, predicates}``, strict-mode safe."""
    signal: AbortSignal


type Complete = Callable[[CompleteRequest], Awaitable[object]]


def _system_prompt(base: str, routes: list[tuple[Route, str]], asks_predicates: bool) -> str:
    answer = (
        "Answer with the route you choose, a probability from 0 to 1 for each route on offer, "
        "and for each entry under predicates the probability from 0 to 1 that the answer to its question is yes."
        if asks_predicates
        else "Answer with the route you choose and a probability from 0 to 1 for each route on offer."
    )
    return "\n".join([base, "", "Routes on offer:", *(f"- {route}: {criterion}" for route, criterion in routes), "", answer])


def _schema(routes: list[Route], predicates: list[str]) -> dict[str, JsonValue]:
    """No minimum or maximum: Anthropic's structured outputs reject numeric bounds, so the range lives in the system prompt."""

    def numbers(keys: list[str]) -> dict[str, JsonValue]:
        return {"type": "object", "properties": {key: {"type": "number"} for key in keys}, "required": list(keys), "additionalProperties": False}

    properties: dict[str, JsonValue] = {"route": {"type": "string", "enum": list(routes)}, "probabilities": numbers(list(routes))}
    if predicates:
        properties["predicates"] = numbers(predicates)
    return {"type": "object", "properties": properties, "required": list(properties), "additionalProperties": False}


class LlmJudge:
    def __init__(
        self,
        *,
        complete: Complete,
        system: str = DEFAULT_SYSTEM,
        criteria: Mapping[Route, str] | None = None,
        context_tokens: float | None = None,
        render: Callable[[JudgeInput], str] | None = None,
    ) -> None:
        self._complete = complete
        self._system = system
        self._criteria = criteria
        self._render = render
        every: list[OfferedRoute] = [{"route": route} for route in ROUTES]
        reserved = tokens_of(_system_prompt(system, resolve_criteria(every, criteria), True) + stringify(_schema(list(ROUTES), [])))
        if context_tokens is not None and not context_tokens > reserved:
            raise ValueError(f"llmJudge: contextTokens must be more than the {reserved} tokens its own prompt takes, got {context_tokens}")
        if context_tokens is not None:
            self.budget: JudgeBudget = {"contextTokens": context_tokens, "reservedTokens": reserved}

    async def __call__(self, input: JudgeInput, signal: AbortSignal, /) -> Judgment:
        routes = resolve_criteria(input["routes"], self._criteria)
        offered: list[Route] = [route for route, _ in routes]
        asked = [predicate["name"] for predicate in input["predicates"]]
        rendered = await render_within(input, render_bytes(getattr(self, "budget", None)), signal)
        signal.throw_if_aborted()
        render = self._render

        async def send(r: Rendered) -> object:
            return await self._complete(
                {
                    "system": _system_prompt(self._system, routes, len(asked) > 0),
                    "prompt": state_text(r) if render is None else render(r.input),
                    "images": input.get("images", []),
                    "schema": _schema(offered, asked),
                    "signal": signal,
                }
            )

        reply = await send_halving(rendered, send)
        return _parse_reply(reply, offered, asked)


def _describe(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, list):
        return "an array"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    return "object"


def _parse_reply(reply: object, routes: list[Route], predicates: list[str]) -> Judgment:
    """Only the route is load-bearing; ride-alongs keep the finite numbers under known keys."""
    if not isinstance(reply, dict):
        raise Exception(f"llmJudge: expected a JSON object, got {_describe(reply)}")
    found = cast(dict[str, Any], reply)
    route: Route | None = next((r for r in routes if r == found.get("route")), None)
    if route is None:
        shown = "undefined" if "route" not in found else stringify(found["route"])
        raise Exception(f"llmJudge: route {shown} is not one of {', '.join(routes)}")
    return {"route": route, "probabilities": _numbers_at(found.get("probabilities"), routes), "predicates": _numbers_at(found.get("predicates"), predicates)}


def _numbers_at(value: object, keys: list[Route] | list[str]) -> dict[str, float]:
    out: dict[str, float] = {}
    if not isinstance(value, dict):
        return out
    found = cast(dict[str, Any], value)
    for key in keys:
        n = found.get(key)
        if isinstance(n, (int, float)) and not isinstance(n, bool) and n == n and n not in (float("inf"), float("-inf")):
            out[key] = n
    return out


def llm_judge(
    *,
    complete: Complete,
    system: str = DEFAULT_SYSTEM,
    criteria: Mapping[Route, str] | None = None,
    context_tokens: float | None = None,
    render: Callable[[JudgeInput], str] | None = None,
) -> LlmJudge:
    """A judge over one structured-output call. ``complete`` takes a ``CompleteRequest`` and returns the parsed JSON reply;
    raise a ``PermanentError`` on a refusal and let other errors propagate."""
    return LlmJudge(complete=complete, system=system, criteria=criteria, context_tokens=context_tokens, render=render)
