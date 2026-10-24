"""``aqi_attend.judges.decisions``: a judge on the OpenAI Decisions API. A port of ``src/decisions.ts``; needs the ``http`` extra.

One ``choice`` question named ``route`` picks among the offered routes, with the criteria as the
choice descriptions, and one ``predicate`` question answers each predicate, named by position so no
predicate name can collide with ``route``. Images go as ``input_image`` data URLs after the text, at
most 128; a request the API rejects over its images is retried once without them.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, cast

from .._json import stringify
from ..core.types import ROUTES, JudgeBudget, JudgeImage, JudgeInput, Judgment, OfferedRoute, Route
from ..errors import PermanentError, message_of
from ..signal import AbortSignal
from ._defaults import DEFAULT_SYSTEM
from ._http import Provider, is_context_length_rejection, is_image_rejection, post_json
from ._prompt import Rendered, render_bytes, render_within, resolve_criteria, send_halving, state_text, tokens_of

if TYPE_CHECKING:
    import httpx

_PROVIDER = Provider("decisionsJudge", "x-request-id")
MAX_QUESTIONS = 200
MAX_IMAGES = 128


class DecisionsJudge:
    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        endpoint: str = "https://api.openai.com/v1/decisions",
        client: httpx.AsyncClient | None = None,
        context_tokens: float | None = None,
        criteria: Mapping[Route, str] | None = None,
        instructions: str = DEFAULT_SYSTEM,
        images: bool = True,
        on_image_error: Callable[[Exception], None] | None = None,
    ) -> None:
        self._api_key = api_key
        self._model = model
        self._endpoint = endpoint
        self._client = client
        self._criteria = criteria
        self._instructions = instructions
        self._images = images
        self._on_image_error = on_image_error
        every: list[OfferedRoute] = [{"route": route} for route in ROUTES]
        reserved = tokens_of(
            stringify(
                {
                    "type": "choice",
                    "name": "route",
                    "instructions": instructions,
                    "choices": [{"value": route, "description": criterion} for route, criterion in resolve_criteria(every, criteria)],
                }
            )
        )
        if context_tokens is not None and not context_tokens > reserved:
            raise ValueError(f"decisionsJudge: contextTokens must be more than the {reserved} tokens its own prompt takes, got {context_tokens}")
        if context_tokens is not None:
            self.budget: JudgeBudget = {"contextTokens": context_tokens, "reservedTokens": reserved}

    def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            import httpx

            self._client = httpx.AsyncClient(timeout=None)
        return self._client

    async def __call__(self, input: JudgeInput, signal: AbortSignal, /) -> Judgment:
        names = [predicate["name"] for predicate in input["predicates"]]
        if len(set(names)) != len(names):
            raise PermanentError(f"decisionsJudge: predicate names must be unique, got {', '.join(names)}")
        if len(names) + 1 > MAX_QUESTIONS:
            raise PermanentError(f"decisionsJudge: {len(names)} predicates plus the route is over the {MAX_QUESTIONS} questions the API takes")
        routes = resolve_criteria(input["routes"], self._criteria)
        rendered = await render_within(input, render_bytes(getattr(self, "budget", None)), signal)
        images = [] if not self._images else self._keep(input.get("images", []))
        questions: list[dict[str, Any]] = [
            {"type": "choice", "name": "route", "instructions": self._instructions, "choices": [{"value": route, "description": criterion} for route, criterion in routes]},
            *({"type": "predicate", "name": f"predicate_{index}", "instructions": predicate["question"]} for index, predicate in enumerate(input["predicates"])),
        ]
        state = {"with_images": len(images) > 0}

        async def post(r: Rendered) -> Any:
            text = state_text(r)
            parts = [{"type": "input_image", "image_url": f"data:{image['mimeType']};base64,{image['data']}"} for image in images]
            body = {
                "model": self._model,
                "input": [{"role": "user", "content": [{"type": "input_text", "text": text}, *parts]}] if state["with_images"] else text,
                "questions": questions,
            }
            return await post_json(_PROVIDER, self._http(), self._endpoint, {"authorization": f"Bearer {self._api_key}"}, stringify(body), signal)

        async def send(r: Rendered) -> Any:
            try:
                return await post(r)
            except Exception as error:
                if not state["with_images"] or not is_image_rejection(error) or is_context_length_rejection(error):
                    raise
                state["with_images"] = False
                if self._on_image_error is not None:
                    self._on_image_error(Exception(f"decisionsJudge: the provider rejected the request with its {len(images)} images, retrying without them: {message_of(error)}"))
                return await post(r)

        reply = await send_halving(rendered, send)
        return _parse_answers(reply, [route for route, _ in routes], names)

    def _keep(self, images: list[JudgeImage]) -> list[JudgeImage]:
        if self._on_image_error is not None:
            for index in range(MAX_IMAGES, len(images)):
                self._on_image_error(Exception(f"decisionsJudge: image {index} dropped: the API takes at most {MAX_IMAGES} images per request"))
        return images[:MAX_IMAGES]


def _finite(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value == value and value not in (float("inf"), float("-inf"))


def _parse_answers(body: Any, routes: list[Route], predicates: list[str]) -> Judgment:
    """Only the route is load-bearing. A refusal of the route question is permanent; a refused predicate is left unanswered."""
    raw: Any = cast(dict[str, Any], body).get("answers") if isinstance(body, dict) else None
    if not isinstance(raw, list):
        raise Exception("decisionsJudge: response has no answers array")
    answers = [cast(dict[str, Any], a) for a in cast(list[Any], raw) if isinstance(a, dict)]
    if any(a.get("type") == "refusal" and a.get("name") == "route" for a in answers):
        raise PermanentError("decisionsJudge: the model refused to answer the route question")
    choice = next((a for a in answers if a.get("type") == "choice" and a.get("name") == "route"), None)
    route: Route | None = next((r for r in routes if choice is not None and r == choice.get("choice")), None)
    if choice is None or route is None:
        shown = "undefined" if choice is None or "choice" not in choice else stringify(choice["choice"])
        raise Exception(f"decisionsJudge: route {shown} is not one of {', '.join(routes)}")
    probabilities: dict[str, float] = {}
    entries = choice.get("probabilities")
    for item in cast(list[Any], entries) if isinstance(entries, list) else []:
        if not isinstance(item, dict):
            continue
        entry = cast(dict[str, Any], item)
        value = next((r for r in routes if r == entry.get("value")), None)
        if value is not None and _finite(entry.get("probability")):
            probabilities[value] = entry["probability"]
    answered: dict[str, float] = {}
    for index, name in enumerate(predicates):
        answer = next((a for a in answers if a.get("type") == "predicate" and a.get("name") == f"predicate_{index}"), None)
        if answer is not None and _finite(answer.get("probability")):
            answered[name] = answer["probability"]
    judgment: Judgment = {"route": route, "probabilities": probabilities, "predicates": answered}
    if _finite(choice.get("confidence")):
        judgment["confidence"] = choice["confidence"]
    return judgment


def decisions_judge(
    *,
    api_key: str,
    model: str,
    endpoint: str = "https://api.openai.com/v1/decisions",
    client: httpx.AsyncClient | None = None,
    context_tokens: float | None = None,
    criteria: Mapping[Route, str] | None = None,
    instructions: str = DEFAULT_SYSTEM,
    images: bool = True,
    on_image_error: Callable[[Exception], None] | None = None,
) -> DecisionsJudge:
    """A judge on the OpenAI Decisions API. ``model`` is required: attend does not choose one for you.

    With ``context_tokens`` it declares a budget whose ``reservedTokens`` is measured from the
    route question with every route on offer.
    """
    return DecisionsJudge(
        api_key=api_key,
        model=model,
        endpoint=endpoint,
        client=client,
        context_tokens=context_tokens,
        criteria=criteria,
        instructions=instructions,
        images=images,
        on_image_error=on_image_error,
    )
