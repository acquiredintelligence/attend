"""``attend.judges.jev``: a judge on TypeSafe's Jev, through ``POST /v1/systemone``. Bring your own TypeSafe key.

One ``choice`` question keyed ``route`` picks among the offered routes, with the criteria as the
option descriptions, and one ``noul`` question answers each predicate, keyed by position
(``predicate_0``, ...) so no predicate name can collide with ``route``. The rendered state is sent as
JSON ``state``. A port of ``src/jev.ts``; needs the ``http`` extra (httpx).

The judge declares a budget of 32k tokens, or 262k with ``long_context``, counting 2 characters per
token. A rendered window over the budget loses its oldest consequence and context events first. A
400 that names ``max_tokens_exceeded`` is retried once with the window halved.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, cast

from .._json import quote, stringify
from ..core.types import ROUTES, JudgeBudget, JudgeImage, JudgeInput, Judgment, OfferedRoute, Route
from ..errors import PermanentError, message_of
from ..signal import AbortSignal
from ._defaults import DEFAULT_SYSTEM
from ._http import Provider, is_context_length_rejection, is_image_rejection, post_json
from ._images import ImageHeader, decoded_length, image_header, is_standard_base64
from ._prompt import Rendered, render_bytes, render_within, resolve_criteria, send_halving, state_text, tokens_of

if TYPE_CHECKING:
    import httpx

_PROVIDER = Provider("jevJudge", "x-typesafe-request-id")
BYTES_PER_JEV_TOKEN = 2
"""Measured: a 42,024-character request with a 300-event state cost 17,423 input tokens."""

JEV_IMAGE_LIMITS = {
    "fileBytes": 16 * 1024 * 1024,
    "allFilesBytes": 32 * 1024 * 1024,
    "minSide": 280,
    "maxSide": 1568,
    "minPixels": 78_400,
    "maxPixels": 2_458_624,
}
"""The provider's documented image limits. Every bound is inclusive."""


class JevJudge:
    """Call it as a judge. ``budget`` is what it declares to ``create_attend``."""

    def __init__(
        self,
        *,
        api_key: str,
        model: str = "jev-latest",
        endpoint: str = "https://api.typesafe.ai/v1/systemone",
        client: httpx.AsyncClient | None = None,
        criteria: Mapping[Route, str] | None = None,
        instructions: str = DEFAULT_SYSTEM,
        long_context: bool = False,
        images: bool = True,
        on_image_error: Callable[[Exception], None] | None = None,
    ) -> None:
        self._api_key = api_key
        self._model = model
        self._endpoint = endpoint
        self._client = client
        self._criteria = criteria
        self._instructions = instructions
        self._long_context = long_context
        self._images = images
        self._on_image_error = on_image_error
        every: list[OfferedRoute] = [{"route": route} for route in ROUTES]
        reserved = stringify({"type": "choice", "instructions": instructions, "criteria": dict(resolve_criteria(every, criteria))})
        self.budget: JudgeBudget = {
            "contextTokens": 262_000 if long_context else 32_000,
            "bytesPerToken": BYTES_PER_JEV_TOKEN,
            "reservedTokens": tokens_of(reserved, BYTES_PER_JEV_TOKEN),
        }

    def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            import httpx

            self._client = httpx.AsyncClient(timeout=None)
        return self._client

    async def __call__(self, input: JudgeInput, signal: AbortSignal, /) -> Judgment:
        names = [predicate["name"] for predicate in input["predicates"]]
        if len(set(names)) != len(names):
            raise PermanentError(f"jevJudge: predicate names must be unique, got {', '.join(names)}")
        routes = resolve_criteria(input["routes"], self._criteria)
        questions: dict[str, Any] = {"route": {"type": "choice", "instructions": self._instructions, "criteria": dict(routes)}}
        for index, predicate in enumerate(input["predicates"]):
            questions[f"predicate_{index}"] = {"type": "noul", "instructions": predicate["question"]}
        rendered = await render_within(input, render_bytes(self.budget), signal)
        assets = [] if not self._images else self._assets(input.get("images", []))
        question_text = stringify(questions)
        refs_text = stringify([ref for ref, _ in assets])
        assets_text = stringify({ref: asset for ref, asset in assets})
        model_text = quote(self._model)
        state = {"with_images": len(assets) > 0}

        async def post(r: Rendered) -> Any:
            headers = {"authorization": f"Bearer {self._api_key}"}
            if self._long_context:
                headers["TypeSafe-Danger-Zone-Long-Context"] = "true"
            text = state_text(r)
            if state["with_images"]:
                headers["TypeSafe-Danger-Zone-Images"] = "true"
                text = f'{text[:-1]},"images":{refs_text}}}'
            body = f'{{"model":{model_text},"state":{text},"questions":{question_text}{f',"assets":{assets_text}' if state["with_images"] else ""}}}'
            return await post_json(_PROVIDER, self._http(), self._endpoint, headers, body, signal)

        async def send(r: Rendered) -> Any:
            try:
                return await post(r)
            except Exception as error:
                if not state["with_images"] or not is_image_rejection(error) or is_context_length_rejection(error):
                    raise
                state["with_images"] = False
                if self._on_image_error is not None:
                    self._on_image_error(Exception(f"jevJudge: the provider rejected the request with its {len(assets)} images, retrying without them: {message_of(error)}"))
                return await post(r)

        reply = await send_halving(rendered, send)
        return _parse_answers(reply, [route for route, _ in routes], names)

    def _assets(self, images: list[JudgeImage]) -> list[tuple[str, dict[str, str]]]:
        """The images within the provider's limits, each under a fresh reference no state text can contain by accident."""
        kept: list[tuple[str, dict[str, str]]] = []
        total = 0
        for index, image in enumerate(images):
            checked = _check_image(image, total)
            if isinstance(checked, str):
                if self._on_image_error is not None:
                    self._on_image_error(Exception(f"jevJudge: image {index} dropped: {checked}"))
                continue
            total += decoded_length(image["data"])
            kept.append((str(uuid.uuid4()), {"type": "image", "format": checked.format, "data_base64": image["data"]}))
        return kept


def _check_image(image: JudgeImage, total: int) -> ImageHeader | str:
    limits = JEV_IMAGE_LIMITS
    mime = image["mimeType"]
    if mime != "image/png" and mime != "image/jpeg":
        return f"{mime} is not accepted, only image/png and image/jpeg"
    if not is_standard_base64(image["data"]):
        return "data is not standard base64 with = padding and no whitespace"
    size = decoded_length(image["data"])
    if size > limits["fileBytes"]:
        return f"{size} bytes is over the {limits['fileBytes']} byte file limit"
    if total + size > limits["allFilesBytes"]:
        return f"{total + size} bytes of images is over the {limits['allFilesBytes']} byte limit for all images"
    header = image_header(image["data"])
    if header is None:
        return "the data is not a PNG or JPEG file with a readable header"
    if header.animated:
        return "animated images are not accepted"
    width, height = header.width, header.height
    if width < limits["minSide"] or height < limits["minSide"]:
        return f"{width}x{height} px has a side under the {limits['minSide']} px minimum"
    if width > limits["maxSide"] or height > limits["maxSide"]:
        return f"{width}x{height} px has a side over the {limits['maxSide']} px maximum"
    pixels = width * height
    if pixels < limits["minPixels"]:
        return f"{width}x{height} px is {pixels} pixels, under the {limits['minPixels']} pixel minimum"
    if pixels > limits["maxPixels"]:
        return f"{width}x{height} px is {pixels} pixels, over the {limits['maxPixels']} pixel maximum"
    return header


def _finite(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value == value and value not in (float("inf"), float("-inf"))


def _parse_answers(body: Any, routes: list[Route], predicates: list[str]) -> Judgment:
    """Only the route is load-bearing; probabilities, predicates and confidence keep what parses."""
    answers: Any = cast(dict[str, Any], body).get("answers") if isinstance(body, dict) else None
    if not isinstance(answers, dict):
        raise Exception("jevJudge: response has no answers map")
    found = cast(dict[str, Any], answers)
    route_answer = found.get("route")
    choice = cast(dict[str, Any], route_answer) if isinstance(route_answer, dict) and cast(dict[str, Any], route_answer).get("type") == "choice" else None
    route: Route | None = next((r for r in routes if choice is not None and r == choice.get("choice")), None)
    if choice is None or route is None:
        raise Exception(f"jevJudge: route {_js(choice.get('choice') if choice is not None else None, choice is None or 'choice' not in choice)} is not one of {', '.join(routes)}")
    probabilities: dict[str, float] = {}
    reported = choice.get("probabilities")
    if isinstance(reported, dict):
        given = cast(dict[str, Any], reported)
        for r in routes:
            if _finite(given.get(r)):
                probabilities[r] = given[r]
    answered: dict[str, float] = {}
    for index, name in enumerate(predicates):
        answer = found.get(f"predicate_{index}")
        if isinstance(answer, dict):
            entry = cast(dict[str, Any], answer)
            if entry.get("type") == "noul" and _finite(entry.get("noul")):
                answered[name] = entry["noul"]
    judgment: Judgment = {"route": route, "probabilities": probabilities, "predicates": answered}
    if _finite(choice.get("confidence")):
        judgment["confidence"] = choice["confidence"]
    return judgment


def _js(value: object, undefined: bool) -> str:
    """``JSON.stringify(value)`` as a template literal prints it: ``undefined`` when absent."""
    return "undefined" if undefined else stringify(value)


def jev_judge(
    *,
    api_key: str,
    model: str = "jev-latest",
    endpoint: str = "https://api.typesafe.ai/v1/systemone",
    client: httpx.AsyncClient | None = None,
    criteria: Mapping[Route, str] | None = None,
    instructions: str = DEFAULT_SYSTEM,
    long_context: bool = False,
    images: bool = True,
    on_image_error: Callable[[Exception], None] | None = None,
) -> JevJudge:
    """A judge on TypeSafe's Jev. Models: ``jev-latest`` (default), ``jev-preview``, ``jev-1.14.0``, ``jev-1.13.0``.

    Raises a retryable error on a network failure, a 408, a 429, a 5xx, a reply without an offered
    route, or a context-length rejection halving did not clear; a ``PermanentError`` on any other
    4xx. Both carry ``status``, ``request_id`` and ``retry_after_ms``. ``client`` is an
    ``httpx.AsyncClient`` to reuse; by default the judge makes its own.
    """
    return JevJudge(
        api_key=api_key,
        model=model,
        endpoint=endpoint,
        client=client,
        criteria=criteria,
        instructions=instructions,
        long_context=long_context,
        images=images,
        on_image_error=on_image_error,
    )
