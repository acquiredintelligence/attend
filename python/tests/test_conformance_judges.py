"""The hosted-judge half of the shared conformance suite: every exchange in ``conformance/fixtures/judges.json``.

A fake transport answers each request in turn, and the test records the request exactly as sent:
the URL, the headers the judge set (httpx's own defaults left out) and the body text.
"""

from __future__ import annotations

import json
import re
from typing import Any

import httpx
import pytest

from attend import AbortController, is_permanent
from attend.errors import message_of
from attend.judges import decisions_judge, jev_judge, llm_judge
from attend.judges.llm import CompleteRequest

from .scenario import plain
from .test_conformance_core import load

CASES: list[dict[str, Any]] = load("judges")["cases"]
UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}")
HTTPX_DEFAULTS = {"host", "accept", "accept-encoding", "connection", "user-agent", "content-length"}
OPTIONS = {
    "criteria": "criteria",
    "instructions": "instructions",
    "longContext": "long_context",
    "model": "model",
    "endpoint": "endpoint",
    "images": "images",
    "contextTokens": "context_tokens",
    "system": "system",
}


def number_uuids(texts: list[str]) -> list[str]:
    seen: dict[str, str] = {}

    def name(match: re.Match[str]) -> str:
        return seen.setdefault(match.group(0), f"<uuid{len(seen)}>")

    return [UUID.sub(name, text) for text in texts]


class Thrown(Exception):
    def __init__(self, message: str, status: int | None, code: str | None, permanent: bool) -> None:
        super().__init__(message)
        if status is not None:
            self.status = status
        if code is not None:
            self.code = code
        if permanent:
            self.permanent = True


async def run_case(case: dict[str, Any]) -> dict[str, Any]:
    requests: list[dict[str, Any]] = []
    image_errors: list[str] = []
    calls = {"n": 0}

    def reply() -> dict[str, Any]:
        replies = case["replies"]
        r = replies[min(calls["n"], len(replies) - 1)]
        calls["n"] += 1
        return r

    def handler(request: httpx.Request) -> httpx.Response:
        headers = {name.lower(): value for name, value in request.headers.items() if name.lower() not in HTTPX_DEFAULTS}
        requests.append({"url": str(request.url), "headers": headers, "body": request.content.decode("utf-8")})
        r = reply()
        if "networkError" in r:
            raise httpx.ConnectError(r["networkError"])
        return httpx.Response(r["status"], content=r["body"].encode("utf-8"), headers=r.get("headers", {}))

    options: dict[str, Any] = {OPTIONS[key]: value for key, value in case["options"].items()}
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    def on_image_error(error: Exception) -> None:
        image_errors.append(message_of(error))

    async def complete(request: CompleteRequest) -> object:
        requests.append(plain({"system": request["system"], "prompt": request["prompt"], "images": request["images"], "schema": request["schema"]}))
        r = reply()
        if "value" in r:
            return r["value"]
        thrown = r["throws"]
        raise Thrown(thrown["message"], thrown.get("status"), thrown.get("code"), bool(thrown.get("permanent")))

    if case["judge"] == "jev":
        judge: Any = jev_judge(api_key="k", client=client, on_image_error=on_image_error, **options)
    elif case["judge"] == "decisions":
        settings: dict[str, Any] = {"model": "m", **options}
        judge = decisions_judge(api_key="k", client=client, on_image_error=on_image_error, **settings)
    else:
        judge = llm_judge(complete=complete, **options)
    try:
        result: dict[str, Any] = {"judgment": plain(await judge(case["input"], AbortController().signal))}
    except Exception as error:
        result = {
            "error": {
                "name": "PermanentError" if type(error).__name__ in ("PermanentError", "PermanentProviderError") else "Error",
                "message": message_of(error),
                "permanent": is_permanent(error),
                "status": getattr(error, "status", None),
                "requestId": getattr(error, "request_id", None),
                "retryAfterMs": getattr(error, "retry_after_ms", None),
            }
        }
    finally:
        await client.aclose()
    bodies = number_uuids([r.get("body", "") for r in requests])
    return {
        "budget": plain(getattr(judge, "budget", None)),
        "requests": [r if "url" not in r else {"url": r["url"], "headers": r["headers"], "body": bodies[i]} for i, r in enumerate(requests)],
        "result": result,
        "imageErrors": image_errors,
    }


@pytest.mark.parametrize("case", CASES, ids=[c["name"] for c in CASES])
async def test_judge_exchange(case: dict[str, Any]) -> None:
    observed = await run_case(case)
    expected = case["expected"]
    assert observed["result"] == expected["result"]
    assert observed["imageErrors"] == expected["imageErrors"]
    assert observed["budget"] == expected["budget"]
    assert len(observed["requests"]) == len(expected["requests"])
    for got, want in zip(observed["requests"], expected["requests"], strict=True):
        if "body" in want:
            assert got["url"] == want["url"]
            assert got["headers"] == want["headers"]
            assert got["body"] == want["body"], "request bodies must match byte for byte"
            json.loads(got["body"])
        else:
            assert got == want


@pytest.mark.parametrize("which", ["jev", "decisions"])
async def test_an_abort_cancels_the_request_and_raises_the_signal_reason(which: str) -> None:
    import asyncio

    started = asyncio.Event()
    cancelled: list[bool] = []

    async def hang(request: httpx.Request) -> httpx.Response:
        started.set()
        try:
            await asyncio.get_running_loop().create_future()
        except asyncio.CancelledError:
            cancelled.append(True)
            raise
        raise AssertionError("unreachable")

    client = httpx.AsyncClient(transport=httpx.MockTransport(hang))
    judge: Any = jev_judge(api_key="k", client=client) if which == "jev" else decisions_judge(api_key="k", model="m", client=client)
    controller = AbortController()
    call = asyncio.ensure_future(judge(CASES[0]["input"], controller.signal))
    await started.wait()
    reason = Exception("attend: judge timed out")
    controller.abort(reason)
    with pytest.raises(Exception, match="attend: judge timed out") as raised:
        await call
    assert raised.value is reason
    assert cancelled == [True]
    await client.aclose()
