"""What the hosted judges share at the HTTP boundary: one POST, one table from status to retryable or permanent,
the provider's error body parsed defensively, and the request id and ``Retry-After`` carried on the raised error."""

from __future__ import annotations

import datetime
import email.utils
import json
import re
import time
from typing import TYPE_CHECKING, Any, cast

from ..errors import PermanentError, message_of
from ..signal import AbortSignal

if TYPE_CHECKING:
    import httpx

QUOTA_CODES = frozenset(
    {"credit_balance_exhausted", "organization_spend_limit_exceeded", "project_spend_limit_exceeded", "organization_usage_limit_exceeded", "insufficient_quota"}
)
"""429 codes that a retry never clears: billing and quota."""

CONTEXT_LENGTH = re.compile(r"context[ _-]?(length|window)|max_tokens_exceeded|prompt is too long|input token count|too many (input )?tokens|token limit", re.I)
"""How OpenAI (the Decisions API's "exceeds the token limit" included), Anthropic, Gemini, TypeSafe and OpenRouter word a request too long for the model."""

IMAGE_REJECTIONS = frozenset({400, 413, 415, 422})
"""Statuses a provider returns when it rejects an image, when the request carried any."""


class ProviderError(Exception):
    """An error a judge raised for a non-2xx response, or a network failure. Retryable unless it is a ``PermanentProviderError``."""

    def __init__(self, message: str, *, status: int | None = None, request_id: str | None = None, retry_after_ms: float | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.request_id = request_id
        self.retry_after_ms = retry_after_ms
        self.context_length = False
        self.from_status = False


class PermanentProviderError(ProviderError, PermanentError):
    """A non-2xx response that a retry would not clear."""


class Provider:
    __slots__ = ("name", "request_id_header")

    def __init__(self, name: str, request_id_header: str) -> None:
        self.name = name
        self.request_id_header = request_id_header


async def post_json(provider: Provider, client: httpx.AsyncClient, endpoint: str, headers: dict[str, str], body: str, signal: AbortSignal) -> Any:
    """POSTs ``body`` as JSON and returns the parsed reply. A network failure is retryable unless ``signal`` aborted."""
    signal.throw_if_aborted()
    try:
        response = await signal.race(client.post(endpoint, content=body.encode("utf-8"), headers={"content-type": "application/json", **headers}))
    except Exception as error:
        signal.throw_if_aborted()
        raise ProviderError(f"{provider.name}: request failed: {message_of(error)}") from error
    if not 200 <= response.status_code < 300:
        try:
            text = response.text
        except Exception:
            text = ""
        raise status_error(provider, response.status_code, response.headers, text)
    try:
        return json.loads(response.content)
    except Exception as error:
        raise ProviderError(f"{provider.name}: response is not JSON") from error


def status_error(provider: Provider, status: int, headers: httpx.Headers | dict[str, str], text: str) -> ProviderError:
    """Retryable: 408, 429 except the quota codes, and every 5xx. Permanent: everything else."""
    message, code = parse_error_body(text)
    request_id = _header(headers, provider.request_id_header)
    retryable = status == 408 or status >= 500 or (status == 429 and (code or "") not in QUOTA_CODES)
    described = f"{provider.name}: HTTP {status}{'' if message == '' else f': {message}'}{'' if request_id is None else f' (request {request_id})'}"
    kind = ProviderError if retryable else PermanentProviderError
    error = kind(described, status=status, request_id=request_id, retry_after_ms=retry_after_ms(headers))
    error.from_status = True
    error.context_length = status in (400, 413) and CONTEXT_LENGTH.search(text) is not None
    return error


def _header(headers: httpx.Headers | dict[str, str], name: str) -> str | None:
    lower = name.lower()
    for key, value in headers.items():
        if key.lower() == lower:
            return value
    return None


def is_image_rejection(error: BaseException) -> bool:
    status = getattr(error, "status", None)
    return isinstance(status, int) and not isinstance(status, bool) and status in IMAGE_REJECTIONS


def is_context_length_rejection(error: BaseException) -> bool:
    """True for a 400 or 413 that names the model's context length, in the response text or in an SDK error's message or code."""
    status = getattr(error, "status", None)
    if status not in (400, 413):
        return False
    if isinstance(error, ProviderError) and error.from_status:
        return error.context_length
    code = getattr(error, "code", None)
    return CONTEXT_LENGTH.search(message_of(error)) is not None or (isinstance(code, str) and CONTEXT_LENGTH.search(code) is not None)


def parse_error_body(text: str) -> tuple[str, str | None]:
    """The provider's own description from any of the documented body shapes; else the raw text, truncated."""
    try:
        body: Any = json.loads(text)
    except Exception:
        return _slice(text), None
    if not isinstance(body, dict):
        return _slice(text), None
    record = cast(dict[str, Any], body)
    error = record.get("error")
    if isinstance(error, str):
        return error, None
    if isinstance(error, dict):
        found = cast(dict[str, Any], error)
        code = found.get("code") if isinstance(found.get("code"), str) else found.get("type") if isinstance(found.get("type"), str) else None
        message = found.get("message")
        return (message if isinstance(message, str) else _slice(text)), code
    if isinstance(record.get("message"), str):
        return record["message"], None
    detail = record.get("detail")
    if isinstance(detail, str):
        return detail, None
    if isinstance(detail, dict) and isinstance(cast(dict[str, Any], detail).get("message"), str):
        return cast(dict[str, Any], detail)["message"], None
    if isinstance(detail, list):
        lines: list[str] = []
        for item in cast(list[Any], detail):
            if not isinstance(item, dict):
                continue
            entry = cast(dict[str, Any], item)
            loc = entry.get("loc")
            where = ".".join("" if part is None else _js_string(part) for part in cast(list[Any], loc)) + ": " if isinstance(loc, list) else ""
            msg = entry.get("msg")
            from .._json import stringify

            lines.append(where + (msg if isinstance(msg, str) else stringify(entry)))
        if lines:
            return "; ".join(lines), None
    return _slice(text), None


def _js_string(value: object) -> str:
    from .._json import number, stringify

    if isinstance(value, str):
        return value
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return number(value)
    if isinstance(value, list):
        return ",".join("" if item is None else _js_string(item) for item in cast(list[Any], value))
    return stringify(value) if isinstance(value, dict) else str(value)


def _slice(text: str) -> str:
    """``text.slice(0, 500)``: 500 UTF-16 code units."""
    if text.isascii():
        return text[:500]
    units = text.encode("utf-16-le", "surrogatepass")[:1000]
    return units.decode("utf-16-le", "surrogatepass")


def retry_after_ms(headers: httpx.Headers | dict[str, str], now: float | None = None) -> float | None:
    """``retry-after-ms`` first, then ``Retry-After`` as seconds or an HTTP date; None when absent or unreadable."""
    ms = _header(headers, "retry-after-ms")
    if ms is not None and re.fullmatch(r"[0-9]+", ms.strip()):
        return int(ms.strip())
    after = _header(headers, "retry-after")
    if after is None:
        return None
    if re.fullmatch(r"[0-9]+", after.strip()):
        return int(after.strip()) * 1000
    at = _http_date(after.strip())
    if at is None:
        return None
    current = time.time() * 1000 if now is None else now
    return max(0, int(at - current))


def _http_date(text: str) -> float | None:
    """ms since the epoch for an HTTP date or, as ``Date.parse`` also takes, an ISO 8601 one; None when unreadable."""
    try:
        return email.utils.parsedate_to_datetime(text).timestamp() * 1000
    except (TypeError, ValueError, IndexError):
        pass
    try:
        parsed = datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    # Date.parse reads a date alone as UTC and a date and time without an offset as local time, as datetime does.
    if parsed.tzinfo is None and len(text) == 10:
        parsed = parsed.replace(tzinfo=datetime.UTC)
    return parsed.timestamp() * 1000


def too_long(error: BaseException, why: str) -> ProviderError:
    """A retryable error for a context-length rejection halving could not clear, keeping the provider's fields."""
    failure = ProviderError(
        f"{message_of(error)}; {why}",
        status=getattr(error, "status", None),
        request_id=getattr(error, "request_id", None),
        retry_after_ms=getattr(error, "retry_after_ms", None),
    )
    failure.__cause__ = error
    return failure
