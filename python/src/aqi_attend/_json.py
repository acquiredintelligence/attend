"""JSON as JavaScript writes it.

The decision id, the record digest the conformance fixtures pin, the judges' request bodies and
the size estimate all depend on the exact text ``JSON.stringify`` produces and on lengths counted
the way JavaScript counts them. These helpers reproduce both for plain JSON values.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from typing import Any

_INDEX = re.compile(r"0|[1-9][0-9]*")
_ESCAPE = re.compile(r'["\\\x00-\x1f\ud800-\udfff]')
_SHORT = {'"': '\\"', "\\": "\\\\", "\b": "\\b", "\f": "\\f", "\n": "\\n", "\r": "\\r", "\t": "\\t"}


def utf16(text: str) -> str:
    """The string JavaScript would hold: adjacent surrogate halves joined into one code point, lone halves kept."""
    if text.isascii():
        return text
    return text.encode("utf-16-le", "surrogatepass").decode("utf-16-le", "surrogatepass")


def utf8_bytes(text: str) -> bytes:
    """UTF-8, replacing lone surrogates with U+FFFD as TextEncoder does."""
    if text.isascii():
        return text.encode("ascii")
    return text.encode("utf-16-le", "surrogatepass").decode("utf-16-le", "replace").encode("utf-8")


def utf8_length(text: str) -> int:
    """UTF-8 byte length, replacing lone surrogates with U+FFFD as TextEncoder does."""
    if text.isascii():
        return len(text)
    return len(utf8_bytes(text))


def utf16_length(text: str) -> int:
    """``text.length`` in JavaScript: UTF-16 code units."""
    if text.isascii():
        return len(text)
    return len(text.encode("utf-16-le", "surrogatepass")) // 2


def _escape(match: re.Match[str]) -> str:
    char = match.group(0)
    short = _SHORT.get(char)
    return short if short is not None else f"\\u{ord(char):04x}"


def quote(text: str) -> str:
    """``JSON.stringify`` of a string: lone surrogates are escaped, everything else outside ASCII is literal."""
    return '"' + _ESCAPE.sub(_escape, utf16(text)) + '"'


def _digits(value: float) -> tuple[str, int]:
    """The shortest round-trip digits of a positive finite float and n, with value = 0.digits * 10**n."""
    mantissa, _, exponent = repr(value).partition("e")
    whole, _, fraction = mantissa.partition(".")
    digits = whole + fraction
    n = len(whole) + (int(exponent) if exponent else 0)
    stripped = digits.lstrip("0")
    n -= len(digits) - len(stripped)
    return stripped.rstrip("0") or "0", n


def number(value: float) -> str:
    """``String(value)`` for a JavaScript number (Number::toString). Integral floats print without a fraction."""
    if isinstance(value, bool):
        raise TypeError("a boolean is not a number")
    if isinstance(value, int) and abs(value) < 10**21:
        return str(value)
    value = float(value)
    if math.isnan(value):
        return "NaN"
    if value == 0:
        return "0"
    if math.isinf(value):
        return "Infinity" if value > 0 else "-Infinity"
    if value < 0:
        return "-" + number(-value)
    digits, n = _digits(value)
    k = len(digits)
    if k <= n <= 21:
        return digits + "0" * (n - k)
    if 0 < n <= 21:
        return digits[:n] + "." + digits[n:]
    if -6 < n <= 0:
        return "0." + "0" * -n + digits
    sign = "+" if n - 1 >= 0 else "-"
    head = digits[0] if k == 1 else digits[0] + "." + digits[1:]
    return f"{head}e{sign}{abs(n - 1)}"


def _key_order(value: Mapping[str, Any]) -> list[str]:
    """Own keys in JavaScript's order: array indices ascending, then the rest in insertion order."""
    keys = list(value)
    indices = [key for key in keys if _INDEX.fullmatch(key) and int(key) < 4294967295]
    if not indices:
        return keys
    rest = [key for key in keys if not (_INDEX.fullmatch(key) and int(key) < 4294967295)]
    return sorted(indices, key=int) + rest


def _utf16_key(key: str) -> bytes:
    return utf16(key).encode("utf-16-be", "surrogatepass")


def _write(value: Any, out: list[str], stable: bool) -> bool:
    """Appends ``value`` as JSON; false when JSON would omit it (only ``None`` keys are kept)."""
    if value is None:
        out.append("null")
    elif value is True:
        out.append("true")
    elif value is False:
        out.append("false")
    elif isinstance(value, str):
        out.append(quote(value))
    elif isinstance(value, (int, float)):
        out.append("null" if isinstance(value, float) and not math.isfinite(value) else number(value))
    elif isinstance(value, Mapping):
        mapping: Mapping[str, Any] = value  # pyright: ignore[reportUnknownVariableType]
        # stableJson sorts the entries and rebuilds the object, which hoists array indices again.
        keys = _key_order(dict.fromkeys(sorted(mapping, key=_utf16_key))) if stable else _key_order(mapping)
        out.append("{")
        first = True
        for key in keys:
            if not first:
                out.append(",")
            first = False
            out.append(quote(key))
            out.append(":")
            _write(mapping[key], out, stable)
        out.append("}")
    elif isinstance(value, Sequence):
        items: Sequence[Any] = value  # pyright: ignore[reportUnknownVariableType]
        out.append("[")
        for index, item in enumerate(items):
            if index:
                out.append(",")
            _write(item, out, stable)
        out.append("]")
    else:
        raise TypeError(f"attend: {type(value).__name__} is not JSON")
    return True


def stringify(value: Any) -> str:
    """``JSON.stringify(value)`` for plain JSON values, byte for byte."""
    out: list[str] = []
    _write(value, out, False)
    return "".join(out)


def stable_json(value: Any) -> str:
    """JSON with every object's keys sorted by UTF-16 code units, so equal values give equal text."""
    out: list[str] = []
    _write(value, out, True)
    return "".join(out)


def clone(value: Any) -> Any:
    """A deep copy of a plain JSON value: what ``structuredClone`` does for one."""
    if isinstance(value, dict):
        mapping: dict[str, Any] = value  # pyright: ignore[reportUnknownVariableType]
        return {key: clone(item) for key, item in mapping.items()}
    if isinstance(value, list):
        items: list[Any] = value  # pyright: ignore[reportUnknownVariableType]
        return [clone(item) for item in items]
    return value


def to_fixed(value: float, digits: int) -> str:
    """``value.toFixed(digits)``: the exact binary value rounded half away from zero."""
    from decimal import ROUND_HALF_UP, Decimal

    if value == 0:
        value = 0.0
    if not math.isfinite(value) or abs(value) >= 1e21:
        return number(value)
    return str(Decimal(value).quantize(Decimal(1).scaleb(-digits), rounding=ROUND_HALF_UP))
