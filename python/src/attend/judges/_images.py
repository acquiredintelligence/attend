"""Image checks for a judge that rejects a whole request over one bad image.

Reads only the PNG IHDR or JPEG SOF header, decoding base64 three bytes at a time, as
``src/internal/images.ts`` does, so a large file costs a few dozen decodes rather than one copy.
"""

from __future__ import annotations

import base64
import binascii
import re
from dataclasses import dataclass

_BASE64 = re.compile(r"[A-Za-z0-9+/]*={0,2}")


@dataclass(slots=True)
class ImageHeader:
    format: str
    width: int
    height: int
    animated: bool


def is_standard_base64(data: str) -> bool:
    """The standard alphabet with ``=`` padding and no whitespace, which is what the providers accept."""
    return len(data) > 0 and len(data) % 4 == 0 and _BASE64.fullmatch(data) is not None


def decoded_length(data: str) -> int:
    padding = 2 if data.endswith("==") else 1 if data.endswith("=") else 0
    return (len(data) // 4) * 3 - padding


def image_header(data: str) -> ImageHeader | None:
    """Width, height and format from the header of a base64 PNG or JPEG; None when it is neither or the header is cut short."""

    def at(i: int) -> int | None:
        group = i // 3
        try:
            decoded = base64.b64decode(data[group * 4 : group * 4 + 4], validate=False)
        except (binascii.Error, ValueError):
            return None
        index = i - group * 3
        return decoded[index] if index < len(decoded) else None

    def u16(i: int) -> int | None:
        a, b = at(i), at(i + 1)
        return None if a is None or b is None else (a << 8) | b

    def u32(i: int) -> int | None:
        a, b = u16(i), u16(i + 2)
        return None if a is None or b is None else a * 65_536 + b

    def tag(i: int) -> str | None:
        found = [at(i), at(i + 1), at(i + 2), at(i + 3)]
        return None if any(b is None for b in found) else "".join(chr(b or 0) for b in found)

    if u32(0) == 0x89504E47 and u32(4) == 0x0D0A1A0A and tag(12) == "IHDR":
        width, height = u32(16), u32(20)
        if width is None or height is None:
            return None
        animated = False
        offset = 8
        while True:
            length, kind = u32(offset), tag(offset + 4)
            if length is None or kind is None or kind == "IDAT" or kind == "IEND":
                break
            if kind == "acTL":
                animated = True
            offset += 12 + length
        return ImageHeader("png", width, height, animated)

    if u16(0) == 0xFFD8:
        offset = 2
        while True:
            marker, code = at(offset), at(offset + 1)
            if marker != 0xFF or code is None:
                return None
            if code == 0xFF:
                offset += 1
                continue
            standalone = code == 0x01 or 0xD0 <= code <= 0xD9
            frame = 0xC0 <= code <= 0xCF and code not in (0xC4, 0xC8, 0xCC)
            if frame:
                height, width = u16(offset + 5), u16(offset + 7)
                return None if height is None or width is None else ImageHeader("jpeg", width, height, False)
            length = 0 if standalone else u16(offset + 2)
            if length is None:
                return None
            offset += 2 + length

    return None
