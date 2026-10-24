"""What the Redis store must share with the TypeScript one: its scripts, its due encoding and its layout."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from attend.store import _redis_scripts as scripts
from attend.store.redis import due_hex, from_due_hex

from .stores import REDIS_URL

TS_SOURCE = Path(__file__).resolve().parents[2] / "src" / "store" / "redis.ts"


def test_the_lua_scripts_are_the_typescript_scripts() -> None:
    source = TS_SOURCE.read_text("utf-8")
    common = re.search(r"const COMMON = `(.*?)`;", source, re.S)
    assert common is not None
    for name in ("COMMON", "COMMIT", "LOAD", "DUE", "REMOVE", "READ", "FIND", "BOUNDS", "TRIM", "EPOCH"):
        found = re.search(rf"const {name} = `(.*?)`;", source, re.S)
        assert found is not None, name
        assert getattr(scripts, name) == found.group(1).replace("${COMMON}", common.group(1)), name


@pytest.mark.parametrize(
    ("value", "hex"),
    [(0, "8000000000000000"), (1000, "c08f400000000000"), (1_761_234_567_000, "c279a11c2bf58000"), (-1, "400fffffffffffff"), (-0.0, "8000000000000000")],
)
def test_due_hex_matches_the_typescript_encoding(value: float, hex: str) -> None:
    assert due_hex(value) == hex
    assert from_due_hex(hex) == value


def test_due_hex_sorts_as_numbers_do() -> None:
    values = [-1e15, -2.5, -1, 0, 0.5, 1, 999, 1000, 1_761_234_567_000, 9e15]
    assert sorted(values, key=due_hex) == values


@pytest.mark.skipif(REDIS_URL is None, reason="ATTEND_REDIS_URL is not set")
async def test_a_typescript_layout_round_trips() -> None:
    """Keys and fields as ``src/store/redis.ts`` writes them: one scope hash, one inputs hash, the due index and the decision stream."""
    from .stores import make_redis

    store = await make_redis()
    inner = store.inner
    client = inner._client  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType, reportUnknownVariableType]
    prefix: str = inner._prefix  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType, reportUnknownVariableType]
    try:
        logged = {"input": {"kind": "flush"}, "now": 1000}
        assert await store.commit([{"scope": "s", "expectedVersion": 0, "inputs": [logged], "snapshot": None, "due": 2000, "appends": []}]) == ["ok"]  # type: ignore[list-item]
        assert await client.hgetall(f"{prefix}scope:s") == {b"version": b"1", b"due": due_hex(2000).encode()}  # pyright: ignore[reportUnknownMemberType]
        assert await client.hgetall(f"{prefix}inputs:s") == {b"1": b'{"input":{"kind":"flush"},"now":1000}'}  # pyright: ignore[reportUnknownMemberType]
        assert await client.zrange(f"{prefix}due-index", 0, -1) == [f"{due_hex(2000)}:s".encode()]  # pyright: ignore[reportUnknownMemberType]
    finally:
        await store.close()
