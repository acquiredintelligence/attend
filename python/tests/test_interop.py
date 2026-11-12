"""One store, two runtimes: the Python port and the TypeScript package read and continue each other's scopes.

Runs when ``ATTEND_INTEROP=1`` and the TypeScript package is built (``pnpm build`` at the repo root),
against Redis and Postgres as ``tests/stores.py`` configures them.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
from pathlib import Path
from typing import Any

import pytest

from aqi_attend import AbortController, JudgeInput, Judgment, Kind, SourceBatch, create_attend, window_id
from aqi_attend.signal import AbortSignal
from aqi_attend.testing import fake_clock

from .scenario import plain
from .stores import POSTGRES_URL, REDIS_URL, make_postgres, make_redis

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = Path(__file__).with_name("interop") / "attend-ts.mjs"
ENABLED = os.environ.get("ATTEND_INTEROP") == "1" and (ROOT / "dist" / "index.js").exists() and shutil.which("node") is not None
KINDS: dict[str, Kind] = {"close": "closing", "say": "flush", "look": "passive", "out": "consequence", "move": "continuous"}

BACKENDS = [name for name, url in (("redis", REDIS_URL), ("postgres", POSTGRES_URL)) if url]


async def judge(input: JudgeInput, signal: AbortSignal) -> Judgment:
    waits = any(e["name"] == "look" for e in input["window"]["events"]) and any(r["route"] == "wait" for r in input["routes"])
    return {"route": "wait" if waits else "interrupt"}


async def typescript(kind: str, prefix: str, mode: str, scope: str) -> dict[str, Any]:
    url = REDIS_URL if kind == "redis" else POSTGRES_URL
    assert url is not None
    process = await asyncio.create_subprocess_exec("node", str(SCRIPT), kind, url, prefix, mode, scope, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    out, err = await process.communicate()
    assert process.returncode == 0, err.decode()
    return json.loads(out)


async def python(store: Any, mode: str, scope: str) -> dict[str, Any]:
    clock = fake_clock(10_000)
    delivered: list[str] = []
    attend = create_attend(store=store, clock=clock, kind=KINDS, judge=judge, sink=lambda d, s: delivered.append(d["id"]), settle={"closing_grace_ms": 100})
    intake = attend.open_intake(scope) if mode == "intake-write" else None
    controller = AbortController()
    running = asyncio.ensure_future(attend.run(signal=controller.signal, sweep_ms=60_000))
    await asyncio.sleep(0)
    if intake is not None:

        async def first(signal: AbortSignal) -> SourceBatch:
            return {"events": [{"id": "raw-a", "name": "move", "at": 10_000, "data": {}, "text": "x" * 2000}], "cursor": "1"}

        async def filtered(signal: AbortSignal) -> SourceBatch:
            return {"events": [], "cursor": "2"}

        async def second(signal: AbortSignal) -> SourceBatch:
            return {"events": [{"id": "raw-b", "name": "move", "at": 10_000, "data": {}}], "cursor": "3"}

        await intake.submit(first, source="feed")
        await intake.submit(filtered, source="feed")
        await intake.submit(second, source="feed")
        await intake.close()
    elif mode == "write":
        await attend.push(scope, [{"id": "a", "name": "close", "text": "close a"}, {"id": "b", "name": "out", "data": {"n": 1.5, "s": "ünï 🌍"}}])
        await clock.advance(100)
        for _ in range(500):
            if delivered:
                break
            await asyncio.sleep(0.005)
        await attend.push(scope, [{"id": "c", "name": "look", "text": "look c"}])
        await clock.advance(4000)
        await attend.push(scope, [{"id": "d", "name": "out", "text": "out d"}])
    elif mode == "pickup":
        # The warm sweep must find the scope the other runtime left due, load it and arm its deadline.
        for _ in range(500):
            if attend._scopes.due(scope) is not None:  # pyright: ignore[reportPrivateUsage]
                break
            await asyncio.sleep(0.005)
        await clock.advance(99)
        assert delivered == []
        for _ in range(500):
            await clock.advance(1 if not delivered else 0)
            if delivered:
                break
            await asyncio.sleep(0.005)
    elif mode == "continue":
        await attend.push(scope, [{"id": "e", "name": "say", "text": "say e"}])
        # The store does real I/O, which advancing the fake clock does not wait for.
        for _ in range(500):
            await clock.advance(0)
            if delivered:
                break
            await asyncio.sleep(0.005)
    controller.abort()
    await running
    record = await attend.inspect(scope)
    rows = (await store.log.read({"after": None, "scope": scope, "limit": 100}))["rows"]
    return plain({"record": record, "rows": rows, "delivered": delivered})


@pytest.mark.skipif(not ENABLED or not BACKENDS, reason="needs ATTEND_INTEROP=1, a built dist/, node, and Redis or Postgres")
@pytest.mark.parametrize("kind", BACKENDS)
async def test_python_and_typescript_share_one_store(kind: str) -> None:
    owned = await (make_redis() if kind == "redis" else make_postgres())
    inner: Any = owned.inner
    prefix = str(inner._prefix if kind == "redis" else inner.prefix)
    try:
        none: list[str] = []
        written = await python(owned, "write", "py")
        delivered: Any = written["delivered"]
        assert delivered == [window_id("py", ["a", "b"])]
        assert (await typescript(kind, prefix, "read", "py")) == {**written, "delivered": none}

        from_ts = await typescript(kind, prefix, "write", "ts")
        delivered = from_ts["delivered"]
        assert delivered == [window_id("ts", ["a", "b"])]
        read = await python(owned, "read", "ts")
        assert read == {**from_ts, "delivered": none}
        assert read["record"]["batch"]["phase"]["phase"] == "waiting"
        assert [e["id"] for e in read["record"]["buffer"]["entries"]] == ["d"]

        continued = await python(owned, "continue", "ts")
        delivered = continued["delivered"]
        rows: Any = continued["rows"]
        assert delivered == [window_id("ts", ["c", "d", "e"])]
        assert [row["id"] for row in rows] == [window_id("ts", ["a", "b"]), window_id("ts", ["c", "d", "e"])]
        assert (await typescript(kind, prefix, "read", "ts"))["record"] == continued["record"]

        left = await typescript(kind, prefix, "pending", "due")
        assert left["record"]["dueAt"] == 10_100 and left["delivered"] == none
        picked = await python(owned, "pickup", "due")
        assert picked["delivered"] == [window_id("due", ["p"])]
        assert picked["record"]["dueAt"] is None
    finally:
        await owned.close()


@pytest.mark.skipif(not ENABLED or not BACKENDS, reason="needs ATTEND_INTEROP=1, a built dist/, node, and Redis or Postgres")
@pytest.mark.parametrize("kind", BACKENDS)
async def test_intake_snapshot_and_tail_continue_without_intake_in_other_runtime(kind: str) -> None:
    owned = await (make_redis() if kind == "redis" else make_postgres())
    inner: Any = owned.inner
    prefix = str(inner._prefix if kind == "redis" else inner.prefix)
    try:
        for writer in ("python", "typescript"):
            scope = "intake-" + writer
            written = await (python(owned, "intake-write", scope) if writer == "python" else typescript(kind, prefix, "intake-write", scope))
            assert written["delivered"] == []
            assert written["record"]["cursors"] == {"feed": "3"}
            stored = await owned.load(scope)
            assert stored is not None and stored["snapshot"] is not None
            assert stored["snapshot"]["reducer"] == 5 and stored["tail"]
            encoded = json.dumps(stored)
            assert all(field not in encoded for field in ('"catchUp"', '"catch_up"', '"prepare"', '"owner"'))
            read = await (typescript(kind, prefix, "read", scope) if writer == "python" else python(owned, "read", scope))
            assert read == written
            continued = await (typescript(kind, prefix, "continue", scope) if writer == "python" else python(owned, "continue", scope))
            assert continued["delivered"] == [window_id(scope, ["raw-a", "raw-b", "e"])]
            assert continued["record"]["cursors"] == {"feed": "3"}
    finally:
        await owned.close()
