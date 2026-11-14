"""Both runtimes continue and fence independent limits on shared stores."""

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from aqi_attend import AbortSignal, Event, JudgeInput, Judgment, SourceBatch, Trace, create_attend
from aqi_attend.testing import fake_clock

from .stores import POSTGRES_URL, REDIS_URL, make_postgres, make_redis
from .test_interop import BACKENDS, ENABLED

SCRIPT = Path(__file__).with_name("interop") / "event-limits-ts.mjs"


async def typescript(kind: str, prefix: str, mode: str, scope: str, phase: str) -> dict[str, Any]:
    url = REDIS_URL if kind == "redis" else POSTGRES_URL
    assert url is not None
    process = await asyncio.create_subprocess_exec("node", str(SCRIPT), kind, url, prefix, mode, scope, phase, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        out, err = await process.communicate()
        assert process.returncode == 0, err.decode()
        return json.loads(out)
    finally:
        if process.returncode is None:
            process.terminate()
            await process.wait()


async def python(store: Any, mode: str, scope: str, phase: str) -> dict[str, Any]:
    clock = fake_clock(10000 if mode == "write" else 10015)
    done = asyncio.Event()
    calls: list[dict[str, Any]] = []

    def event(id: str, name: str = "reading") -> Event:
        return {"id": id, "name": name, "at": clock.now(), "data": {}, "text": "q" * 128}

    async def judge(input: JudgeInput, signal: AbortSignal) -> Judgment:
        calls.append(
            {
                "at": clock.now(),
                "ids": [e["id"] for e in input["window"]["events"]],
                "wait": any(r["route"] == "wait" for r in input["routes"]),
                "dropped": input["window"]["dropped"],
                "gap": input["window"]["gap"],
            }
        )
        if mode == "write" and phase == "retrying":
            raise RuntimeError("controlled")
        return {"route": "wait" if mode == "write" else "queue"}

    def observe(trace: Trace) -> None:
        if (mode == "write" and trace["kind"] in ("waiting", "retrying")) or (mode == "continue" and trace["kind"] == "judged"):
            done.set()

    engine = create_attend(
        store=store,
        clock=clock,
        kind={"reading": "continuous", "meta": "context", "result": "consequence"},
        judge=judge,
        sink="log",
        observe=observe,
        settle={
            "continuation": "accumulate",
            "event_limits": {"counted": 4 if mode == "refuse" else 3, "background": 2},
            "stream_gap_ms": 10,
            "wait_horizon_ms": 20,
            "retry_backoff_ms": [30],
        },
    )
    if mode == "read":
        return {"record": await engine.inspect(scope)}
    if mode == "refuse":
        before = await store.load(scope)
        with pytest.raises(ValueError, match="unresolved"):
            await engine.push(scope, event("refused", "meta"))
        assert await store.load(scope) == before
        return {"rejected": True, "unchanged": True}
    intake = engine.open_intake(scope)
    running = asyncio.create_task(engine.run())
    try:
        if mode == "write":

            async def initial(signal: AbortSignal) -> SourceBatch:
                return {"events": [event("a"), event("x", "meta"), event("y", "result")], "cursor": "1"}

            await intake.submit(initial, source="feed")
            await clock.advance(10)
            await done.wait()
            await clock.advance(5)

            async def fresh(signal: AbortSignal) -> SourceBatch:
                return {"events": [event("z", "meta")], "cursor": "2", "truncated": True}

            await intake.submit(fresh, source="feed")
        else:

            async def more(signal: AbortSignal) -> SourceBatch:
                return {"events": [event("b"), event("c")], "cursor": "3"}

            await intake.submit(more, source="feed")
            if phase == "retrying":
                await clock.advance(24)
                assert calls == []
                await clock.advance(1)
            await done.wait()
    finally:
        await intake.close()
        await engine.stop()
        await running
    return {"record": await engine.inspect(scope), "calls": calls}


@pytest.mark.skipif(not ENABLED or not BACKENDS, reason="needs ATTEND_INTEROP=1, built dist/, node, and Redis or Postgres")
@pytest.mark.parametrize("kind", BACKENDS)
@pytest.mark.parametrize("phase", ["waiting", "retrying"])
@pytest.mark.parametrize("writer", ["python", "typescript"])
async def test_independent_limits_continue_across_runtimes(kind: str, phase: str, writer: str) -> None:
    owned = await (make_redis() if kind == "redis" else make_postgres())
    inner: Any = owned.inner
    prefix = str(inner._prefix if kind == "redis" else inner.prefix)
    scope = "independent-" + phase + "-" + writer
    try:
        written = await (python(owned, "write", scope, phase) if writer == "python" else typescript(kind, prefix, "write", scope, phase))
        stored = await owned.load(scope)
        assert stored is not None and stored["snapshot"] is not None
        assert stored["snapshot"]["reducer"] == 5 and stored["tail"]
        assert stored["snapshot"]["policy"].get("eventLimits") == {"counted": 3, "background": 2}
        read = await (typescript(kind, prefix, "read", scope, phase) if writer == "python" else python(owned, "read", scope, phase))
        assert read["record"] == written["record"]
        refused = await (typescript(kind, prefix, "refuse", scope, phase) if writer == "python" else python(owned, "refuse", scope, phase))
        assert refused == {"rejected": True, "unchanged": True}
        continued = await (typescript(kind, prefix, "continue", scope, phase) if writer == "python" else python(owned, "continue", scope, phase))
        assert continued["calls"] == [{"at": 10040 if phase == "retrying" else 10015, "ids": ["a", "y", "z", "b", "c"], "wait": False, "dropped": 1, "gap": True}]
        assert continued["record"]["batch"] is None
        assert continued["record"]["cursors"] == {"feed": "3"}
    finally:
        await owned.close()
