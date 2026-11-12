"""A failed-then-waiting window retains its next backoff across runtime changes."""

import asyncio
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from aqi_attend import AbortSignal, Decision, JudgeInput, Judgment, SourceBatch, Trace, create_attend, window_id
from aqi_attend.testing import fake_clock

from .stores import POSTGRES_URL, REDIS_URL, make_postgres, make_redis
from .test_interop import BACKENDS, ENABLED

SCRIPT = Path(__file__).with_name("interop") / "continuation-ts.mjs"


async def typescript(kind: str, prefix: str, mode: str, scope: str) -> dict[str, Any]:
    url = REDIS_URL if kind == "redis" else POSTGRES_URL
    assert url is not None
    process = await asyncio.create_subprocess_exec("node", str(SCRIPT), kind, url, prefix, mode, scope, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        out, err = await process.communicate()
        assert process.returncode == 0, err.decode()
        return json.loads(out)
    finally:
        if process.returncode is None:
            process.terminate()
            await process.wait()


async def python(store: Any, mode: str, scope: str) -> dict[str, Any]:
    base = fake_clock(10_000)
    retrying, waiting, armed, delivered = asyncio.Event(), asyncio.Event(), asyncio.Event(), asyncio.Event()
    calls: list[dict[str, float | int]] = []
    retries: list[float] = []
    deliveries: list[str] = []

    class Clock:
        def now(self) -> float:
            return base.now()

        def at(self, at: float, fn: Callable[[], None]) -> Callable[[], None]:
            cancel = base.at(at, fn)
            if at == 10_060:
                armed.set()
            return cancel

    def observe(trace: Trace) -> None:
        if trace["kind"] == "retrying" and trace["stage"] == "judge":
            retries.append(trace["at"])
            retrying.set()
        if trace["kind"] == "waiting":
            waiting.set()
        if trace["kind"] == "delivered":
            delivered.set()

    async def judge(input: JudgeInput, signal: AbortSignal) -> Judgment:
        calls.append({"at": base.now(), "waits": input["window"]["waits"]})
        if len(calls) == 1:
            raise RuntimeError("controlled outage")
        return {"route": "wait" if mode == "write" else "queue"}

    async def sink(decision: Decision, signal: AbortSignal) -> None:
        deliveries.append(decision["id"])

    engine = create_attend(
        store=store,
        clock=Clock(),
        kind={"reading": "continuous"},
        judge=judge,
        sink=sink,
        observe=observe,
        settle={"continuation": "accumulate", "stream_gap_ms": 10, "wait_horizon_ms": 20, "retry_backoff_ms": [30, 60]},
    )
    intake = engine.open_intake(scope)
    running = asyncio.create_task(engine.run())
    try:
        if mode == "write":

            async def prepare(signal: AbortSignal) -> SourceBatch:
                return {"events": [{"id": "a", "name": "reading", "at": 10_000, "data": {}, "text": "x" * 1000}], "cursor": "1"}

            await intake.submit(prepare, source="feed")
            await base.advance(10)
            await retrying.wait()
            await base.advance(30)
            await waiting.wait()
        else:
            await armed.wait()
            await base.advance(60)
            await retrying.wait()
            await base.advance(60)
            await delivered.wait()
    finally:
        await intake.close()
        await engine.stop()
        await running
    return {"record": await engine.inspect(scope), "calls": calls, "retries": retries, "deliveries": deliveries}


@pytest.mark.skipif(not ENABLED or not BACKENDS, reason="needs ATTEND_INTEROP=1, built dist/, node, and Redis or Postgres")
@pytest.mark.parametrize("kind", BACKENDS)
async def test_waiting_failure_allowance_crosses_runtime_boundary(kind: str) -> None:
    owned = await (make_redis() if kind == "redis" else make_postgres())
    inner: Any = owned.inner
    prefix = str(inner._prefix if kind == "redis" else inner.prefix)
    try:
        for writer in ("python", "typescript"):
            scope = "continuation-" + writer
            written = await (python(owned, "write", scope) if writer == "python" else typescript(kind, prefix, "write", scope))
            assert written["calls"] == [{"at": 10_010, "waits": 0}, {"at": 10_040, "waits": 0}]
            assert written["retries"] == [10_040] and not written["deliveries"]
            assert written["record"]["batch"]["phase"] == {"phase": "waiting", "attempt": 2, "until": 10_060, "failures": 1}
            continued = await (typescript(kind, prefix, "continue", scope) if writer == "python" else python(owned, "continue", scope))
            assert continued["retries"] == [10_120]
            assert continued["calls"] == [{"at": 10_060, "waits": 1}, {"at": 10_120, "waits": 1}]
            assert continued["deliveries"] == [window_id(scope, ["a"])]
            assert continued["record"]["batch"] is None
            assert continued["record"]["outbox"] == {"decisions": [], "deadLetters": []}
            assert continued["record"]["cursors"] == {"feed": "1"}
    finally:
        await owned.close()
