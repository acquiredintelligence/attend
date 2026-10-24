"""In-process latency from ``push`` to delivery for a flush event, as ``bench/latency.mjs`` measures it in TypeScript.

One probe at a time, ``--probe-ms`` apart: the clock starts before ``push`` of one flush event and
stops when the sink receives its decision. The judge answers at once, so the numbers are attend's
own: intake, the commit, the scheduler tick, the judge commit and the delivery commit.

    uv run python bench/latency.py --store memory
    uv run python bench/latency.py --store redis --redis-url redis://127.0.0.1:16379
"""

from __future__ import annotations

import argparse
import asyncio
import json
import secrets
import statistics
import sys
import time
from typing import Any

from aqi_attend import AbortController, AbortSignal, Decision, JudgeInput, Judgment, create_attend, memory_store
from aqi_attend.store.types import ScopeStore


def percentiles(samples: list[float]) -> dict[str, float]:
    ordered = sorted(samples)

    def at(p: float) -> float:
        return round(ordered[min(len(ordered) - 1, int(p / 100 * len(ordered)))], 1)

    return {"p50": at(50), "p90": at(90), "p99": at(99), "max": round(ordered[-1], 1), "mean": round(statistics.fmean(ordered), 1)}


async def open_store(name: str, redis_url: str) -> tuple[ScopeStore, Any]:
    if name == "memory":
        return memory_store(), None
    import redis.asyncio as redis

    from aqi_attend.store.redis import redis_store

    client: Any = redis.Redis.from_pool(redis.BlockingConnectionPool.from_url(redis_url, max_connections=16, timeout=None))  # pyright: ignore[reportUnknownMemberType]
    prefix = f"attend-bench:{{{secrets.token_hex(4)}}}:"

    async def close() -> None:
        keys: list[Any] = [key async for key in client.scan_iter(match=f"{prefix}*", count=500)]
        if keys:
            await client.unlink(*keys)
        await client.aclose()

    return redis_store(client, prefix=prefix), close


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--store", choices=["memory", "redis"], default="memory")
    parser.add_argument("--redis-url", default="redis://127.0.0.1:16379")
    parser.add_argument("--samples", type=int, default=200)
    parser.add_argument("--warmup", type=int, default=20)
    parser.add_argument("--probe-ms", type=float, default=20)
    options = parser.parse_args()

    store, close = await open_store(options.store, options.redis_url)
    pending: dict[str, Any] = {}
    flush_to_judge: list[float] = []
    judge_to_sink: list[float] = []
    flush_to_sink: list[float] = []

    async def judge(input: JudgeInput, signal: AbortSignal) -> Judgment:
        probe = pending.get("probe")
        if probe is not None and any(event["id"] == probe["id"] for event in input["window"]["events"]):
            probe["t1"] = time.perf_counter()
        return {"route": "interrupt"}

    async def sink(decision: Decision, signal: AbortSignal) -> None:
        probe = pending.get("probe")
        if probe is not None and any(event["id"] == probe["id"] for event in decision["window"]["events"]):
            probe["t2"] = time.perf_counter()
            probe["done"].set_result(None)

    attend = create_attend(store=store, kind={"bench.flush": "flush"}, judge=judge, sink=sink, dead_letter=lambda letter, signal: None)
    controller = AbortController()
    running = asyncio.ensure_future(attend.run(signal=controller.signal, sweep_ms=60_000))
    await asyncio.sleep(0.05)
    loop = asyncio.get_running_loop()
    try:
        for i in range(options.warmup + options.samples):
            await asyncio.sleep(options.probe_ms / 1000)
            probe: dict[str, Any] = {"id": f"e{i}", "done": loop.create_future()}
            pending["probe"] = probe
            t0 = time.perf_counter()
            await attend.push("bench-probe", [{"id": probe["id"], "name": "bench.flush", "data": {"seq": i, "x": 1, "y": 2, "value": 0.42}, "text": f"flush {i}"}])
            await asyncio.wait_for(probe["done"], 5)
            if i >= options.warmup:
                flush_to_judge.append((probe["t1"] - t0) * 1e6)
                judge_to_sink.append((probe["t2"] - probe["t1"]) * 1e6)
                flush_to_sink.append((probe["t2"] - t0) * 1e6)
    finally:
        controller.abort()
        await running
        if close is not None:
            await close()
    print(
        json.dumps(
            {
                "report": "latency",
                "runtime": f"python {sys.version.split()[0]}",
                "store": options.store,
                "samples": len(flush_to_sink),
                "flushToJudgeUs": percentiles(flush_to_judge),
                "judgeToSinkUs": percentiles(judge_to_sink),
                "flushToSinkUs": percentiles(flush_to_sink),
            }
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
