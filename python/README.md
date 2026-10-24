# attend for Python

Decide when an event should wake your agent, in an asyncio process.

This is the Python port of [attend](https://github.com/acquiredintelligence/attend). It has the same design and the same semantics as the TypeScript package, and its decision ids match it byte for byte. Both implementations pass the shared fixtures in [`conformance/`](../conformance), and either one can read and continue a scope the other wrote in a shared Redis or Postgres. [How attend works](../docs/how-it-works.md) and [the reference](../docs/reference.md) apply to both; this page covers what is particular to Python.

```python
import asyncio

from aqi_attend import create_attend, default_notice


async def judge(input, signal):
    return {"route": "interrupt" if input["window"]["reason"] == "flush" else "note"}


async def sink(decision, signal):
    print(f"[{decision['route']}]\n{default_notice(decision)}")


async def main():
    attend = create_attend(kind={"user.message": "flush", "build.failed": "closing"}, judge=judge, sink=sink)
    running = asyncio.create_task(attend.run())
    await attend.push("session-1", {"name": "build.failed", "text": "Build 4521 failed: 1 test"})
    await attend.push("session-1", {"name": "user.message", "text": "can you look?"})
    await asyncio.sleep(0.1)
    await attend.stop()
    await running


asyncio.run(main())
```

```text
[interrupt]
Build 4521 failed: 1 test
can you look?
Attend to this now, even mid-task.
```

## Install

```sh
pip install aqi-attend                # the engine, no dependencies
pip install "aqi-attend[redis]"       # redis-py, for aqi_attend.store.redis
pip install "aqi-attend[postgres]"    # asyncpg, for aqi_attend.store.postgres
pip install "aqi-attend[http]"        # httpx, for jev_judge and decisions_judge
```

It needs Python 3.12 or later and imports as `aqi_attend`.

## Run it in a worker over Redis

Embed attend in the long-running asyncio worker that already receives your events, and give it a store so a restart loses nothing. Push events where they happen; `run()` settles, judges and delivers in the same process, with no network hop between the event and the decision.

```python
import redis.asyncio as redis

from aqi_attend import create_attend
from aqi_attend.judges import jev_judge
from aqi_attend.store.redis import redis_store

client = redis.Redis.from_pool(redis.BlockingConnectionPool.from_url(REDIS_URL, max_connections=50, timeout=None))
blocking = redis.Redis.from_url(REDIS_URL)

attend = create_attend(
    store=redis_store(client, prefix="attend:{app}:", blocking=blocking),
    kind=kind_of,
    judge=jev_judge(api_key=TYPESAFE_API_KEY),
    sink=deliver,
    dead_letter=keep_dead_letter,
    settle={"closing_grace_ms": 500},
)
```

redis-py runs each concurrent command on a pooled connection of its own, and its default pool raises once 100 are in use, so give the client a `BlockingConnectionPool`. `blocking` is a second client for the decision log's `XREAD BLOCK`, so that `attend.decisions()` hears each decision at once. For Postgres, `await migrate(pool, prefix=...)` at deploy, then pass `postgres_store(pool, listener=connection)` with an asyncpg pool and a dedicated connection for `LISTEN`.

## What differs from TypeScript

- Keyword arguments and `settle` keys are snake_case, such as `create_attend(settle={"wait_horizon_ms": 5000})`. Every value that crosses a store or a judge (events, windows, decisions, dead letters, records, traces) is a dict with the TypeScript field names, such as `decision["window"]["settledAt"]`, so both runtimes share one persisted layout.
- A judge, sink, dead-letter handler, `context` or `capture` is called with `(value, signal)`, where `signal` is an `aqi_attend.AbortSignal`. Honour it with `await signal.race(work)` or by checking `signal.aborted`.
- `push`, `flush` and `end` return futures that resolve once the input is persisted. They take the input when called, as the TypeScript promises do.
- Cancelling the task that runs `run()` stops it the way `stop()` does.

[DEVIATIONS.md](../DEVIATIONS.md#python-port) lists every difference.

## Judges

`aqi_attend.judges` has `jev_judge` (TypeSafe's Jev, with your own key), `decisions_judge` (the OpenAI Decisions API) and `llm_judge`, which wraps any structured-output call you pass as `complete`. They send the same requests as the TypeScript judges, byte for byte. [Choose a judge](../docs/judges.md) explains each one, the window budget and the act threshold.

## Test with a fake clock

```python
from aqi_attend import create_attend
from aqi_attend.testing import fake_clock, recording_sink, scripted_judge

clock = fake_clock()
judge = scripted_judge([{"route": "queue"}])
sink = recording_sink()
attend = create_attend(clock=clock, kind={"build.finished": "closing"}, judge=judge, sink=sink)
running = asyncio.create_task(attend.run())
await attend.push("repo", {"id": "done-1", "name": "build.finished"})
await clock.advance(500)
assert [d["route"] for d in sink.delivered] == ["queue"]
```

`aqi_attend.testing` also has the crash harness and `crash_suite` and `multi_scope_crash_suite`, and `aqi_attend.conformance` has the store conformance suites, for a store adapter of your own.

## Develop

```sh
uv sync
uv run pytest                      # memory only
ATTEND_REDIS_URL=redis://127.0.0.1:6379 ATTEND_POSTGRES_URL=postgres://postgres:attend@127.0.0.1:5432/attend uv run pytest
uv run pyright && uv run ruff check
uv run python bench/latency.py --store redis
```

## License

MIT, Zettel Labs, Inc.
