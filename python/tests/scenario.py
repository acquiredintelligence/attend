"""Runs one conformance scenario through the Python runtime, the way ``test/conformance/scenario.ts`` does through the TypeScript one."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from typing import Any, cast

from aqi_attend import PermanentError, create_attend, memory_store
from aqi_attend._json import stable_json
from aqi_attend.attend import JudgeContext
from aqi_attend.clock import drain
from aqi_attend.core.types import DeadLetter, Decision, JudgeImage, JudgeInput, Judgment, Trace
from aqi_attend.errors import message_of, name_of
from aqi_attend.signal import AbortController, AbortSignal
from aqi_attend.testing import fake_clock

UNCOMPARED_TRACES = {"backpressure"}

_SETTLE = {
    "closingGraceMs": "closing_grace_ms",
    "streamGapMs": "stream_gap_ms",
    "passiveQuietMs": "passive_quiet_ms",
    "waitHorizonMs": "wait_horizon_ms",
    "maxWaits": "max_waits",
    "maxEvents": "max_events",
    "maxWindowMs": "max_window_ms",
    "retryBackoffMs": "retry_backoff_ms",
    "judgeTimeoutMs": "judge_timeout_ms",
    "deliveryTimeoutMs": "delivery_timeout_ms",
    "recentCount": "recent_count",
    "seenCount": "seen_count",
    "maxOutbox": "max_outbox",
    "maxDeadLetters": "max_dead_letters",
    "deadLetterBackoffMaxMs": "dead_letter_backoff_max_ms",
    "maxWindowBytes": "max_window_bytes",
}


def plain(value: Any) -> Any:
    return json.loads(stable_json(value))


def _pick(scripts: Mapping[str, Any] | None, scope: str) -> Any:
    if scripts is None:
        return None
    return scripts.get(scope, scripts.get("*"))


def _outcome_of(attempt: Any) -> str:
    if attempt == "ok":
        return "ok"
    if "fail" in attempt:
        return f"{'permanent' if attempt.get('permanent') else 'fail'}: {attempt['fail']}"
    if "hang" in attempt:
        return "hang"
    return f"hold: {attempt['hold']}"


def _error_text(error: BaseException) -> str:
    return f"{name_of(error)}: {message_of(error)}"


async def run_scenario(scenario: Mapping[str, Any]) -> dict[str, Any]:
    loop = asyncio.get_running_loop()
    clock = fake_clock(scenario.get("start", 1000))
    store = memory_store()
    observed: dict[str, Any] = {"judgeCalls": {}, "deliveries": {}, "deadLetterCalls": {}, "traces": {}, "results": [], "inspections": [], "records": {}, "log": []}
    holds: dict[str, asyncio.Future[Any]] = {}
    answered: dict[str, int] = {}
    fired: set[str] = set()
    attempts: dict[str, int] = {}
    captures: dict[str, int] = {}

    def record(kind: str, scope: str, value: Any) -> None:
        observed[kind].setdefault(scope, []).append(value)

    def failure(message: str, permanent: bool | None) -> Exception:
        return PermanentError(message) if permanent else Exception(message)

    async def settle(answer: Mapping[str, Any], signal: AbortSignal) -> Any:
        if "fail" in answer:
            raise failure(answer["fail"], answer.get("permanent"))
        if "hang" in answer:
            await signal.wait()
            raise Exception("aborted")
        if "ignore" in answer:
            await loop.create_future()
        if "hold" in answer:
            future = loop.create_future()
            holds[answer["hold"]] = future
            return await future
        if "malformed" in answer:
            return answer["malformed"]
        return json.loads(json.dumps(answer))

    class Judge:
        budget = scenario.get("config", {}).get("budget")

        async def __call__(self, input: JudgeInput, signal: AbortSignal, /) -> Judgment:
            record("judgeCalls", input["scope"], plain(input))
            script = _pick(scenario["judge"], input["scope"])
            if script is None:
                raise Exception(f"no judge script for scope {input['scope']}")
            n = answered.get(input["scope"], 0)
            answered[input["scope"]] = n + 1
            answer = None
            if "answers" in script:
                answer = script["answers"][min(n, len(script["answers"]) - 1)]
            else:
                ids = {event["id"] for event in input["window"]["events"]}
                for index, rule in enumerate(script["rules"]):
                    if "has" in rule and not all(id in ids for id in rule["has"]):
                        continue
                    if "lacks" in rule and any(id in ids for id in rule["lacks"]):
                        continue
                    if "offers" in rule and not any(route["route"] == rule["offers"] for route in input["routes"]):
                        continue
                    if rule.get("first") and n > 0:
                        continue
                    key = f"{index}:{input['window']['id']}"
                    if rule.get("once") and key in fired:
                        continue
                    if rule.get("once"):
                        fired.add(key)
                    answer = rule["answer"]
                    break
            if answer is None:
                raise Exception("no scripted answer")
            return cast(Judgment, await settle(answer, signal))

    judge = Judge()
    if judge.budget is None:
        del Judge.budget

    def handler(scripts: Mapping[str, Any], lane: str) -> Any:
        async def run(scope: str, route: str | None, signal: AbortSignal) -> str:
            script = _pick(scripts, scope)
            attempt: Any = "ok"
            if script is not None and "attempts" in script:
                key = f"{lane}:{scope}"
                n = attempts.get(key, 0)
                attempts[key] = n + 1
                attempt = script["attempts"][n] if n < len(script["attempts"]) else "ok"
            elif script is not None:
                attempt = next((rule["answer"] for rule in script["rules"] if "route" not in rule or rule["route"] == route), "ok")
            outcome = _outcome_of(attempt)
            if attempt != "ok":
                await settle(attempt, signal)
            return outcome

        return run

    sink_scripts = scenario.get("sink", {"*": {"attempts": []}})
    deliver = None if sink_scripts == "log" else handler(sink_scripts, "sink")
    letter_scripts = scenario.get("deadLetter", "log")
    handle = None if letter_scripts == "log" else handler(letter_scripts, "deadLetter")
    config = scenario.get("config", {})

    async def sink(decision: Decision, signal: AbortSignal) -> None:
        assert deliver is not None
        entry = {"decision": plain(decision), "outcome": "pending"}
        record("deliveries", decision["scope"], entry)
        try:
            entry["outcome"] = await deliver(decision["scope"], decision["route"], signal)
        except Exception as error:
            entry["outcome"] = _error_text(error)
            raise

    async def dead_letter(letter: DeadLetter, signal: AbortSignal) -> None:
        assert handle is not None
        entry = {"letter": plain(letter), "outcome": "pending"}
        record("deadLetterCalls", letter["scope"], entry)
        try:
            decision = letter["decision"]
            entry["outcome"] = await handle(letter["scope"], decision["route"] if decision is not None else None, signal)
        except Exception as error:
            entry["outcome"] = _error_text(error)
            raise

    def observe(trace: Trace) -> None:
        if trace["kind"] in UNCOMPARED_TRACES or "scope" not in trace:
            return
        record("traces", trace["scope"], plain(trace))

    def context(scope: str, signal: AbortSignal) -> JudgeContext:
        live: JudgeContext = {}
        facts = _pick(config.get("facts"), scope)
        if facts is not None:
            live["facts"] = facts
        if "contextPredicates" in config:
            live["predicates"] = config["contextPredicates"]
        return live

    async def capture(scope: str, signal: AbortSignal) -> JudgeImage | list[JudgeImage] | None:
        answers = _pick(scenario["capture"], scope) or [None]
        n = captures.get(scope, 0)
        captures[scope] = n + 1
        answer = answers[min(n, len(answers) - 1)]
        if isinstance(answer, dict) and "fail" in answer:
            raise Exception(str(cast(dict[str, Any], answer)["fail"]))
        if isinstance(answer, dict) and "hang" in answer:
            await loop.create_future()
        return cast(JudgeImage | list[JudgeImage] | None, answer)

    options: dict[str, Any] = {}
    for name, key in (
        ("guidance", "guidance"),
        ("criteria", "criteria"),
        ("predicates", "predicates"),
        ("actThreshold", "act_threshold"),
        ("windowShare", "window_share"),
        ("judgeConcurrency", "judge_concurrency"),
        ("captureTimeoutMs", "capture_timeout_ms"),
    ):
        if name in config:
            options[key] = config[name]
    if "facts" in config or "contextPredicates" in config:
        options["context"] = context
    if "capture" in scenario:
        options["capture"] = capture
    if "settle" in scenario:
        options["settle"] = {_SETTLE[key]: value for key, value in scenario["settle"].items()}

    attend = create_attend(
        store=store,
        clock=clock,
        kind=scenario["kinds"],
        judge=judge,
        sink="log" if deliver is None else sink,
        dead_letter="log" if handle is None else dead_letter,
        observe=observe,
        **options,
    )
    controller = AbortController()
    running = asyncio.ensure_future(attend.run(signal=controller.signal, sweep_ms=scenario.get("sweepMs", 1000)))
    await drain()
    scopes: set[str] = set()

    async def result(index: int, work: asyncio.Future[None]) -> None:
        try:
            await work
            observed["results"].append({"step": index, "error": None})
        except Exception as error:
            observed["results"].append({"step": index, "error": {"name": name_of(error), "message": message_of(error)}})

    for index, step in enumerate(scenario["steps"]):
        if "push" in step:
            scopes.add(step["push"])
            events = [{"id": e["id"], "at": e.get("at", 0), "name": e["name"], "data": e.get("data", {}), **({"text": e["text"]} if "text" in e else {})} for e in step["events"]]
            await result(index, attend.push(step["push"], events))
        elif "advance" in step:
            await clock.advance(step["advance"])
        elif "flush" in step:
            scopes.add(step["flush"])
            await result(index, attend.flush(step["flush"]))
        elif "end" in step:
            scopes.add(step["end"])
            await result(index, attend.end(step["end"]))
        elif "release" in step:
            future = holds.pop(step["release"])
            if "fail" in step:
                future.set_exception(failure(step["fail"], step.get("permanent")))
            else:
                future.set_result(step.get("answer", "ok"))
        else:
            scopes.add(step["inspect"])
            observed["inspections"].append({"step": index, "scope": step["inspect"], "record": plain(await attend.inspect(step["inspect"]))})
        await drain()
    controller.abort()
    await running
    for scope in sorted(scopes, key=lambda s: s.encode("utf-16-be", "surrogatepass")):
        observed["records"][scope] = plain(await attend.inspect(scope))
    observed["log"] = plain((await store.log.read({"after": None, "limit": 10_000}))["rows"])
    return observed
