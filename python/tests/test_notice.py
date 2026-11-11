"""``default_notice`` folding. The shared fixtures in ``conformance/fixtures/notice.json`` pin the exact text across runtimes."""

from __future__ import annotations

from typing import Any

from aqi_attend import default_notice
from aqi_attend.core.types import Decision, Entry, Kind

HINT = "Take this up when the current step is done."


def entry(id: str, kind: Kind, name: str, data: dict[str, Any], text: str | None = None) -> Entry:
    made: Entry = {"id": id, "at": 0, "name": name, "data": data, "kind": kind, "seenAt": 1}
    if text is not None:
        made["text"] = text
    return made


def decision(events: list[Entry]) -> Decision:
    return {
        "id": "w",
        "scope": "s",
        "route": "queue",
        "probabilities": {"interrupt": 0, "queue": 0, "note": 0, "ask": 0, "skip": 0, "wait": 0},
        "predicates": {},
        "confidence": None,
        "window": {"id": "w", "events": events, "reason": "closing", "waits": 0, "dropped": 0, "gap": False, "settledAt": 5},
        "at": 5,
    }


def test_a_run_of_events_that_read_the_same_is_one_line_with_its_count() -> None:
    failed = [entry(f"f{i}", "consequence", "job.failed", {"job": "sync"}) for i in range(26)]
    assert default_notice(decision(failed)) == f'job.failed {{"job":"sync"}} (26 times)\n{HINT}'
    mixed = [entry("a", "flush", "x", {}, "retry"), entry("own", "context", "agent.said", {}), entry("b", "passive", "y", {}, "retry")]
    assert default_notice(decision(mixed)) == f"retry (2 times)\n{HINT}"
    saved = [entry("a", "closing", "file.saved", {"path": "a.ts"}), entry("b", "closing", "file.saved", {"path": "b.ts"})]
    assert default_notice(decision(saved)) == f'file.saved {{"path":"a.ts"}}\nfile.saved {{"path":"b.ts"}}\n{HINT}'


def test_the_steps_of_one_continuous_act_are_its_first_and_last_step() -> None:
    turned = [entry(f"t{i}", "continuous", "dial.turned", {"value": i}) for i in range(26)]
    assert default_notice(decision(turned)) == f'dial.turned {{"value":0}} → {{"value":25}} (26 steps)\n{HINT}'
    worded = [entry(f"c{i}", "continuous", "cursor.moved", {"line": line}, f"cursor at line {line}") for i, line in enumerate([0, 4, 9])]
    assert default_notice(decision(worded)) == f"cursor at line 0 → cursor at line 9 (3 steps)\n{HINT}"
    broken = [*turned[:2], entry("z", "continuous", "zoom.changed", {"value": 2}), entry("p", "passive", "dial.turned", {"value": 3}), *turned[4:6]]
    assert default_notice(decision(broken)).split("\n") == [
        'dial.turned {"value":0} → {"value":1} (2 steps)',
        'zoom.changed {"value":2}',
        'dial.turned {"value":3}',
        'dial.turned {"value":4} → {"value":5} (2 steps)',
        HINT,
    ]


def test_the_line_limit_counts_folded_lines_and_folding_can_be_left_off() -> None:
    events = [
        *[entry(f"f{i}", "consequence", "job.failed", {"job": "sync"}) for i in range(4)],
        *[entry(f"t{i}", "continuous", "dial.turned", {"value": i}) for i in range(3)],
        entry("done", "closing", "user.said", {}, "done"),
    ]
    assert default_notice(decision(events), max_lines=2).split("\n") == ["(4 older events not shown)", 'dial.turned {"value":0} → {"value":2} (3 steps)', "done", HINT]
    assert default_notice(decision(events), max_lines=2, fold=False).split("\n") == ["(6 older events not shown)", 'dial.turned {"value":2}', "done", HINT]
    assert len(default_notice(decision(events), fold=False).split("\n")) == 9
