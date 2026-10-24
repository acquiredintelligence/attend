"""attend: decide when an event should wake your agent.

The Python port of the TypeScript package ``@acquiredintelligence/attend``: the same design, the
same semantics and the same decision ids.
"""

from .attend import Attend, DeadLetterHandler, Judge, JudgeContext, Sink, Source, create_attend, routes, settle_policy
from .clock import Clock, system_clock
from .core.ids import window_id
from .core.types import (
    DEFAULT_POLICY,
    KINDS,
    ROUTES,
    DeadLetter,
    Decision,
    DeliveredRoute,
    Entry,
    Event,
    JudgeBudget,
    JudgeImage,
    JudgeInput,
    Judgment,
    Kind,
    Route,
    ScopeRecord,
    SettlePolicy,
    SettleReason,
    Trace,
    Window,
)
from .errors import BackpressureError, ConflictError, OutboxFullError, PermanentError, is_permanent
from .notice import default_notice
from .signal import AbortController, AbortSignal
from .sources import EventLog, log_source, memory_log
from .store.memory import memory_store
from .store.types import ScopeStore

__all__ = [
    "DEFAULT_POLICY",
    "KINDS",
    "ROUTES",
    "AbortController",
    "AbortSignal",
    "Attend",
    "BackpressureError",
    "Clock",
    "ConflictError",
    "DeadLetter",
    "DeadLetterHandler",
    "Decision",
    "DeliveredRoute",
    "Entry",
    "Event",
    "EventLog",
    "Judge",
    "JudgeBudget",
    "JudgeContext",
    "JudgeImage",
    "JudgeInput",
    "Judgment",
    "Kind",
    "OutboxFullError",
    "PermanentError",
    "Route",
    "ScopeRecord",
    "ScopeStore",
    "SettlePolicy",
    "SettleReason",
    "Sink",
    "Source",
    "Trace",
    "Window",
    "create_attend",
    "default_notice",
    "is_permanent",
    "log_source",
    "memory_log",
    "memory_store",
    "routes",
    "settle_policy",
    "system_clock",
    "window_id",
]
