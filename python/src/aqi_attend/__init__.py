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
    EventLimits,
    JudgeBudget,
    JudgeImage,
    JudgeInput,
    Judgment,
    Kind,
    Route,
    ScopeRecord,
    SettlePolicy,
    SettleReason,
    SourceBatch,
    Trace,
    Window,
)
from .errors import BackpressureError, ConflictError, IntakeError, OutboxFullError, PermanentError, is_permanent
from .fold import EventFold, fold_events
from .notice import default_notice
from .shell.intake import Intake, Prepare
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
    "EventFold",
    "EventLimits",
    "EventLog",
    "Intake",
    "IntakeError",
    "Judge",
    "JudgeBudget",
    "JudgeContext",
    "JudgeImage",
    "JudgeInput",
    "Judgment",
    "Kind",
    "OutboxFullError",
    "PermanentError",
    "Prepare",
    "Route",
    "ScopeRecord",
    "ScopeStore",
    "SettlePolicy",
    "SettleReason",
    "Sink",
    "Source",
    "SourceBatch",
    "Trace",
    "Window",
    "create_attend",
    "default_notice",
    "fold_events",
    "is_permanent",
    "log_source",
    "memory_log",
    "memory_store",
    "routes",
    "settle_policy",
    "system_clock",
    "window_id",
]
