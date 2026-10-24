"""The store port: one snapshot and a short tail of inputs per scope, a due index and the decision log."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Literal, NotRequired, TypedDict

from ..core.types import DeadLetter, Decision, DecisionAppend, DeliveredRoute, LoggedInput, ScopeRecord, SettlePolicy
from ..signal import AbortSignal


class Snapshot(TypedDict):
    """A ScopeRecord plus what replay needs: the policy and the reducer version it was taken under."""

    version: int
    record: ScopeRecord
    policy: SettlePolicy
    reducer: int


class StoredScope(TypedDict):
    """A scope as stored: the latest snapshot and every input committed after it, oldest first."""

    version: int
    snapshot: Snapshot | None
    tail: list[LoggedInput]


class SerializedWrite(TypedDict):
    """The JSON of a write's pieces, made when each piece was produced, so a group commit only binds strings."""

    inputs: list[str]
    snapshot: str | None
    appends: list[str]


class DecisionRowDecision(TypedDict):
    kind: Literal["decision"]
    id: str
    scope: str
    at: float
    route: DeliveredRoute
    body: Decision
    position: str


class DecisionRowDeadLetter(TypedDict):
    kind: Literal["deadLetter"]
    id: str
    scope: str
    at: float
    route: DeliveredRoute | None
    body: DeadLetter
    position: str


type DecisionRow = DecisionRowDecision | DecisionRowDeadLetter
type DecisionKind = Literal["decision", "deadLetter"]
type CommitOutcome = Literal["ok", "conflict"]


class ScopeWrite(TypedDict):
    """One scope's part of a group commit.

    It applies iff the stored version equals ``expectedVersion`` (0 means no stored scope). On
    success the stored version becomes ``expectedVersion + len(inputs)``. A snapshot is taken at
    the write's final version: it replaces the stored one and trims every input at or below it.
    ``due`` absent: unchanged; None: clear it. Everything in one write lands together or not at all.
    """

    scope: str
    expectedVersion: int
    inputs: list[LoggedInput]
    snapshot: Snapshot | None
    due: NotRequired[float | None]
    appends: list[DecisionAppend]
    serialized: NotRequired[SerializedWrite]


class DueKey(TypedDict):
    """A keyset position in the due index: ascending by ``due``, then ``scope``."""

    due: float
    scope: str


class DecisionQuery(TypedDict):
    after: str | None
    scope: NotRequired[str]
    scopePrefix: NotRequired[str]
    kinds: NotRequired[Sequence[DecisionKind]]
    routes: NotRequired[Sequence[DeliveredRoute]]
    sinceAt: NotRequired[float]
    limit: int
    scanLimit: NotRequired[int]


class DecisionPage(TypedDict):
    rows: list[DecisionRow]
    checked: str | None
    more: bool


class LogBounds(TypedDict):
    first: str | None
    last: str | None


class Retention(TypedDict, total=False):
    olderThanMs: float
    maxRows: int


class DecisionLog(ABC):
    """Decisions and dead letters in one total order per store, written only by ``ScopeStore.commit``."""

    @abstractmethod
    async def epoch(self) -> str: ...

    @abstractmethod
    async def bounds(self) -> LogBounds: ...

    @abstractmethod
    async def read(self, query: DecisionQuery) -> DecisionPage: ...

    @abstractmethod
    async def find(self, kind: DecisionKind, id: str) -> DecisionRow | None: ...

    @abstractmethod
    async def wait(self, after: str | None, block_ms: float, signal: AbortSignal) -> None:
        """Returns when the head passes ``after``, after ``block_ms``, or on abort. Never raises."""

    @abstractmethod
    async def trim(self, retention: Retention) -> None: ...

    @abstractmethod
    def compare(self, a: str, b: str) -> int: ...


class ScopeStore(ABC):
    """The store port. ``commit`` returns once the group is durable, with one outcome per write, in order.

    Writes for different scopes are independent: a conflict changes nothing for its scope, and the
    others commit. A group that names one scope twice raises. A failed ``commit`` applied nothing.
    Every value a store returns is plain data it never changes afterwards.
    """

    durable: bool
    log: DecisionLog

    @abstractmethod
    async def load(self, scope: str) -> StoredScope | None: ...

    @abstractmethod
    async def commit(self, writes: Sequence[ScopeWrite]) -> list[CommitOutcome]: ...

    @abstractmethod
    async def due(self, now: float, limit: int, after: DueKey | None = None) -> list[DueKey]:
        """Scopes whose due value is <= now, ascending by (due, scope), strictly after ``after``, at most ``limit``."""

    @abstractmethod
    async def remove(self, scope: str) -> None: ...

    async def close(self) -> None:
        """Releases what the store itself opened. Never closes a client the app passed in."""
        return None
