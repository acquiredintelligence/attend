"""Exclusive local intake contracts."""

import asyncio
from collections.abc import Awaitable, Callable

from ..core.types import SourceBatch
from ..signal import AbortSignal

type Prepare = Callable[[AbortSignal], Awaitable[SourceBatch]]


class Intake:
    """Registers ordered preparation and waits for immediate causal work."""

    def submit(self, prepare: Prepare, *, source: str | None = None, signal: AbortSignal | None = None) -> asyncio.Future[None]:
        raise NotImplementedError

    def close(self) -> Awaitable[None]:
        raise NotImplementedError
