"""Time, injected. ``fake_clock`` in ``aqi_attend.testing`` makes settling deterministic."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Protocol

from .signal import AbortSignal


class Clock(Protocol):
    def now(self) -> float:
        """ms since the epoch."""
        ...

    def at(self, at: float, fn: Callable[[], None]) -> Callable[[], None]:
        """Runs ``fn`` at or after ``at``. Returns a cancel."""
        ...


class SystemClock:
    """Wall-clock timers on the running loop, with already-due work scheduled on the next iteration."""

    def now(self) -> float:
        return time.time_ns() // 1_000_000

    def at(self, at: float, fn: Callable[[], None]) -> Callable[[], None]:
        loop = asyncio.get_running_loop()
        delay = (at - self.now()) / 1000
        handle = loop.call_soon(fn) if delay <= 0 else loop.call_later(min(delay, 2**31 / 1000), fn)
        return handle.cancel


def system_clock() -> Clock:
    return SystemClock()


async def sleep(clock: Clock, ms: float, signal: AbortSignal) -> None:
    """Returns after ``ms`` on ``clock``, or at once on abort."""
    if signal.aborted:
        return
    future = asyncio.get_running_loop().create_future()

    def wake() -> None:
        if not future.done():
            future.set_result(None)

    cancel = clock.at(clock.now() + ms, wake)
    remove = signal.add_listener(wake)
    try:
        await future
    finally:
        cancel()
        remove()


async def drain() -> None:
    """Runs the loop until nothing but timers and real I/O is left to do."""
    loop = asyncio.get_running_loop()
    ready = getattr(loop, "_ready", None)
    for _ in range(100_000):
        await asyncio.sleep(0)
        if ready is not None and len(ready) == 0:
            return
        if ready is None and _ >= 64:
            return
