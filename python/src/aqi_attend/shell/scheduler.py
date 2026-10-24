"""One lazy min-heap and one clock timer, with bounded firing slices."""

from __future__ import annotations

import asyncio
import heapq
import time
from collections.abc import Callable

from ..clock import Clock


class Scheduler:
    """Fires ``on_due(scope)`` once each armed scope's earliest instant passes."""

    def __init__(self, clock: Clock, on_due: Callable[[str], None], slice_us: float = 50) -> None:
        self._clock = clock
        self._on_due = on_due
        self._slice_us = slice_us
        self._heap: list[tuple[float, int, str]] = []
        self._queued: dict[str, float] = {}
        self._seq = 0
        self._timer: tuple[float, Callable[[], None]] | None = None
        self._continuation: asyncio.Handle | None = None
        self._firing = False

    def arm(self, scope: str, at: float | None) -> None:
        if at is None:
            return
        prior = self._queued.get(scope)
        if prior is not None and prior <= at:
            return
        self._queued[scope] = at
        heapq.heappush(self._heap, (at, self._seq, scope))
        self._seq += 1
        self._arm_head()

    def lag_ms(self) -> float:
        return max(0, self._clock.now() - self._heap[0][0]) if self._heap else 0

    def stop(self) -> None:
        if self._timer is not None:
            self._timer[1]()
        if self._continuation is not None:
            self._continuation.cancel()
        self._timer = None
        self._continuation = None
        self._heap.clear()
        self._queued.clear()

    def _arm_head(self) -> None:
        if not self._heap or self._firing or self._continuation is not None:
            return
        at = self._heap[0][0]
        if self._timer is not None and self._timer[0] <= at:
            return
        if self._timer is not None:
            self._timer[1]()
        self._timer = (at, self._clock.at(at, self._fire))

    def _fire(self) -> None:
        self._timer = None
        self._continuation = None
        self._firing = True
        started = time.perf_counter()
        heap = self._heap
        while heap and heap[0][0] <= self._clock.now():
            at, _, scope = heapq.heappop(heap)
            if self._queued.get(scope) == at:
                del self._queued[scope]
                self._on_due(scope)
            if (time.perf_counter() - started) * 1_000_000 >= self._slice_us and heap and heap[0][0] <= self._clock.now():
                self._firing = False
                self._continuation = asyncio.get_running_loop().call_soon(self._fire)
                return
        self._firing = False
        self._arm_head()
