"""Cancellation, as ``AbortController`` and ``AbortSignal`` give it in JavaScript.

attend hands a judge, a sink and a dead-letter handler a signal and expects them to stop when it
aborts. It never cancels their tasks: a call that ignores its signal runs on, and its lease
recovers the work, exactly as in the TypeScript package.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar("T")


class AbortError(Exception):
    """The default reason of an aborted signal."""

    def __init__(self, message: str = "This operation was aborted") -> None:
        super().__init__(message)


class AbortSignal:
    """Aborts at most once, with a reason, and tells its listeners."""

    __slots__ = ("_aborted", "_listeners", "_reason")

    def __init__(self) -> None:
        self._aborted = False
        self._reason: BaseException | None = None
        self._listeners: list[Callable[[], None]] = []

    @property
    def aborted(self) -> bool:
        return self._aborted

    @property
    def reason(self) -> BaseException | None:
        return self._reason

    def throw_if_aborted(self) -> None:
        if self._aborted:
            raise self._reason if self._reason is not None else AbortError()

    def add_listener(self, listener: Callable[[], None]) -> Callable[[], None]:
        """Calls ``listener`` once on abort, or at once if already aborted. Returns a remover."""
        if self._aborted:
            listener()
            return lambda: None
        self._listeners.append(listener)

        def remove() -> None:
            with contextlib.suppress(ValueError):
                self._listeners.remove(listener)

        return remove

    def _abort(self, reason: BaseException | None) -> None:
        if self._aborted:
            return
        self._aborted = True
        self._reason = reason if reason is not None else AbortError()
        listeners, self._listeners = self._listeners, []
        for listener in listeners:
            listener()

    async def wait(self) -> None:
        """Returns once the signal aborts."""
        if self._aborted:
            return
        future = asyncio.get_running_loop().create_future()
        remove = self.add_listener(lambda: None if future.done() else future.set_result(None))
        try:
            await future
        finally:
            remove()

    async def race(self, work: Awaitable[T]) -> T:
        """Awaits ``work`` unless the signal aborts first, then cancels it and raises the reason."""
        self.throw_if_aborted()
        task = asyncio.ensure_future(work)
        aborted = asyncio.ensure_future(self.wait())
        try:
            await asyncio.wait({task, aborted}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            aborted.cancel()
            # Also when the awaiting task is cancelled, so the work never outlives its caller.
            if not task.done():
                task.cancel()
        if task.done() and not task.cancelled():
            return task.result()
        raise self._reason if self._reason is not None else AbortError()

    @staticmethod
    def any(signals: list[AbortSignal]) -> AbortSignal:
        """A signal that aborts when the first of ``signals`` does, with its reason, then stops listening to the rest."""
        controller = AbortController()
        for signal in signals:
            if signal.aborted:
                controller.abort(signal.reason)
                return controller.signal
        removers = [signal.add_listener(lambda signal=signal: controller.abort(signal.reason)) for signal in signals]

        def detach() -> None:
            for remove in removers:
                remove()

        controller.signal.add_listener(detach)
        return controller.signal

    @staticmethod
    def aborted_signal(reason: BaseException | None = None) -> AbortSignal:
        controller = AbortController()
        controller.abort(reason)
        return controller.signal


class AbortController:
    __slots__ = ("signal",)

    def __init__(self) -> None:
        self.signal = AbortSignal()

    def abort(self, reason: BaseException | None = None) -> None:
        self.signal._abort(reason)  # pyright: ignore[reportPrivateUsage]
