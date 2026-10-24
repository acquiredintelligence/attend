"""Window ids: FNV-1a 64 over ``len:part`` for the scope and each event id, as 16 hex characters."""

from __future__ import annotations

from collections.abc import Iterable

from .._json import utf8_bytes
from .types import FnvState

_OFFSET = 0xCBF29CE484222325
_PRIME = 0x100000001B3
_MASK = 0xFFFFFFFFFFFFFFFF


def window_id(scope: str, ids: Iterable[str]) -> str:
    """The id of the window over ``ids`` in ``scope``; identical to the TypeScript ``windowId``."""
    state = fnv_start(scope)
    for id in ids:
        fold_id(state, id)
    return fnv_hex(state)


def fnv_start(scope: str) -> FnvState:
    """A fresh FNV-1a 64 state with ``len:scope`` already folded."""
    state = FnvState(_OFFSET)
    fold_id(state, scope)
    return state


def fold_id(state: FnvState, id: str) -> None:
    """Folds ``len:id`` (UTF-8 byte length, a colon, the UTF-8 bytes) into ``state`` in place."""
    data = utf8_bytes(id)
    h = state.value
    for byte in b"%d:" % len(data) + data:
        h = ((h ^ byte) * _PRIME) & _MASK
    state.value = h


def fnv_hex(state: FnvState) -> str:
    """The state as 16 lowercase hex characters."""
    return f"{state.value:016x}"
