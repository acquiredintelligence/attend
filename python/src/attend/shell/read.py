"""Replaying a stored scope, independently of any live writer."""

from __future__ import annotations

from typing import cast

from .._json import clone
from ..core.reducer import apply
from ..core.state import from_record, initial_state, to_record
from ..core.types import DEFAULT_POLICY, ApplyConfig, ApplyOutput, DeliveryMode, ScopeRecord, ScopeState
from ..store.types import ScopeStore, StoredScope


def replay(scope: str, stored: StoredScope, decisions: DeliveryMode, dead_letters: DeliveryMode) -> ScopeState:
    """Replays immutable stored data under the snapshot's policy, ignoring effects and appends."""
    snapshot = stored["snapshot"]
    policy = snapshot["policy"] if snapshot is not None else DEFAULT_POLICY
    state = from_record(snapshot["record"], policy) if snapshot is not None else initial_state(scope)
    config = ApplyConfig(policy, decisions, dead_letters, False)
    for logged in stored["tail"]:
        apply(config, state, logged, ApplyOutput())
        state.version += 1
    if state.version != stored["version"]:
        raise ValueError(f'attend: invalid stored version for scope "{scope}"')
    return state


async def read_record(store: ScopeStore, scope: str, decisions: DeliveryMode = "lane", dead_letters: DeliveryMode = "lane") -> ScopeRecord | None:
    """Reads the durable record, independently of any live writer's cached state."""
    stored = await store.load(scope)
    return None if stored is None else cast(ScopeRecord, clone(to_record(replay(scope, stored, decisions, dead_letters))))
