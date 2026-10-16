import type { ScopeRecord, ScopeState, SettlePolicy } from './types.js';

export function initialState(scope: string, policy: SettlePolicy): ScopeState {
  throw new Error('attend: initialState is not implemented');
}

/** Builds owned state from a snapshot. Copies every array it will mutate and never mutates `record`. O(W + S + outbox). */
export function fromRecord(record: ScopeRecord, policy: SettlePolicy): ScopeState {
  throw new Error('attend: fromRecord is not implemented');
}

/** The state as a fresh `ScopeRecord`, deep-equal to the copying reducer's. Shares immutable parts. O(W + S + outbox). */
export function toRecord(state: ScopeState): ScopeRecord {
  throw new Error('attend: toRecord is not implemented');
}

/** True when the scope holds nothing: an empty window, no batch, empty lanes and `dueAt === null`. Such a scope may be evicted from the cache. */
export function quiescent(state: ScopeState): boolean {
  throw new Error('attend: quiescent is not implemented');
}

/** Checks the state's invariants after an apply, in tests and debug builds. Throws, naming the broken invariant. */
export function assertInvariants(state: ScopeState, policy: SettlePolicy): void {
  throw new Error('attend: assertInvariants is not implemented');
}
