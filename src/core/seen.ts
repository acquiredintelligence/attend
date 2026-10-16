import type { ScopeState } from './types.js';

/**
 * Admits `id` for a batch. Returns false when it is already known, so the caller counts a
 * duplicate. Otherwise counts it for the ring and for the open entry the caller appends, and
 * stages the ring write. Eviction waits for `seenTrim`.
 */
export function seenAdmit(state: ScopeState, id: string): boolean {
  throw new Error('attend: seenAdmit is not implemented');
}

/** After the batch: evicts the oldest ids down to `seenCount`, decrementing `known` and deleting ids at 0. */
export function seenTrim(state: ScopeState, seenCount: number): void {
  throw new Error('attend: seenTrim is not implemented');
}
