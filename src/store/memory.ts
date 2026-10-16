import type { ScopeStore } from './types.js';

export interface MemoryStoreOptions {
  decisions?: { maxRows?: number; maxAgeMs?: number };
}

/** Snapshots and inputs held by reference, a due index bucketed by second, a bounded decision ring and a random epoch. */
export function memoryStore(options?: MemoryStoreOptions): ScopeStore {
  throw new Error('attend: memoryStore is not implemented');
}

/** For tests: deep-freezes every snapshot and logged input at commit, so code that mutates committed data throws. */
export function freezingStore(inner: ScopeStore): ScopeStore {
  throw new Error('attend: freezingStore is not implemented');
}
