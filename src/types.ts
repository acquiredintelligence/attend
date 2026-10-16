import type { ScopeRecord } from './core/types.js';

/** Domain types shared by the reducer and public adapters. */
export * from './core/types.js';
/** The log and snapshot store port. */
export * from './store/types.js';

/**
 * The legacy port implemented by attend/sqlite, attend/postgres and attend/redis until P6b.
 */
export interface StateStore {
  /** The record for `scope`, deep-equal to what was last put, or null if none. */
  get(scope: string): Promise<ScopeRecord | null>;
  /**
   * Write `record` iff the stored version equals `expectedVersion`; 0 means "no record
   * exists". Among N concurrent puts with the same expected version exactly one returns
   * 'ok'; the others return 'conflict' and change nothing.
   */
  put(record: ScopeRecord, expectedVersion: number): Promise<'ok' | 'conflict'>;
  /** Scopes whose `dueAt` is non-null and <= `now`, ascending by `dueAt`, at most `limit`. A read, not a claim. */
  due(now: number, limit: number): Promise<string[]>;
  /** Forget the scope. A later get is null and a later put at version 0 succeeds. */
  remove(scope: string): Promise<void>;
}
