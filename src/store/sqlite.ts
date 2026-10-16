import type { Retention, ScopeStore } from './types.js';

export type SqliteValue = string | number | bigint | null;

export interface SqliteStatement {
  run(...params: SqliteValue[]): { changes: number | bigint };
  get(...params: SqliteValue[]): unknown;
  all(...params: SqliteValue[]): unknown[];
}

export interface SqliteDatabase {
  exec(sql: string): void;
  prepare(sql: string): SqliteStatement;
}

export interface SqliteStoreOptions {
  /** Prepended to the table names. Letters, digits and underscores. Default 'attend_'. */
  prefix?: string;
  retain?: Retention;
}

/**
 * Given a handle: in-thread, one transaction per macrotask turn. Given a file: a worker thread owns
 * the database, so commits are asynchronous and groups form under load.
 */
export function sqliteStore(db: SqliteDatabase | { file: string; worker: true; synchronous?: 'NORMAL' | 'FULL' }, options?: SqliteStoreOptions): ScopeStore {
  throw new Error('attend: sqliteStore is not implemented');
}
