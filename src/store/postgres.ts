import type { Retention, ScopeStore } from './types.js';

export interface PostgresClient {
  query(text: string, values?: unknown[]): Promise<{ rows: unknown[]; rowCount: number | null }>;
}

/** A dedicated connection for LISTEN, so subscribers in other processes wake on new decisions. */
export interface PostgresListener {
  listen(channel: string, onNotify: () => void): Promise<() => Promise<void>>;
}

export interface PostgresStoreOptions {
  prefix?: string;
  retain?: Retention;
  listener?: PostgresListener;
  /** Opt-in `SET LOCAL synchronous_commit = off` per group: can lose the last commits, never corrupts. */
  asyncCommit?: boolean;
}

/** Creates the scopes, inputs, snapshots, decisions and meta tables. Idempotent. Needs PostgreSQL 13 or later. */
export async function migrate(client: PostgresClient, options?: { prefix?: string }): Promise<void> {
  throw new Error('attend: migrate is not implemented');
}

export function postgresStore(client: PostgresClient, options?: PostgresStoreOptions): ScopeStore {
  throw new Error('attend: postgresStore is not implemented');
}
