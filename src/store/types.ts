import type { DecisionAppend, DeliveredRoute, LoggedInput, ScopeRecord, SettlePolicy } from '../core/types.js';

/** A snapshot is a ScopeRecord plus what replay needs: the policy and the reducer version it was taken under. */
export interface Snapshot {
  version: number;
  record: ScopeRecord;
  policy: SettlePolicy;
  reducer: number;
}

/** A scope as stored: the latest snapshot and every input committed after it, oldest first. */
export interface StoredScope {
  version: number;
  snapshot: Snapshot | null;
  tail: readonly LoggedInput[];
}

/**
 * One scope's part of a group commit. It applies iff the stored version equals
 * `expectedVersion`; 0 means no stored scope. On success the stored version becomes
 * `expectedVersion + inputs.length`. A snapshot is always taken at the write's final version: it
 * replaces the stored one and trims every input at or below its version, so an adapter writes no
 * input rows for a write that carries one. Everything in one write lands together or not at all.
 */
export interface ScopeWrite {
  scope: string;
  expectedVersion: number;
  inputs: readonly LoggedInput[];
  snapshot: Snapshot | null;
  /** The coarse due value, already rounded up to the second by the shell, and stored as given. Undefined: unchanged. Null: clear it. */
  due?: number | null;
  appends: readonly DecisionAppend[];
  serialized?: SerializedWrite;
}

/**
 * The JSON of a write's pieces, made when each piece was produced, so a group commit only binds
 * strings. `inputs[i]` is `JSON.stringify(write.inputs[i])`; `snapshot` is
 * `JSON.stringify(write.snapshot.record)`, or null without a snapshot; `appends[i]` is
 * `JSON.stringify(write.appends[i].body)`. The shell sets it on every write to a durable store,
 * and the memory store ignores it.
 */
export interface SerializedWrite {
  inputs: string[];
  snapshot: string | null;
  appends: string[];
}

export type CommitOutcome = 'ok' | 'conflict';

/** A keyset position in the due index: ascending by `due`, then `scope`. */
export interface DueKey {
  due: number;
  scope: string;
}

/**
 * The store port. `commit` resolves once the group is durable, with one outcome per write, in
 * order. Writes for different scopes are independent: a conflict changes nothing for its scope,
 * and the others commit. A group that names one scope twice rejects. A rejected `commit` applied
 * nothing. Proven by `scopeStoreConformance` and `decisionLogConformance`.
 */
export interface ScopeStore {
  /** False for the memory store: snapshots and inputs survive the instance, not the process. */
  readonly durable: boolean;
  load(scope: string): Promise<StoredScope | null>;
  commit(writes: readonly ScopeWrite[]): Promise<readonly CommitOutcome[]>;
  /** Scopes whose due value is <= now, ascending by (due, scope), strictly after `after`, at most `limit`. A read, not a claim. */
  due(now: number, limit: number, after?: DueKey): Promise<DueKey[]>;
  /** Forgets the scope: a later load is null, it is no longer due, and a commit at version 0 succeeds. */
  remove(scope: string): Promise<void>;
  readonly log: DecisionLog;
  /** Releases what the store itself opened, such as a worker. Never closes a client the app passed in. */
  close?(): Promise<void>;
}

/** Opaque, and ordered by `DecisionLog.compare`: a memory sequence number, a SQLite rowid, a Postgres `txid.n`, a Redis stream id. */
export type Position = string;
export type DecisionKind = DecisionAppend['kind'];
export type DecisionRow = DecisionAppend & { position: Position };

export interface DecisionQuery {
  /** Strictly after this position; null starts from the oldest retained row. */
  after: Position | null;
  scope?: string;
  scopePrefix?: string;
  kinds?: readonly DecisionKind[];
  /** Rows whose route is one of these. A row with a null route never matches. */
  routes?: readonly DeliveredRoute[];
  /** Skip rows decided before this instant. */
  sinceAt?: number;
  limit: number;
  /** Rows examined before stopping with `more: true`. Default 4 × `limit`. */
  scanLimit?: number;
}

export interface DecisionPage {
  /** Matching rows, oldest first. */
  rows: DecisionRow[];
  /**
   * The position checked up to, whether or not anything matched: the cursor to hand out. When
   * `more` is false it is the head the read saw (never behind `after`, and null on an empty log).
   * When `more` is true it is the last row examined, so reading on from it skips nothing.
   */
  checked: Position | null;
  /** True when the read stopped at `limit` or `scanLimit` with rows left past `checked`; false when it reached the head. */
  more: boolean;
}

export interface LogBounds {
  /** The retention floor: the oldest readable position. */
  first: Position | null;
  /** The head: the newest readable position. */
  last: Position | null;
}

export interface Retention {
  olderThanMs?: number;
  maxRows?: number;
}

/**
 * Decisions and dead letters in one total order per store, written only by `ScopeStore.commit`
 * in the same commit as the input that produced them. An append whose (kind, id) is already
 * logged is skipped, and its write still commits. A reader never sees a row appear below a
 * position it has already read.
 */
export interface DecisionLog {
  /** Names the log's incarnation: random per instance for memory, persistent for durable stores. */
  epoch(): Promise<string>;
  bounds(): Promise<LogBounds>;
  read(query: DecisionQuery): Promise<DecisionPage>;
  find(kind: DecisionKind, id: string): Promise<DecisionRow | null>;
  /** Resolves when the head passes `after`, after `blockMs`, or on abort. Never rejects. */
  wait(after: Position | null, blockMs: number, signal: AbortSignal): Promise<void>;
  /**
   * Drops rows from the oldest end: keeps at most `maxRows` newest rows, and drops leading rows
   * whose `at` is older than `Date.now() - olderThanMs`. Never drops the head.
   */
  trim(retention: Retention): Promise<void>;
  /** Negative, zero or positive, as `a` comes before, equals or follows `b`. */
  compare(a: Position, b: Position): number;
}
