import type { Clock, DeadLetter, DeliveredRoute, Effect, Event, Input, JsonObject, Judge, JudgeImage, Kind, Route, ScopeRecord, ScopeState, SettlePolicy, Sink, Source, Trace } from '../core/types.js';
import type { DecisionKind, DecisionRow, ScopeStore, ScopeWrite } from '../store/types.js';

export type DeadLetterHandler = (letter: DeadLetter, signal: AbortSignal) => void | Promise<void>;

/** Live context fetched when the judge is called, and again on every retry. */
export interface JudgeContext {
  facts?: JsonObject;
  /** Questions the judge answers beside the route, by name. Merged over `AttendConfig.predicates`. */
  predicates?: Record<string, string>;
}

export interface AdmissionOptions {
  /** Raw, loading and unapplied inputs per process before push rejects with BackpressureError. Default 20,000. */
  maxQueuedInputs?: number;
  /** Shed load once scheduler lag exceeds this many ms. */
  maxLagMs?: number;
}

export interface AttendConfig {
  store: ScopeStore;
  judge: Judge;
  /** Pure. Called once per event at push; the event carries the result into the logged input. */
  kind: (event: Event) => Kind;
  /** A Sink delivers in-process through the decisions lane. `'log'`: the decision row is the delivery. Required: no silent default. */
  sink: Sink | 'log';
  /** A handler gets today's lane, which never gives up. `'log'`: the dead-letter row is final. Required. */
  deadLetter: DeadLetterHandler | 'log';
  settle?: Partial<SettlePolicy>;
  /** Share of the judge's budget a window may fill before it settles with reason `size`. Default 0.5. */
  windowShare?: number;
  /**
   * In (0, 1]. When set, the route acts (interrupt or ask) iff p(interrupt) + p(ask) is at least
   * this, applied before the verdict is logged. Absent: the judge's route, unchanged.
   */
  actThreshold?: number;
  guidance?: string;
  criteria?: Partial<Record<Route, string>>;
  predicates?: Record<string, string>;
  context?: (scope: string, signal: AbortSignal) => JudgeContext | Promise<JudgeContext>;
  capture?: (scope: string, signal: AbortSignal) => Promise<JudgeImage[] | JudgeImage | null>;
  captureTimeoutMs?: number;
  observe?: (trace: Trace) => void;
  clock?: Clock;
  admission?: AdmissionOptions;
  /** Judge calls in flight per process. Past it, calls are deferred with `judgeDeferred`. Default: no cap. */
  judgeConcurrency?: number;
  /** Idle quiescent scopes are dropped from the cache after this long. Default 300,000. */
  cache?: { idleMs?: number };
}

/** Today's RunOptions and abort semantics. The sweep pages the whole due index. */
export interface RunOptions {
  signal: AbortSignal;
  sweepMs?: number;
  sweepLimit?: number;
}

export interface DecisionFilter {
  scope?: string;
  scopePrefix?: string;
  routes?: readonly DeliveredRoute[];
  kinds?: readonly DecisionKind[];
}

export interface DecisionReadOptions {
  /** A cursor from an earlier page or from MCP Events. Null starts from now. */
  cursor?: string | null;
  maxAgeMs?: number;
  pageSize?: number;
  signal: AbortSignal;
}

export interface DecisionCursorPage {
  rows: DecisionRow[];
  /** The same `v1.<epoch>.<position>` cursor MCP subscribers get. */
  cursor: string;
  /** True when rows were skipped: an unknown epoch, or a cursor below the retention floor. */
  truncated: boolean;
}

export interface Attend {
  /** As today. May also reject with BackpressureError. */
  push(scope: string, events: Event[]): Promise<void>;
  /** Settles the open window at the next tick, reason `closing`, with `wait` still offered. */
  end(scope: string): Promise<void>;
  flush(scope: string): Promise<void>;
  attach(scope: string, source: Source, options?: { signal?: AbortSignal }): Promise<void>;
  /** The record as stored: load, replay and `toRecord`, then a deep copy. */
  inspect(scope: string): Promise<ScopeRecord | null>;
  /** Reads the store's decision log in-process, with the same cursors MCP subscribers use. */
  decisions(filter: DecisionFilter, options: DecisionReadOptions): AsyncIterable<DecisionCursorPage>;
  run(options: RunOptions): Promise<void>;
}

/** Shell-internal: one per scope in the shell's slot map. */
export interface ScopeSlot {
  state: ScopeState | null;
  loading: Promise<void> | null;
  /** The write in the open group, still taking inputs. Its waiters resolve when it commits. */
  staged: StagedWrite | null;
  /** The write in a group being committed. While it is set, inputs queue raw and same-source events merge. */
  inflight: StagedWrite | null;
  queued: Input[];
  tailBytes: number;
  snapshotBytes: number;
  lastTouch: number;
}

export interface StagedWrite {
  write: ScopeWrite;
  /** Kept so a conflict can reload and re-apply them, up to 8 times. */
  raw: Input[];
  effects: Effect[];
  waiters: { resolve: () => void; reject: (error: unknown) => void }[];
}

/** Shell-internal: smart batching, one group in flight per store, writes sorted by scope, bisect on failure. */
export interface CommitQueue {
  stage(slot: ScopeSlot): void;
  /** Resolves when everything staged so far has committed or been rejected. */
  drain(): Promise<void>;
}

/** Shell-internal: one min-heap, one native timer, lazy re-arm, `sliceUs` of work per macrotask. */
export interface Scheduler {
  arm(scope: string, dueAt: number | null): void;
  /** How late the most recent due entry fired. */
  lagMs(): number;
  stop(): void;
}

export interface SchedulerOptions {
  clock: Clock;
  /** Default 50. */
  sliceUs?: number;
  onDue(scope: string): void;
}
