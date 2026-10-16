import type { Clock, DeadLetter, Decision, DeliveredRoute, Effect, Event, Input, JsonObject, Judge, JudgeImage, Kind, Route, ScopeRecord, ScopeState, SettlePolicy, DecisionAppend, LoggedInput, Sink, Source, Trace } from '../core/types.js';
import type { DecisionKind, DecisionRow, ScopeStore, ScopeWrite, SerializedWrite } from '../store/types.js';

/** A handler for a window that exhausted judge or sink delivery. */
export type DeadLetterHandler = (letter: DeadLetter, signal: AbortSignal) => void | Promise<void>;

/** Live context fetched when the judge is called, and again on every retry. */
export interface JudgeContext {
  facts?: JsonObject;
  /** Questions the judge answers beside the route, by name. Merged over `AttendConfig.predicates`. */
  predicates?: Record<string, string>;
}

/** Process-wide intake limits. */
export interface AdmissionOptions {
  /** Raw, loading and unapplied inputs per process before push rejects with BackpressureError. Default 20,000. */
  maxQueuedInputs?: number;
  /** Shed load once scheduler lag exceeds this many ms. */
  maxLagMs?: number;
}

/** An event as `push` takes it: `id` defaults to a random UUID, `at` to the clock and `data` to `{}`. Pass your own `id` to drop redeliveries. */
export interface EventInput {
  name: string;
  id?: string;
  at?: number;
  data?: JsonObject;
  text?: string;
}

/** Store, effects, policy and resource limits for an instance. */
export interface AttendConfig {
  /** Default `memoryStore()`. */
  store?: ScopeStore;
  judge: Judge;
  /** Pure. Called once per event at push; the event carries the result into the logged input. A map gives each listed name its kind and every other name `consequence`. */
  kind: ((event: Event) => Kind) | Readonly<Record<string, Kind>>;
  /** A Sink, or one function for every delivered route, delivers in-process through the decisions lane. `'log'`: the decision row is the delivery. Required: no silent default. */
  sink: Sink | ((decision: Decision, signal: AbortSignal) => void | Promise<void>) | 'log';
  /** A handler gets today's lane, which never gives up. `'log'` (the default): the dead-letter row in the decision log is final. */
  deadLetter?: DeadLetterHandler | 'log';
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
  /** Positive integer cap on judge calls in flight per process. Past it, calls are deferred with `judgeDeferred`. Default: no cap. */
  judgeConcurrency?: number;
  /** Idle quiescent scopes are dropped from the cache after this long. Default 300,000. */
  cache?: { idleMs?: number };
}

/** Today's RunOptions and abort semantics. The sweep pages the whole due index. */
export interface RunOptions {
  /** Without one, the run lasts until `stop()`. */
  signal?: AbortSignal;
  sweepMs?: number;
  sweepLimit?: number;
}

/** Limits a decision-log subscription by scope, route or row kind. */
export interface DecisionFilter {
  scope?: string;
  scopePrefix?: string;
  routes?: readonly DeliveredRoute[];
  kinds?: readonly DecisionKind[];
}

/** Cursor, retention horizon and cancellation for a subscription. */
export interface DecisionReadOptions {
  /** A cursor from an earlier page or from MCP Events. Null starts from now. */
  cursor?: string | null;
  maxAgeMs?: number;
  pageSize?: number;
  signal: AbortSignal;
}

/** Copied rows and the cursor checked through this page. */
export interface DecisionCursorPage {
  rows: DecisionRow[];
  /** The same `v1.<epoch>.<position>` cursor MCP subscribers get. */
  cursor: string;
  /** True when rows were skipped: an unknown epoch, or a cursor below the retention floor. */
  truncated: boolean;
}

/** Intake, inspection, subscriptions and runner lifecycle. */
export interface Attend {
  /** One event or several. May also reject with BackpressureError. */
  push(scope: string, events: EventInput | readonly EventInput[]): Promise<void>;
  /** Settles the open window at the next tick, reason `closing`, with `wait` still offered. */
  end(scope: string): Promise<void>;
  flush(scope: string): Promise<void>;
  attach(scope: string, source: Source, options?: { signal?: AbortSignal }): Promise<void>;
  /** The record as stored: load, replay and `toRecord`, then a deep copy. */
  inspect(scope: string): Promise<ScopeRecord | null>;
  /** Reads the store's decision log in-process, with the same cursors MCP subscribers use. */
  decisions(filter: DecisionFilter, options: DecisionReadOptions): AsyncIterable<DecisionCursorPage>;
  run(options?: RunOptions): Promise<void>;
  /** Stops the run, whether or not it was given a signal, and resolves when it has finished. Resolves at once when nothing is running. */
  stop(): Promise<void>;
}

/** An input and its callers, retained until commit or abandonment. */
export interface RawInput {
  input: Input;
  signal: AbortSignal | undefined;
  rejectHook: ((error: unknown) => void) | undefined;
  done: Promise<void>;
  resolve(): void;
  reject(error: unknown): void;
  count: number;
  conflicts: number;
}

/** The owned state, queued callers and persistence accounting for one scope. */
export interface ScopeSlot {
  scope: string;
  state: ScopeState | null;
  loading: Promise<void> | null;
  staged: StagedWrite | null;
  inflight: StagedWrite | null;
  queued: RawInput[];
  tailBytes: number;
  tailCount: number;
  snapshotBytes: number;
  snapshotNeeded: boolean;
  forceWrite: boolean;
  storedDue: number | null | undefined;
  lastTouch: number;
}

/** An open write and the raw inputs and effects whose fate it determines. */
export interface StagedWrite {
  write: ScopeWrite & { inputs: LoggedInput[]; appends: DecisionAppend[] };
  raw: RawInput[];
  effects: Effect[];
  bytes: number;
  snapshotBytes: number;
  state: ScopeState;
  serialized: SerializedWrite | undefined;
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
  /** Current lateness of the earliest queued entry, or zero when nothing is overdue. */
  lagMs(): number;
  stop(): void;
}

export interface SchedulerOptions {
  clock: Clock;
  /** Default 50. */
  sliceUs?: number;
  onDue(scope: string): void;
}
