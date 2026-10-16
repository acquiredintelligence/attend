/**
 * The domain. Everything here is plain JSON: a scope's whole durable state is one
 * `ScopeRecord`, and `step` in ./step.ts is a pure function over it.
 */

export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };
export type JsonObject = { [key: string]: JsonValue };

// ─── events and kinds ────────────────────────────────────────────────────────

/** One event as a source hands it in. `id` is unique within the scope and is the dedup key. */
export interface Event {
  id: string;
  /** ms since the epoch, as the source reports it. Informational: timers use `Entry.seenAt`. */
  at: number;
  name: string;
  data: JsonObject;
  /** One plain line describing the event. The judge never writes text; notices are built from these. */
  text?: string;
}

export const KINDS = ['closing', 'flush', 'continuous', 'passive', 'consequence', 'context'] as const;

/**
 * The settle role of an event. `closing`, `continuous` and `passive` are acts: things that
 * happen in the scope and can call for the agent, whether the user did them or an outside
 * party did, such as a deploy that failed. A flush is not an act but a direct address. The
 * last two never settle a window on their own.
 * - closing: ends an act; the window settles after a short grace so consequences land.
 * - flush: direct address; settles at once, with wait not offered, and is never shed.
 * - continuous: one step of an ongoing act; settles once no further step has arrived for a gap.
 * - passive: a dwell or look; settles after a quiet period.
 * - consequence: output that follows from an act; attaches to the act that caused it.
 * - context: the agent's own earlier output; rides along.
 */
export type Kind = (typeof KINDS)[number];

/** An event in a window: the event, the kind it was given at ingest, and the local clock then. */
export type Entry = Event & { kind: Kind; seenAt: number };

// ─── routes ──────────────────────────────────────────────────────────────────

export const ROUTES = ['interrupt', 'queue', 'note', 'ask', 'skip', 'wait'] as const;

/**
 * - interrupt: deliver now, even mid-turn.
 * - queue: deliver when the agent's current turn ends.
 * - note: context for the next turn; no response.
 * - ask: offer the user a prompt to call the agent in.
 * - skip: nothing.
 * - wait: hold the window and judge it again; never leaves the reducer.
 */
export type Route = (typeof ROUTES)[number];
/** What a judgment resolves to once `wait` is spent. */
export type DecidedRoute = Exclude<Route, 'wait'>;
/** What reaches a sink. A sink never sees `skip`. */
export type DeliveredRoute = Exclude<DecidedRoute, 'skip'>;

export type SettleReason = 'closing' | 'flush' | 'stream_quiet' | 'passive_quiet' | 'overflow' | 'age' | 'forced';

// ─── the settle policy ───────────────────────────────────────────────────────

/** Round starting points, not tuned values. Every number is in ms unless it is a count. */
export interface SettlePolicy {
  closingGraceMs: number;
  streamGapMs: number;
  passiveQuietMs: number;
  waitHorizonMs: number;
  maxWaits: number;
  /** Entries one window holds. Consequences and context leave first; a window of only acts settles as `overflow`. */
  maxEvents: number;
  /** A window settles this long after its first act however busy it is; older act-less entries are pruned. */
  maxWindowMs: number;
  /** Backoff before each retry of a judge call, a delivery or a source; its length is the retry budget for the first two. */
  retryBackoffMs: number[];
  /** Aborts a judge call. Its lease is twice this, so a slow call is never duplicated while live. */
  judgeTimeoutMs: number;
  /** Lease on a delivery. Expiry counts as one failed attempt. */
  deliveryTimeoutMs: number;
  /** Verdicts kept as judge context. */
  recentCount: number;
  /** Event ids kept for dedup. */
  seenCount: number;
  /** Undelivered decisions per scope before `push` and a source's `emit` reject. */
  maxOutbox: number;
  /** Undelivered dead letters per scope before `push` and a source's `emit` reject. */
  maxDeadLetters: number;
  /** Cap on the backoff between dead-letter deliveries, which never give up. */
  deadLetterBackoffMaxMs: number;
}

export const DEFAULT_POLICY: SettlePolicy = {
  closingGraceMs: 500,
  streamGapMs: 1000,
  passiveQuietMs: 4000,
  waitHorizonMs: 10_000,
  maxWaits: 2,
  maxEvents: 250,
  maxWindowMs: 60_000,
  retryBackoffMs: [1000, 4000, 16_000],
  judgeTimeoutMs: 15_000,
  deliveryTimeoutMs: 10_000,
  recentCount: 8,
  seenCount: 500,
  maxOutbox: 1000,
  maxDeadLetters: 1000,
  deadLetterBackoffMaxMs: 60_000,
};

// ─── the record ──────────────────────────────────────────────────────────────

/** The open window. Always present; empty entries mean no window is open. */
export interface Buffer {
  entries: Entry[];
  /** Non-act entries shed by overflow or age. Acts are never shed. */
  dropped: number;
  /** The source reported lost events while this window was open, or before it opened. */
  gap: boolean;
  /** Set by `flush`; the window settles at the next tick whatever its deadline says. */
  forcedAt: number | null;
}

/** A settled window: what the judge sees and what a decision carries. */
export interface Window {
  /** Hash of the scope and the event ids; it becomes the decision id. */
  id: string;
  /** Oldest first, kinds included. */
  events: Entry[];
  /**
   * Why the judge sees it: `flush` when the window holds a direct address, `forced` when
   * `flush()` forced it, else the deadline that fired. The first two never offer `wait` and
   * survive a merge with later events.
   */
  reason: SettleReason;
  waits: number;
  dropped: number;
  gap: boolean;
  settledAt: number;
}

/**
 * `attempt` numbers the judge calls for this window and only grows, across a wait and across a
 * merge, so a result from an earlier call never matches a later one. `failures` counts the
 * failed calls since the last wait and indexes the backoff; a wait resets it.
 */
export type BatchPhase =
  | { phase: 'judging'; attempt: number; failures: number; leaseUntil: number; offerWait: boolean }
  | { phase: 'waiting'; attempt: number; until: number }
  | { phase: 'retrying'; attempt: number; failures: number; at: number; offerWait: boolean };

/** The one settled window. One slot: that is single-flight. */
export interface Batch {
  window: Window;
  phase: BatchPhase;
}

/** One routed verdict, kept as judge context. */
export interface Verdict {
  id: string;
  route: DecidedRoute;
  at: number;
}

/**
 * What a sink receives. `id` is the window id, so the same window redelivered has the
 * same id (the sink's idempotency key) and a merged, re-judged window has a different one.
 */
export interface Decision {
  id: string;
  scope: string;
  route: DeliveredRoute;
  /** Normalised over every route; routes the judge was not offered are 0. */
  probabilities: Record<Route, number>;
  /** Answers to the ride-along questions, by name. */
  predicates: Record<string, number>;
  window: Window;
  at: number;
}

/** A window no judge or sink could take. Delivered to the dead-letter handler and never dropped. */
export interface DeadLetter {
  id: string;
  scope: string;
  /** Which call gave up. */
  stage: 'judge' | 'sink';
  window: Window;
  /** The decision the sink could not take; null when the judge gave up. */
  decision: Decision | null;
  message: string;
  permanent: boolean;
  attempts: number;
  at: number;
}

/**
 * `attempt` numbers the delivery attempts for this id and only grows, across the escalation to
 * a dead letter too. `failures` counts the failed attempts in the current lane and indexes the
 * backoff, so a fresh dead letter starts at the first rung. A retry is its own phase, never a
 * lease, so a backoff is never counted as a failed attempt.
 */
export type DeliveryPhase =
  | { phase: 'inFlight'; attempt: number; failures: number; leaseUntil: number }
  | { phase: 'retrying'; attempt: number; failures: number; at: number };

export type DeliveryItem = { id: string } & (
  | { kind: 'decision'; decision: Decision }
  | { kind: 'deadLetter'; letter: DeadLetter }
);

/** An outbox entry. */
export interface Delivery {
  item: DeliveryItem;
  phase: DeliveryPhase;
}

export type OutboxLane = 'decisions' | 'deadLetters';

/**
 * Two lanes, each delivered one at a time, oldest first, so a dead-letter handler that is
 * down never holds a decision back. A decision that gives up leaves `decisions` and joins the
 * end of `deadLetters`.
 */
export type Outbox = Record<OutboxLane, Delivery[]>;

/**
 * A scope's entire durable state. The store indexes `scope`, `version` and `dueAt` and
 * treats the rest as an opaque document. Every field has one writer: `step`.
 */
export interface ScopeRecord {
  /** Compare-and-swap token; 0 before the first put. */
  version: number;
  scope: string;
  /** Each attached source's position after its last ingested event, by `Source.key`; null when that source has none. */
  cursors: Record<string, string | null>;
  /** Recent event ids, oldest first, bounded by `seenCount`. */
  seen: string[];
  buffer: Buffer;
  batch: Batch | null;
  outbox: Outbox;
  /** Oldest first, bounded by `recentCount`. */
  recent: Verdict[];
  /** The next instant the record wants a tick; null when nothing is pending. */
  dueAt: number | null;
}

// ─── the judge's answer ──────────────────────────────────────────────────────

/** What a judge returns. Validated against the offered routes inside `step`. */
export interface Judgment {
  route: Route;
  /** Per offered route. Missing routes count as 0; the reducer renormalises. */
  probabilities?: Partial<Record<Route, number>>;
  predicates?: Record<string, number>;
}

// ─── inputs ──────────────────────────────────────────────────────────────────

export interface EventsInput {
  kind: 'events';
  events: Event[];
  /** The `Source.key` these came from. A push has none, and then `cursor` is ignored. */
  source?: string;
  /** Absent: leave the source's cursor as it is. Present (string or null): replace it. */
  cursor?: string | null;
  /** The source reported a gap before these events. */
  truncated?: boolean;
}

/** Settle the open window at the next tick, reason `forced`. */
export interface FlushInput {
  kind: 'flush';
}

export type Input =
  | EventsInput
  | FlushInput
  | { kind: 'tick' }
  | { kind: 'judged'; batchId: string; attempt: number; judgment: Judgment }
  | { kind: 'judgeFailed'; batchId: string; attempt: number; message: string; permanent: boolean }
  | { kind: 'delivered'; deliveryId: string; attempt: number }
  | { kind: 'deliveryFailed'; deliveryId: string; attempt: number; message: string; permanent: boolean };

// ─── effects ─────────────────────────────────────────────────────────────────

export interface JudgeEffect {
  kind: 'judge';
  scope: string;
  batchId: string;
  attempt: number;
  window: Window;
  /** The routes the judge may answer with; `wait` present iff offered. */
  offered: Route[];
  recent: Verdict[];
  leaseUntil: number;
}

export interface InFlightDelivery {
  item: DeliveryItem;
  phase: Extract<DeliveryPhase, { phase: 'inFlight' }>;
}

export interface DeliverEffect {
  kind: 'deliver';
  scope: string;
  delivery: InFlightDelivery;
}

export type Trace =
  | { kind: 'ingested'; scope: string; count: number; duplicates: number }
  | { kind: 'gap'; scope: string; source: string | null; cursor: string | null }
  | { kind: 'dropped'; scope: string; entries: Entry[]; why: 'overflow' | 'age' }
  | { kind: 'settled'; scope: string; batchId: string; reason: SettleReason; count: number; merged: boolean }
  | { kind: 'judging'; scope: string; batchId: string; attempt: number; offerWait: boolean }
  | { kind: 'judged'; scope: string; batchId: string; route: DecidedRoute }
  | { kind: 'waiting'; scope: string; batchId: string; waits: number; until: number }
  | { kind: 'retrying'; scope: string; stage: 'judge' | 'sink' | 'deadLetter' | 'source'; id: string; attempt: number; at: number; message: string }
  | { kind: 'delivering'; scope: string; deliveryId: string; attempt: number }
  | { kind: 'delivered'; scope: string; deliveryId: string }
  | { kind: 'deadLettered'; scope: string; letter: DeadLetter }
  | { kind: 'stale'; scope: string; input: Input['kind']; why: string }
  | { kind: 'conflict'; scope: string; version: number }
  | { kind: 'abandoned'; scope: string; input: Input['kind'] }
  | { kind: 'failed'; scope: string; input: Input['kind']; message: string };

export type TraceEffect = { kind: 'trace'; trace: Trace };

export type Effect = JudgeEffect | DeliverEffect | TraceEffect;

export interface Stepped<E extends Effect = Effect> {
  record: ScopeRecord;
  effects: E[];
}

// ─── ports ───────────────────────────────────────────────────────────────────

/**
 * Holds one `ScopeRecord` per scope. The store reads `scope`, `version` and `dueAt`
 * and keeps the rest as an opaque JSON document.
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

export interface LogEntry {
  /** Opaque, strictly increasing in read order within a scope. */
  position: string;
  event: Event;
}

/**
 * An append-only log per scope for producers outside the process. At-least-once: the
 * reducer dedups by `event.id`, so the log never has to.
 */
export interface EventLog {
  /** Durable before resolving. Order within one call is preserved. */
  append(scope: string, events: Event[]): Promise<void>;
  /**
   * Entries strictly after `after` (`null`: from the oldest retained entry, never from the
   * head), oldest first, at most `limit`. With `blockMs`, an adapter that can wait returns
   * early when an entry arrives and returns an empty page at the timeout; one that cannot
   * returns at once.
   */
  read(scope: string, after: string | null, options: { limit: number; blockMs?: number }): Promise<LogEntry[]>;
}

export interface SourceBatch {
  events: Event[];
  /** The position after these events; omit when the source has no cursor. */
  cursor?: string | null;
  truncated?: boolean;
}

/**
 * Something that feeds a scope. `emit` resolves once the batch is persisted, and the
 * source must await it before producing more: that is the backpressure. Rejecting ends
 * the subscription with that error; `attach` resubscribes after a backoff unless the error
 * is a `PermanentError`.
 */
export interface Source {
  /** Names the source within a scope. The record keeps one cursor per key, so two sources on one scope never share a position. */
  key: string;
  subscribe(cursor: string | null, emit: (batch: SourceBatch) => Promise<void>, signal: AbortSignal): Promise<void>;
}

export interface OfferedRoute {
  route: Route;
  /** The caller's criterion for this route, when configured. Judges supply their own otherwise. */
  criterion?: string;
}

/** What a judge decides from. Plain JSON, so a judge can run in another process or on a server. */
export interface JudgeInput {
  scope: string;
  window: Window;
  /** `wait` is absent when the window may not wait. */
  routes: OfferedRoute[];
  /** Who the agent is and what matters to its user, when configured. */
  guidance?: string;
  /** Yes/no questions to answer beside the route, each as a probability. */
  predicates: { name: string; question: string }[];
  /** Live facts from `context()`, such as whether the agent is mid-turn. */
  facts: JsonObject;
  /** This scope's last few verdicts, oldest first. */
  recent: Verdict[];
  now: number;
}

/**
 * Decides how the agent hears about a window. Throw to retry; throw a `PermanentError`
 * to dead-letter at once. A route that was not offered counts as a retryable failure.
 * Must honour `signal`.
 */
export type Judge = (input: JudgeInput, signal: AbortSignal) => Promise<Judgment>;

/**
 * Delivers decisions to the agent. Called at least once per decision and must be
 * idempotent on `decision.id`. Throw to retry; throw a `PermanentError` to dead-letter
 * at once. Deliveries in one scope arrive one at a time, oldest first.
 */
export interface Sink {
  deliver(decision: Decision, signal: AbortSignal): Promise<void>;
}

/** Time, injected. `fakeClock()` in ./testing.ts makes settling deterministic. */
export interface Clock {
  now(): number;
  /** Run `fn` at or after `at`. Returns a cancel. */
  at(at: number, fn: () => void): () => void;
}
