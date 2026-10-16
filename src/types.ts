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
 * The settle role of an event.
 * - closing: ends an act; the window settles after a short grace so consequences land.
 * - flush: direct address; settles at once, with wait not offered.
 * - continuous: a gesture on a named stream; settles when every driven stream is quiet for a gap.
 * - passive: a dwell or look; settles after a quiet period.
 * - consequence: system output; attaches to the act that caused it, never settles alone.
 * - context: the agent's own output; rides along, never settles.
 */
export type Kind = (typeof KINDS)[number];

/** What `kind(event)` returns. Stored on the entry at ingest, so a changed kind function never reinterprets an open window. */
export type Kinded = { kind: Exclude<Kind, 'continuous'> } | { kind: 'continuous'; stream: string };

/** An event in a window: the event, its kind, and the local clock at ingest. */
export type Entry = Event & Kinded & { seenAt: number };

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
  /** Backoff before each retry of a judge call or a delivery; its length is the retry budget. */
  retryBackoffMs: number[];
  /** Aborts a judge call. Its lease is twice this, so a slow call is never duplicated while live. */
  judgeTimeoutMs: number;
  /** Lease on a delivery. Expiry counts as one failed attempt. */
  deliveryTimeoutMs: number;
  /** Verdicts kept as judge context. */
  recentCount: number;
  /** Event ids kept for dedup. */
  seenCount: number;
  /** Undelivered items per scope before `push` rejects. */
  maxOutbox: number;
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
  /** Set by `flush`; the window settles at the next step whatever its deadline says. */
  forcedAt: number | null;
}

/** A settled window: what the judge sees and what a decision carries. */
export interface Window {
  /** Hash of the event ids; it becomes the decision id. */
  id: string;
  /** Oldest first, kinds included. */
  events: Entry[];
  reason: SettleReason;
  waits: number;
  dropped: number;
  gap: boolean;
  settledAt: number;
}

export type BatchPhase =
  | { phase: 'judging'; attempt: number; leaseUntil: number; offerWait: boolean }
  | { phase: 'waiting'; until: number }
  | { phase: 'retrying'; attempt: number; at: number; offerWait: boolean };

/** The one settled window. One slot: that is single-flight. */
export type Batch = Window & BatchPhase;

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

/** A retry is its own phase, never a lease, so a backoff is never counted as a failed attempt. */
export type DeliveryPhase =
  | { phase: 'inFlight'; attempt: number; leaseUntil: number }
  | { phase: 'retrying'; attempt: number; at: number };

export type DeliveryItem = { id: string } & (
  | { kind: 'decision'; decision: Decision }
  | { kind: 'deadLetter'; letter: DeadLetter }
);

/** An outbox entry. Attempt numbers only grow for one id, so a stale ack never matches a later attempt. */
export type Delivery = DeliveryItem & DeliveryPhase;

/**
 * A scope's entire durable state. The store indexes `scope`, `version` and `dueAt` and
 * treats the rest as an opaque document. Every field has one writer: `step`.
 */
export interface ScopeRecord {
  /** Compare-and-swap token; 0 before the first put. */
  version: number;
  scope: string;
  /** The source's position after the last ingested event; null when the source has none. */
  cursor: string | null;
  /** Recent event ids, oldest first, bounded by `seenCount`. */
  seen: string[];
  buffer: Buffer;
  batch: Batch | null;
  /** Oldest first. Delivered one at a time. */
  outbox: Delivery[];
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
  /** Absent: leave the cursor as it is. Present (string or null): replace it. */
  cursor?: string | null;
  /** The source reported a gap before these events. */
  truncated?: boolean;
}

export type Input =
  | EventsInput
  | { kind: 'tick' }
  | { kind: 'judged'; batchId: string; attempt: number; judgment: Judgment }
  | { kind: 'judgeFailed'; batchId: string; attempt: number; message: string; permanent: boolean }
  | { kind: 'delivered'; deliveryId: string; attempt: number }
  | { kind: 'deliveryFailed'; deliveryId: string; attempt: number; message: string; permanent: boolean }
  /** Settle the open window now, reason `forced`. */
  | { kind: 'flush' };

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

export type InFlightDelivery = DeliveryItem & Extract<DeliveryPhase, { phase: 'inFlight' }>;

export interface DeliverEffect {
  kind: 'deliver';
  scope: string;
  delivery: InFlightDelivery;
}

export type Trace =
  | { kind: 'ingested'; scope: string; count: number; duplicates: number }
  | { kind: 'gap'; scope: string; cursor: string | null }
  | { kind: 'dropped'; scope: string; entries: Entry[]; why: 'overflow' | 'age' }
  | { kind: 'settled'; scope: string; batchId: string; reason: SettleReason; count: number; merged: boolean }
  | { kind: 'judging'; scope: string; batchId: string; attempt: number; offerWait: boolean }
  | { kind: 'judged'; scope: string; batchId: string; route: DecidedRoute }
  | { kind: 'waiting'; scope: string; batchId: string; waits: number; until: number }
  | { kind: 'retrying'; scope: string; stage: 'judge' | 'sink' | 'deadLetter'; id: string; attempt: number; at: number; message: string }
  | { kind: 'delivering'; scope: string; deliveryId: string; attempt: number }
  | { kind: 'delivered'; scope: string; deliveryId: string }
  | { kind: 'deadLettered'; scope: string; letter: DeadLetter }
  | { kind: 'stale'; scope: string; input: Input['kind']; why: string }
  | { kind: 'conflict'; scope: string; version: number }
  | { kind: 'abandoned'; scope: string; input: Input['kind'] }
  | { kind: 'failed'; scope: string; input: Input['kind']; message: string }
  | { kind: 'backpressure'; scope: string; outbox: number };

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
 * the subscription with that error.
 */
export interface Source {
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
