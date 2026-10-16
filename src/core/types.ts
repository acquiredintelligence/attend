/**
 * The domain, and the owned state the in-place reducer mutates. Everything but `ScopeState` and
 * its parts is plain JSON: a `ScopeRecord` is a scope's state as a snapshot.
 */

export type JsonPrimitive = string | number | boolean | null;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };
export type JsonObject = { [key: string]: JsonValue };

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

export type SettleReason = 'closing' | 'flush' | 'stream_quiet' | 'passive_quiet' | 'overflow' | 'age' | 'forced' | 'size';

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
  /**
   * Estimated rendered bytes at which the open window settles with reason `size`. Absent means
   * off, and the reducer then behaves exactly as before. The shell derives it from the judge's
   * budget unless the app sets it.
   */
  maxWindowBytes?: number;
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

/** The open window. Always present; empty entries mean no window is open. */
export interface Buffer {
  entries: Entry[];
  /** Non-act entries shed by overflow or age. Acts are never shed. */
  dropped: number;
  /** The source reported lost events while this window was open, or before it opened. */
  gap: boolean;
  /**
   * Set by `flush`; the window settles at the next tick whatever its deadline says. Set while a
   * judge call is in flight, it forbids that call's verdict to hold the window.
   */
  forcedAt: number | null;
  /** Set by `end()`. Absent when unset, so a record that never saw `end()` is unchanged. */
  endedAt?: number;
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
  /** How sure the judge was of the route, from 0 to 1, when it says; null when it does not. */
  confidence: number | null;
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

/** A scope's state as one JSON document: the snapshot format, and what `inspect` returns. Every field has one writer: the reducer. */
export interface ScopeRecord {
  /** The count of committed inputs: the compare-and-swap token. An input that changed nothing is never committed. */
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

/** What a judge returns. Validated against the offered routes inside `step`. */
export interface Judgment {
  route: Route;
  /** Per offered route. Missing routes count as 0; the reducer renormalises. */
  probabilities?: Partial<Record<Route, number>>;
  predicates?: Record<string, number>;
  /** From 0 to 1, when the judge reports one. Anything else is dropped. */
  confidence?: number;
}

export interface EventsInput {
  kind: 'events';
  events: (Event & { kind: Kind })[];
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

/**
 * The runner stopped before starting the call this effect asked for. The lease is released
 * and the call is due again at once with no failure counted, so the next runner takes it up
 * on its first tick. The attempt number goes unspent and the next call takes it: a call that
 * never happened produced no result that could collide with it. Stale against any other
 * attempt, so a replay changes nothing.
 */
export type ReleasedInput =
  | { kind: 'judgeReleased'; batchId: string; attempt: number }
  | { kind: 'deliveryReleased'; deliveryId: string; attempt: number };

/**
 * Punctuation from a producer that knows the unit is over. The open window settles at the next
 * tick with reason `closing`, with no grace and with `wait` still offered. Stale when there is
 * nothing to settle, like `flush`. Yields traces only.
 */
export interface EndInput {
  kind: 'end';
}

/**
 * The shell could not start the call because of its `judgeConcurrency` cap. Like `judgeReleased`,
 * except that the batch retries at `until` rather than now. The attempt goes unspent and no
 * failure is counted. Stale against any other attempt.
 */
export interface JudgeDeferredInput {
  kind: 'judgeDeferred';
  batchId: string;
  attempt: number;
  until: number;
}

export type Input =
  | EventsInput
  | FlushInput
  | EndInput
  | ReleasedInput
  | JudgeDeferredInput
  | { kind: 'tick' }
  | { kind: 'judged'; batchId: string; attempt: number; judgment: Judgment }
  | { kind: 'judgeFailed'; batchId: string; attempt: number; message: string; permanent: boolean }
  | { kind: 'delivered'; deliveryId: string; attempt: number }
  | { kind: 'deliveryFailed'; deliveryId: string; attempt: number; message: string; permanent: boolean };

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
  | { kind: 'released'; scope: string; stage: 'judge' | 'sink' | 'deadLetter'; id: string; attempt: number }
  | { kind: 'deadLettered'; scope: string; letter: DeadLetter }
  | { kind: 'stale'; scope: string; input: Input['kind']; why: string }
  | { kind: 'conflict'; scope: string; version: number }
  | { kind: 'abandoned'; scope: string; input: Input['kind'] }
  | { kind: 'failed'; scope: string; input: Input['kind']; message: string }
  | { kind: 'captureFailed'; scope: string; batchId: string; message: string }
  | { kind: 'deferred'; scope: string; batchId: string; attempt: number; until: number }
  | { kind: 'replayedAcrossUpgrade'; scope: string; from: number; to: number }
  | { kind: 'backpressure'; queued: number; lagMs: number }
  | { kind: 'routeThresholded'; scope: string; batchId: string; from: Route; to: Route; act: number };

export type TraceEffect = { kind: 'trace'; trace: Trace };

export type Effect = JudgeEffect | DeliverEffect | TraceEffect;

export interface Stepped<E extends Effect = Effect> {
  record: ScopeRecord;
  effects: E[];
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

/** A still image for the judge, as base64 with no `data:` prefix. Hosted judges have their own format and size limits. */
export interface JudgeImage {
  data: string;
  mimeType: 'image/png' | 'image/jpeg' | 'image/webp' | 'image/gif';
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
  /** What `capture()` returned for this call, when configured and it returned any. */
  images?: JudgeImage[];
  /** This scope's last few verdicts, oldest first. */
  recent: Verdict[];
  now: number;
}

/**
 * Decides how the agent hears about a window. Throw to retry; throw a `PermanentError`
 * to dead-letter at once. A route that was not offered counts as a retryable failure.
 * Must honour `signal`. It may declare a `budget`; a plain function still works.
 */
export type Judge = ((input: JudgeInput, signal: AbortSignal) => Promise<Judgment>) & { budget?: JudgeBudget };

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

/** What a judge declares about its input limit. `jevJudge` declares 64k tokens, or 262k with `longContext`. */
export interface JudgeBudget {
  contextTokens: number;
  /** Tokens the judge spends outside the window: system prompt, criteria, facts. Default: measured from the judge's own prompt. */
  reservedTokens?: number;
  /** Default 4. */
  bytesPerToken?: number;
}

/** FNV-1a 64 state on two unsigned 32-bit limbs. `foldId` mutates it in place. Its output is identical to `windowId`'s. */
export interface FnvState {
  hi: number;
  lo: number;
}

/**
 * The seen ring: the last `capacity` ingested ids, oldest at `head`. Capacity grows lazily up to
 * `seenCount`. Eviction happens only in `seenTrim`, after a whole batch.
 */
export interface IdRing {
  ids: string[];
  head: number;
  size: number;
  capacity: number;
}

/**
 * known.get(id) = (seen-ring slots holding id) + (1 if id is in the open window) + (1 if id is in
 * the held window). An id with count 0 is deleted, so an id is known iff it is present.
 */
export type KnownIds = Map<string, number>;

/**
 * Everything `deadline()` reads, kept O(1). Acts and flushes are never shed or pruned, so their
 * fields only move forward. NaN means none for the `*At` fields; `minFlushAt` and
 * `prefixMinSheddableAt` use Infinity for none. Equal to what a scan of the live entries derives.
 */
export interface TriggerSummary {
  minFlushAt: number;
  lastClosingAt: number;
  lastStreamAt: number;
  lastPassiveAt: number;
  firstActAt: number;
  /** Index of the first act relative to `start`, or -1. Adjusted on removal, unchanged by compaction. */
  firstActIndex: number;
  /** Smallest `seenAt` among sheddable entries before the first act: the O(1) prune test. */
  prefixMinSheddableAt: number;
  /** Live consequence and context entries; zero means nothing can be shed. */
  sheddable: number;
  lastSeenAt: number;
}

/** The open window, appended in place by its single writer. Live entries are `entries[start..]`. */
export interface OpenWindow {
  entries: Entry[];
  /** Index in `entries` and `sizes` of the first live slot. Slots before it are removed entries, at most half the live count. */
  start: number;
  /** Estimated rendered bytes per entry; `sizes[start..]` parallels the live entries. Recomputed by `fromRecord`; never stored. */
  sizes: number[];
  /** The sum of the live sizes. Compared with `SettlePolicy.maxWindowBytes`. */
  bytes: number;
  dropped: number;
  gap: boolean;
  forcedAt: number | null;
  endedAt: number | null;
  summary: TriggerSummary;
  /** FNV state over `len:scope` then each live entry's `len:id`, when `hashValid`. */
  hash: FnvState;
  /** False after a prune or shed inside the window; the settle then refolds the final list. */
  hashValid: boolean;
  /** False once an appended `seenAt` was smaller than the last one; prune then scans the whole prefix. */
  monotone: boolean;
}

/** The one settled window and its phase. `hash` continues over buffer ids when the window merges. */
export interface HeldBatch {
  window: Window;
  phase: BatchPhase;
  hash: FnvState | null;
}

/**
 * A scope's state, owned by exactly one writer and mutated in place by `apply`. Containers that
 * effects, snapshots or decision rows may hold (cursors, recent, Window, Delivery) are replaced,
 * never mutated.
 */
export interface ScopeState {
  scope: string;
  /** The count of committed inputs: the compare-and-swap token. */
  version: number;
  cursors: Readonly<Record<string, string | null>>;
  seen: IdRing;
  known: KnownIds;
  open: OpenWindow;
  batch: HeldBatch | null;
  /** Mutated in place: one in flight per lane, oldest first. `toRecord` copies them. */
  outbox: { decisions: Delivery[]; deadLetters: Delivery[] };
  recent: readonly Verdict[];
  dueAt: number | null;
}

/** What `apply` needs from the config. Nothing async and no app code: kinds arrive in the logged events. */
export interface ApplyConfig {
  policy: SettlePolicy;
  /** `lane`: a Sink is configured and the decisions lane works as today. `log`: the decision row is the delivery. */
  decisions: 'lane' | 'log';
  /** `lane`: a dead-letter handler is configured. `log`: the dead-letter row is final. */
  deadLetters: 'lane' | 'log';
  /** Build trace effects. False when nothing observes. Traces never change state. */
  traces: boolean;
}

/** A row `apply` asks the store to append in the same write as the input that produced it. */
export type DecisionAppend =
  | { kind: 'decision'; id: string; scope: string; at: number; route: DeliveredRoute; body: Decision }
  | { kind: 'deadLetter'; id: string; scope: string; at: number; route: DeliveredRoute | null; body: DeadLetter };

/** Filled by `apply`. `changed` is false when the input changed nothing; the shell then neither logs nor writes it. */
export interface ApplyOutput {
  effects: Effect[];
  appends: DecisionAppend[];
  changed: boolean;
}

/**
 * What the store keeps per committed input, and all that replay needs: the input, with its
 * events already carrying their kinds, and the clock it was applied at. Immutable once logged.
 */
export interface LoggedInput {
  input: Input;
  now: number;
}

/** What the pure `step` wrapper needs: the policy, plus the delivery modes (both default to `lane`). */
export interface StepConfig {
  policy: SettlePolicy;
  decisions?: 'lane' | 'log';
  deadLetters?: 'lane' | 'log';
}
