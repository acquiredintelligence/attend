/**
 * The core: `step(config, record, input, now) -> { record, effects }`.
 *
 * Pure, synchronous, deterministic. Reads no clock (now is an argument), does no I/O,
 * and depends only on JSON and the config's kind function. Three invariants:
 *
 * 1. Every input is idempotent against the record: a replayed `events` dedups, a stale
 *    `judged`, `judgeFailed`, `delivered` or `deliveryFailed` is ignored, a `tick` re-derives.
 *    The shell may therefore reload and re-step on a compare-and-swap conflict.
 * 2. `events`, `flush` and a release yield traces only. Settling, judging and delivering happen
 *    on the other inputs, so a producer without a runner never strands an effect, and a stopped
 *    runner that releases a call never asks for another.
 * 3. Timers are state: `record.dueAt` is recomputed at the end of every step.
 */
import type {
  Batch,
  BatchPhase,
  Buffer,
  DeadLetter,
  Decision,
  Effect,
  Entry,
  Event,
  EventsInput,
  FlushInput,
  InFlightDelivery,
  Input,
  Judgment,
  Kind,
  OutboxLane,
  ReleasedInput,
  Route,
  ScopeRecord,
  SettlePolicy,
  SettleReason,
  Stepped,
  Trace,
  TraceEffect,
  Window,
} from './types.js';
import { ROUTES } from './types.js';

/** What the reducer needs from the config: the kind function and the policy. Nothing async. */
export interface StepConfig {
  kind: (event: Event) => Kind;
  policy: SettlePolicy;
}

export function initialRecord(scope: string): ScopeRecord {
  return {
    version: 0,
    scope,
    cursors: {},
    seen: [],
    buffer: emptyBuffer(),
    batch: null,
    outbox: { decisions: [], deadLetters: [] },
    recent: [],
    dueAt: null,
  };
}

function emptyBuffer(): Buffer {
  return { entries: [], dropped: 0, gap: false, forcedAt: null };
}

export function isAct(entry: { kind: Kind }): boolean {
  return entry.kind === 'closing' || entry.kind === 'continuous' || entry.kind === 'passive';
}

/** Consequences and context may leave a window; acts and a flush never do. */
function isSheddable(entry: { kind: Kind }): boolean {
  return entry.kind === 'consequence' || entry.kind === 'context';
}

const LANES: readonly OutboxLane[] = ['decisions', 'deadLetters'];

interface Ctx {
  config: StepConfig;
  now: number;
  out: Effect[];
}

type InputByKind = { [K in Input['kind']]: Extract<Input, { kind: K }> };
type Handlers = { [K in keyof InputByKind]: (ctx: Ctx, r: ScopeRecord, input: InputByKind[K]) => ScopeRecord };

const handlers: Handlers = {
  events: ingest,
  tick: (_ctx, r) => r,
  judged: onJudged,
  judgeFailed: (ctx, r, input) => {
    const b = liveBatch(ctx, r, input.batchId, input.attempt, 'judgeFailed');
    return b ? failJudge(ctx, r, b.window, b.phase, input.message, input.permanent) : r;
  },
  delivered: onDelivered,
  deliveryFailed: (ctx, r, input) => {
    const lane = liveDelivery(ctx, r, input.deliveryId, input.attempt, 'deliveryFailed');
    return lane === null ? r : failDelivery(ctx, r, lane, input.message, input.permanent);
  },
  flush: onFlush,
  judgeReleased: (ctx, r, input) => {
    const b = liveBatch(ctx, r, input.batchId, input.attempt, 'judgeReleased');
    return b ? releaseJudge(ctx, r, b.window, b.phase) : r;
  },
  deliveryReleased: (ctx, r, input) => {
    const lane = liveDelivery(ctx, r, input.deliveryId, input.attempt, 'deliveryReleased');
    return lane === null ? r : releaseDelivery(ctx, r, lane);
  },
};

function dispatch<K extends keyof InputByKind>(ctx: Ctx, r: ScopeRecord, kind: K, input: InputByKind[K]): ScopeRecord {
  return handlers[kind](ctx, r, input);
}

/** The inputs that yield traces only (invariant 2): no sweep follows them. */
const UNSWEPT: ReadonlySet<Input['kind']> = new Set<Input['kind']>(['events', 'flush', 'judgeReleased', 'deliveryReleased']);

/** Overload: ingest, flush and a release never emit anything but traces (invariant 2). */
export function step(config: StepConfig, record: ScopeRecord, input: EventsInput | FlushInput | ReleasedInput, now: number): Stepped<TraceEffect>;
export function step(config: StepConfig, record: ScopeRecord, input: Input, now: number): Stepped;
export function step(config: StepConfig, record: ScopeRecord, input: Input, now: number): Stepped {
  const ctx: Ctx = { config, now, out: [] };
  let r = dispatch(ctx, structuredClone(record), input.kind, input);
  if (!UNSWEPT.has(input.kind)) r = sweep(ctx, r);
  r.dueAt = nextDue(config.policy, r);
  return { record: r, effects: ctx.out };
}

function trace(ctx: Ctx, t: Trace): void {
  ctx.out.push({ kind: 'trace', trace: t });
}

function keepLast<T>(list: T[], n: number): T[] {
  return n <= 0 ? [] : list.slice(-n);
}

// ─── ingest ──────────────────────────────────────────────────────────────────

function ingest(ctx: Ctx, r: ScopeRecord, input: EventsInput): ScopeRecord {
  const known = new Set<string>(r.seen);
  for (const e of r.buffer.entries) known.add(e.id);
  for (const e of r.batch?.window.events ?? []) known.add(e.id);
  let duplicates = 0;
  for (const e of input.events) {
    if (known.has(e.id)) {
      duplicates += 1;
      continue;
    }
    known.add(e.id);
    r.buffer.entries.push({ ...e, kind: ctx.config.kind(e), seenAt: ctx.now });
    r.seen.push(e.id);
  }
  r.seen = keepLast(r.seen, ctx.config.policy.seenCount);
  prune(ctx, r);
  shedBuffer(ctx, r);
  const source = input.source ?? null;
  if (source !== null && input.cursor !== undefined) r.cursors[source] = input.cursor;
  if (input.truncated) {
    r.buffer.gap = true;
    trace(ctx, { kind: 'gap', scope: r.scope, source, cursor: source === null ? null : (r.cursors[source] ?? null) });
  }
  trace(ctx, { kind: 'ingested', scope: r.scope, count: input.events.length - duplicates, duplicates });
  return r;
}

/** Act-less entries older than `maxWindowMs` that precede the first act never join a window: they are pruned. */
function prune(ctx: Ctx, r: ScopeRecord): void {
  const entries = r.buffer.entries;
  const firstAct = entries.findIndex(isAct);
  const head = firstAct === -1 ? entries.length : firstAct;
  const stale: Entry[] = [];
  const kept: Entry[] = [];
  for (let i = 0; i < entries.length; i += 1) {
    const e = entries[i];
    if (e === undefined) continue;
    if (i < head && isSheddable(e) && e.seenAt + ctx.config.policy.maxWindowMs <= ctx.now) stale.push(e);
    else kept.push(e);
  }
  if (stale.length === 0) return;
  r.buffer.entries = kept;
  r.buffer.dropped += stale.length;
  trace(ctx, { kind: 'dropped', scope: r.scope, entries: stale, why: 'age' });
}

function shedBuffer(ctx: Ctx, r: ScopeRecord): void {
  const { entries, dropped } = shed(ctx, r.scope, r.buffer.entries, r.buffer.dropped);
  r.buffer.entries = entries;
  r.buffer.dropped = dropped;
}

/** Drop the oldest sheddable entries until the list fits `maxEvents`. A list of acts stays as it is. */
function shed(ctx: Ctx, scope: string, entries: Entry[], dropped: number): { entries: Entry[]; dropped: number } {
  const max = ctx.config.policy.maxEvents;
  if (entries.length <= max) return { entries, dropped };
  const gone: Entry[] = [];
  const kept = [...entries];
  while (kept.length > max) {
    const i = kept.findIndex(isSheddable);
    if (i === -1) break;
    const [e] = kept.splice(i, 1);
    if (e) gone.push(e);
  }
  if (gone.length > 0) trace(ctx, { kind: 'dropped', scope, entries: gone, why: 'overflow' });
  return { entries: kept, dropped: dropped + gone.length };
}

// ─── deadlines ───────────────────────────────────────────────────────────────

/**
 * The earliest instant the open window settles, with the reason of that deadline, or null
 * while it holds no act and no flush. Uses `seenAt` (the local clock at ingest), never
 * `event.at`. The judged window's reason can be stronger: see `settleReason`.
 */
export function deadline(policy: SettlePolicy, buffer: Buffer): { at: number; reason: SettleReason } | null {
  const entries = buffer.entries;
  const candidates: { at: number; reason: SettleReason }[] = [];
  if (buffer.forcedAt !== null) candidates.push({ at: buffer.forcedAt, reason: 'forced' });
  const last = entries[entries.length - 1];
  if (entries.length > policy.maxEvents && last) candidates.push({ at: last.seenAt, reason: 'overflow' });
  let closing: Entry | undefined;
  let stream: Entry | undefined;
  let passive: Entry | undefined;
  let firstAct: Entry | undefined;
  for (const e of entries) {
    if (e.kind === 'flush') candidates.push({ at: e.seenAt, reason: 'flush' });
    if (e.kind === 'closing') closing = e;
    if (e.kind === 'continuous') stream = e;
    if (e.kind === 'passive') passive = e;
    if (firstAct === undefined && isAct(e)) firstAct = e;
  }
  if (closing) candidates.push({ at: closing.seenAt + policy.closingGraceMs, reason: 'closing' });
  if (stream) candidates.push({ at: stream.seenAt + policy.streamGapMs, reason: 'stream_quiet' });
  if (passive) candidates.push({ at: passive.seenAt + policy.passiveQuietMs, reason: 'passive_quiet' });
  if (firstAct) candidates.push({ at: firstAct.seenAt + policy.maxWindowMs, reason: 'age' });
  let best: { at: number; reason: SettleReason } | null = null;
  for (const c of candidates) if (best === null || c.at < best.at) best = c;
  return best;
}

/**
 * The next instant anything in the record becomes actionable. Never earlier than a live
 * lease: a judging batch holds the buffer back, so a running judge never causes a busy loop.
 */
function nextDue(policy: SettlePolicy, r: ScopeRecord): number | null {
  const times: number[] = [];
  const b = r.batch?.phase;
  if (b) times.push(b.phase === 'judging' ? b.leaseUntil : b.phase === 'waiting' ? b.until : b.at);
  if (b?.phase !== 'judging') {
    const d = deadline(policy, r.buffer);
    if (d) times.push(d.at);
  }
  for (const lane of LANES) {
    const head = r.outbox[lane][0]?.phase;
    if (head) times.push(head.phase === 'inFlight' ? head.leaseUntil : head.at);
  }
  return times.length === 0 ? null : Math.min(...times);
}

// ─── the sweep: everything time-driven, in a fixed order ─────────────────────

/** The restart contract: one tick resolves every expiry in this order. */
function sweep(ctx: Ctx, r: ScopeRecord): ScopeRecord {
  const b = r.batch;
  if (b && b.phase.phase === 'judging' && b.phase.leaseUntil <= ctx.now) r = failJudge(ctx, r, b.window, b.phase, 'judge lease expired', false);
  r = pumpOutbox(ctx, r);
  prune(ctx, r);
  r = settle(ctx, r);
  const held = r.batch;
  if (!held) return r;
  if (held.phase.phase === 'retrying' && held.phase.at <= ctx.now) return beginJudging(ctx, r, held.window, held.phase, held.phase.offerWait);
  if (held.phase.phase === 'waiting' && held.phase.until <= ctx.now) return horizon(ctx, r, held.window, held.phase);
  return r;
}

/**
 * Move the buffer into the batch slot once its deadline has passed. Allowed when the slot
 * is empty or holds a waiting or retrying batch (the two merge, keeping waits and attempts).
 * Never while judging: the buffer stays open and settles the moment `judged` lands.
 */
function settle(ctx: Ctx, r: ScopeRecord): ScopeRecord {
  if (r.batch?.phase.phase === 'judging') return r;
  const due = deadline(ctx.config.policy, r.buffer);
  if (!due || due.at > ctx.now) return r;
  return settleBuffer(ctx, r, due.reason, true);
}

/**
 * Why a window is in front of the judge: a direct address beats a forced settle, which beats
 * whichever deadline fired. Both outlive a merge, so a window that holds a flush or was forced
 * stays so whatever joins it later, and neither ever offers `wait`.
 */
function settleReason(events: Entry[], forced: boolean, fired: SettleReason): SettleReason {
  if (events.some((e) => e.kind === 'flush')) return 'flush';
  return forced ? 'forced' : fired;
}

function isDirect(reason: SettleReason): boolean {
  return reason === 'flush' || reason === 'forced';
}

/** The horizon judges the held window with everything that arrived meanwhile, consequences included, and wait withheld. */
function horizon(ctx: Ctx, r: ScopeRecord, window: Window, phase: Extract<BatchPhase, { phase: 'waiting' }>): ScopeRecord {
  return r.buffer.entries.length === 0 ? beginJudging(ctx, r, window, phase, false) : settleBuffer(ctx, r, window.reason, false);
}

/** Judge calls so far, and failures since the last wait, carried from a held batch into its next call. */
interface Counters {
  attempt: number;
  failures?: number;
}

/** The buffer, merged with a waiting or retrying batch, becomes the one judged window. A forced empty buffer judges the held batch alone. */
function settleBuffer(ctx: Ctx, r: ScopeRecord, fired: SettleReason, mayWait: boolean): ScopeRecord {
  const held = r.batch;
  const buf = r.buffer;
  let forced = buf.forcedAt !== null;
  if (buf.entries.length === 0) {
    r.buffer.forcedAt = null;
    if (!held) return r;
    const window = { ...held.window, reason: settleReason(held.window.events, forced, held.window.reason) };
    return beginJudging(ctx, r, window, held.phase, false);
  }
  let events = buf.entries;
  let dropped = buf.dropped;
  let gap = buf.gap;
  let waits = 0;
  let prior: Counters = { attempt: 0 };
  if (held) {
    events = [...held.window.events, ...events];
    dropped += held.window.dropped;
    gap = gap || held.window.gap;
    waits = held.window.waits;
    prior = held.phase;
    forced = forced || held.window.reason === 'forced';
  }
  ({ entries: events, dropped } = shed(ctx, r.scope, events, dropped));
  const reason = settleReason(events, forced, fired);
  const window: Window = {
    id: windowId(r.scope, events.map((e) => e.id)),
    events,
    reason,
    waits,
    dropped,
    gap,
    settledAt: ctx.now,
  };
  r.buffer = emptyBuffer();
  trace(ctx, { kind: 'settled', scope: r.scope, batchId: window.id, reason, count: events.length, merged: held !== null });
  return beginJudging(ctx, r, window, prior, mayWait && !isDirect(reason) && waits < ctx.config.policy.maxWaits);
}

function beginJudging(ctx: Ctx, r: ScopeRecord, window: Window, prior: Counters, offerWait: boolean): ScopeRecord {
  const attempt = prior.attempt + 1;
  const leaseUntil = ctx.now + ctx.config.policy.judgeTimeoutMs * 2;
  r.batch = { window, phase: { phase: 'judging', attempt, failures: prior.failures ?? 0, leaseUntil, offerWait } };
  ctx.out.push({
    kind: 'judge',
    scope: r.scope,
    batchId: window.id,
    attempt,
    window,
    offered: offeredRoutes(offerWait),
    recent: r.recent,
    leaseUntil,
  });
  trace(ctx, { kind: 'judging', scope: r.scope, batchId: window.id, attempt, offerWait });
  return r;
}

export function offeredRoutes(offerWait: boolean): Route[] {
  return ROUTES.filter((route) => offerWait || route !== 'wait');
}

// ─── judge results ───────────────────────────────────────────────────────────

type Judging = Batch & { phase: Extract<BatchPhase, { phase: 'judging' }> };

function liveBatch(ctx: Ctx, r: ScopeRecord, batchId: string, attempt: number, input: Input['kind']): Judging | null {
  const b = r.batch;
  if (b && b.phase.phase === 'judging' && b.window.id === batchId && b.phase.attempt === attempt) return { window: b.window, phase: b.phase };
  trace(ctx, { kind: 'stale', scope: r.scope, input, why: `no live judge attempt ${attempt} for batch ${batchId}` });
  return null;
}

function onJudged(ctx: Ctx, r: ScopeRecord, input: Extract<Input, { kind: 'judged' }>): ScopeRecord {
  const b = liveBatch(ctx, r, input.batchId, input.attempt, 'judged');
  if (!b) return r;
  const verdict = validate(input.judgment, offeredRoutes(b.phase.offerWait));
  if (verdict === null) return failJudge(ctx, r, b.window, b.phase, `route not offered: ${String(input.judgment.route)}`, false);
  const { window } = b;
  if (verdict.route === 'wait') {
    const until = ctx.now + ctx.config.policy.waitHorizonMs;
    const waits = window.waits + 1;
    r.batch = { window: { ...window, waits }, phase: { phase: 'waiting', attempt: b.phase.attempt, until } };
    trace(ctx, { kind: 'waiting', scope: r.scope, batchId: window.id, waits, until });
    return r;
  }
  r.recent = keepLast([...r.recent, { id: window.id, route: verdict.route, at: ctx.now }], ctx.config.policy.recentCount);
  trace(ctx, { kind: 'judged', scope: r.scope, batchId: window.id, route: verdict.route });
  if (verdict.route !== 'skip') {
    const decision: Decision = {
      id: window.id,
      scope: r.scope,
      route: verdict.route,
      probabilities: verdict.probabilities,
      predicates: verdict.predicates,
      window,
      at: ctx.now,
    };
    r.outbox.decisions.push({ item: { id: window.id, kind: 'decision', decision }, phase: { phase: 'retrying', attempt: 0, failures: 0, at: ctx.now } });
  }
  r.batch = null;
  return r;
}

/** A judgment after validation: the decision's vectors, with the route still allowed to be `wait`. */
type Validated = Pick<Decision, 'probabilities' | 'predicates'> & { route: Route };

function unit(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 && value <= 1 ? value : null;
}

/**
 * Validate the judge's answer at the boundary. The route must be offered, or the answer is
 * null. Probabilities outside [0, 1] or for routes not offered count as 0 and the vector is
 * renormalised; when it sums to 0 the chosen route gets 1. Predicates outside [0, 1] are dropped.
 */
export function validate(judgment: Judgment, offered: Route[]): Validated | null {
  const route = offered.find((candidate) => candidate === judgment.route);
  if (route === undefined) return null;
  const raw: Record<Route, number> = { interrupt: 0, queue: 0, note: 0, ask: 0, skip: 0, wait: 0 };
  let sum = 0;
  for (const candidate of offered) {
    const p = unit(judgment.probabilities?.[candidate]);
    if (p !== null) {
      raw[candidate] = p;
      sum += p;
    }
  }
  const probabilities = { ...raw };
  if (sum > 0) for (const candidate of ROUTES) probabilities[candidate] = raw[candidate] / sum;
  else probabilities[route] = 1;
  const predicates: Record<string, number> = {};
  for (const [name, value] of Object.entries(judgment.predicates ?? {})) {
    const p = unit(value);
    if (p !== null) predicates[name] = p;
  }
  return { route, probabilities, predicates };
}

function failJudge(ctx: Ctx, r: ScopeRecord, window: Window, phase: Judging['phase'], message: string, permanent: boolean): ScopeRecord {
  const failures = phase.failures + 1;
  const delay = permanent ? undefined : ctx.config.policy.retryBackoffMs[failures - 1];
  if (delay !== undefined) {
    const at = ctx.now + delay;
    r.batch = { window, phase: { phase: 'retrying', attempt: phase.attempt, failures, at, offerWait: phase.offerWait } };
    trace(ctx, { kind: 'retrying', scope: r.scope, stage: 'judge', id: window.id, attempt: phase.attempt, at, message });
    return r;
  }
  const letter: DeadLetter = {
    id: window.id,
    scope: r.scope,
    stage: 'judge',
    window,
    decision: null,
    message,
    permanent,
    attempts: phase.attempt,
    at: ctx.now,
  };
  r.outbox.deadLetters.push({ item: { id: window.id, kind: 'deadLetter', letter }, phase: { phase: 'retrying', attempt: 0, failures: 0, at: ctx.now } });
  trace(ctx, { kind: 'deadLettered', scope: r.scope, letter });
  r.batch = null;
  return r;
}

/** The runner never started this call: the batch goes back to retrying at once, its attempt unspent and its failures as they were. */
function releaseJudge(ctx: Ctx, r: ScopeRecord, window: Window, phase: Judging['phase']): ScopeRecord {
  r.batch = { window, phase: { phase: 'retrying', attempt: phase.attempt - 1, failures: phase.failures, at: ctx.now, offerWait: phase.offerWait } };
  trace(ctx, { kind: 'released', scope: r.scope, stage: 'judge', id: window.id, attempt: phase.attempt });
  return r;
}

// ─── deliveries ──────────────────────────────────────────────────────────────

/** Only a lane's head is ever in flight, so a result names the lane whose head it belongs to. */
function liveDelivery(ctx: Ctx, r: ScopeRecord, deliveryId: string, attempt: number, input: Input['kind']): OutboxLane | null {
  for (const lane of LANES) {
    const head = r.outbox[lane][0];
    if (head && head.item.id === deliveryId && head.phase.phase === 'inFlight' && head.phase.attempt === attempt) return lane;
  }
  trace(ctx, { kind: 'stale', scope: r.scope, input, why: `no live delivery attempt ${attempt} for ${deliveryId}` });
  return null;
}

function onDelivered(ctx: Ctx, r: ScopeRecord, input: Extract<Input, { kind: 'delivered' }>): ScopeRecord {
  const lane = liveDelivery(ctx, r, input.deliveryId, input.attempt, 'delivered');
  if (lane === null) return r;
  r.outbox[lane].shift();
  trace(ctx, { kind: 'delivered', scope: r.scope, deliveryId: input.deliveryId });
  return r;
}

/** Backoff for a dead letter: the retry list, then doubling, capped. It never runs out. */
function deadLetterDelay(policy: SettlePolicy, failures: number): number {
  const list = policy.retryBackoffMs;
  const cap = policy.deadLetterBackoffMaxMs;
  const base = list[Math.min(failures, list.length) - 1] ?? cap;
  const beyond = Math.max(0, failures - list.length);
  return Math.min(cap, base * 2 ** beyond);
}

/**
 * A failed attempt at the lane's in-flight head: a decision retries through the backoff list
 * and then leaves its lane as a dead letter; a dead letter retries with capped backoff,
 * whatever the error.
 */
function failDelivery(ctx: Ctx, r: ScopeRecord, lane: OutboxLane, message: string, permanent: boolean): ScopeRecord {
  const d = r.outbox[lane][0];
  if (!d || d.phase.phase !== 'inFlight') return r;
  const { item } = d;
  const { attempt } = d.phase;
  const failures = d.phase.failures + 1;
  if (item.kind === 'deadLetter') {
    const at = ctx.now + deadLetterDelay(ctx.config.policy, failures);
    r.outbox[lane][0] = { item, phase: { phase: 'retrying', attempt, failures, at } };
    trace(ctx, { kind: 'retrying', scope: r.scope, stage: 'deadLetter', id: item.id, attempt, at, message });
    return r;
  }
  const delay = permanent ? undefined : ctx.config.policy.retryBackoffMs[failures - 1];
  if (delay !== undefined) {
    const at = ctx.now + delay;
    r.outbox[lane][0] = { item, phase: { phase: 'retrying', attempt, failures, at } };
    trace(ctx, { kind: 'retrying', scope: r.scope, stage: 'sink', id: item.id, attempt, at, message });
    return r;
  }
  const letter: DeadLetter = {
    id: item.id,
    scope: r.scope,
    stage: 'sink',
    window: item.decision.window,
    decision: item.decision,
    message,
    permanent,
    attempts: attempt,
    at: ctx.now,
  };
  r.outbox[lane].shift();
  r.outbox.deadLetters.push({ item: { id: item.id, kind: 'deadLetter', letter }, phase: { phase: 'retrying', attempt, failures: 0, at: ctx.now } });
  trace(ctx, { kind: 'deadLettered', scope: r.scope, letter });
  return r;
}

/** The runner never started the lane's in-flight head: it goes back to retrying at once, its attempt unspent and its failures as they were. */
function releaseDelivery(ctx: Ctx, r: ScopeRecord, lane: OutboxLane): ScopeRecord {
  const d = r.outbox[lane][0];
  if (!d || d.phase.phase !== 'inFlight') return r;
  const { item } = d;
  const { attempt, failures } = d.phase;
  r.outbox[lane][0] = { item, phase: { phase: 'retrying', attempt: attempt - 1, failures, at: ctx.now } };
  trace(ctx, { kind: 'released', scope: r.scope, stage: item.kind === 'decision' ? 'sink' : 'deadLetter', id: item.id, attempt });
  return r;
}

function pumpOutbox(ctx: Ctx, r: ScopeRecord): ScopeRecord {
  for (const lane of LANES) r = pumpLane(ctx, r, lane);
  return r;
}

/** One delivery per lane in flight at a time, oldest first. An expired lease is one failed attempt. */
function pumpLane(ctx: Ctx, r: ScopeRecord, lane: OutboxLane): ScopeRecord {
  const head = r.outbox[lane][0];
  if (!head) return r;
  if (head.phase.phase === 'inFlight') {
    if (head.phase.leaseUntil > ctx.now) return r;
    r = failDelivery(ctx, r, lane, 'delivery lease expired', false);
  }
  const next = r.outbox[lane][0];
  if (!next || next.phase.phase !== 'retrying' || next.phase.at > ctx.now) return r;
  const attempt = next.phase.attempt + 1;
  const inFlight: InFlightDelivery = { item: next.item, phase: { phase: 'inFlight', attempt, failures: next.phase.failures, leaseUntil: ctx.now + ctx.config.policy.deliveryTimeoutMs } };
  r.outbox[lane][0] = inFlight;
  ctx.out.push({ kind: 'deliver', scope: r.scope, delivery: inFlight });
  trace(ctx, { kind: 'delivering', scope: r.scope, deliveryId: next.item.id, attempt });
  return r;
}

// ─── flush ───────────────────────────────────────────────────────────────────

/**
 * Marks the buffer forced; the runner's next tick settles it, so a flush without a runner
 * strands no effect. A window already in front of the judge is forced too: the force outlives
 * its verdict, so a `wait` is judged again at once with `wait` withheld.
 */
function onFlush(ctx: Ctx, r: ScopeRecord): ScopeRecord {
  if (r.buffer.entries.length === 0 && r.batch === null) {
    trace(ctx, { kind: 'stale', scope: r.scope, input: 'flush', why: 'nothing to settle' });
    return r;
  }
  r.buffer.forcedAt = ctx.now;
  return r;
}

// ─── ids ─────────────────────────────────────────────────────────────────────

const FNV_OFFSET = 0xcbf29ce484222325n;
const FNV_PRIME = 0x100000001b3n;
const MASK_64 = 0xffffffffffffffffn;

/**
 * FNV-1a 64 over the scope and the event ids, each as its UTF-8 byte length, a colon and its
 * bytes, as 16 hex characters. The length prefix keeps `['a\nb']` apart from `['a', 'b']`, and
 * the scope keeps the same ids in two scopes apart. Portable and zero-dep.
 */
export function windowId(scope: string, ids: readonly string[]): string {
  const encoder = new TextEncoder();
  let h = FNV_OFFSET;
  const feed = (bytes: Uint8Array) => {
    for (const byte of bytes) {
      h ^= BigInt(byte);
      h = (h * FNV_PRIME) & MASK_64;
    }
  };
  for (const part of [scope, ...ids]) {
    const bytes = encoder.encode(part);
    feed(encoder.encode(`${bytes.length}:`));
    feed(bytes);
  }
  return h.toString(16).padStart(16, '0');
}
