/**
 * The core: `step(config, record, input, now) -> { record, effects }`.
 *
 * Pure, synchronous, deterministic. Reads no clock (now is an argument), does no I/O,
 * and depends only on JSON and the config's kind function. Three invariants:
 *
 * 1. Every input is idempotent against the record: a replayed `events` dedups, a stale
 *    `judged`, `judgeFailed`, `delivered` or `deliveryFailed` is ignored, a `tick` re-derives.
 *    The shell may therefore reload and re-step on a compare-and-swap conflict.
 * 2. `events` yields traces only. Settling, judging and delivering happen on the other
 *    inputs, so a producer without a runner never strands an effect.
 * 3. Timers are state: `record.dueAt` is recomputed at the end of every step.
 */
import type {
  Batch,
  Buffer,
  DeadLetter,
  Decision,
  Delivery,
  DeliveryItem,
  Effect,
  Entry,
  Event,
  EventsInput,
  InFlightDelivery,
  Input,
  Judgment,
  Kinded,
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
  kind: (event: Event) => Kinded;
  policy: SettlePolicy;
}

export function initialRecord(scope: string): ScopeRecord {
  return {
    version: 0,
    scope,
    cursor: null,
    seen: [],
    buffer: emptyBuffer(),
    batch: null,
    outbox: [],
    recent: [],
    dueAt: null,
  };
}

function emptyBuffer(): Buffer {
  return { entries: [], dropped: 0, gap: false, forcedAt: null };
}

export function isAct(entry: Kinded): boolean {
  return entry.kind === 'closing' || entry.kind === 'continuous' || entry.kind === 'passive';
}

/** Consequences and context may leave a window; acts and a flush never do. */
function isSheddable(entry: Kinded): boolean {
  return entry.kind === 'consequence' || entry.kind === 'context';
}

/**
 * The outbox is two lanes told apart by `kind`, so a dead-letter handler that is down never
 * holds a decision back. Each lane delivers one at a time, oldest first.
 */
const LANES = ['decision', 'deadLetter'] as const;
type Lane = (typeof LANES)[number];

function laneHead(r: ScopeRecord, lane: Lane): number {
  return r.outbox.findIndex((d) => d.kind === lane);
}

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
    return b ? failJudge(ctx, r, b, input.message, input.permanent) : r;
  },
  delivered: onDelivered,
  deliveryFailed: (ctx, r, input) => {
    const i = liveDelivery(ctx, r, input.deliveryId, input.attempt, 'deliveryFailed');
    return i === null ? r : failDelivery(ctx, r, i, input.message, input.permanent);
  },
  flush: onFlush,
};

function dispatch<K extends keyof InputByKind>(ctx: Ctx, r: ScopeRecord, kind: K, input: InputByKind[K]): ScopeRecord {
  return handlers[kind](ctx, r, input);
}

/** Overload: ingest never emits anything but traces (invariant 2). */
export function step(config: StepConfig, record: ScopeRecord, input: EventsInput, now: number): Stepped<TraceEffect>;
export function step(config: StepConfig, record: ScopeRecord, input: Input, now: number): Stepped;
export function step(config: StepConfig, record: ScopeRecord, input: Input, now: number): Stepped {
  const ctx: Ctx = { config, now, out: [] };
  let r = dispatch(ctx, structuredClone(record), input.kind, input);
  if (input.kind !== 'events') r = sweep(ctx, r);
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
  for (const e of r.batch?.events ?? []) known.add(e.id);
  let duplicates = 0;
  for (const e of input.events) {
    if (known.has(e.id)) {
      duplicates += 1;
      continue;
    }
    known.add(e.id);
    r.buffer.entries.push({ ...e, ...ctx.config.kind(e), seenAt: ctx.now });
    r.seen.push(e.id);
  }
  r.seen = keepLast(r.seen, ctx.config.policy.seenCount);
  prune(ctx, r);
  shedBuffer(ctx, r);
  if (input.cursor !== undefined) r.cursor = input.cursor;
  if (input.truncated) {
    r.buffer.gap = true;
    trace(ctx, { kind: 'gap', scope: r.scope, cursor: r.cursor });
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
 * The earliest instant the open window settles, with its reason, or null while it holds
 * no act and no flush. Uses `seenAt` (the local clock at ingest), never `event.at`.
 */
export function deadline(policy: SettlePolicy, buffer: Buffer): { at: number; reason: SettleReason } | null {
  const entries = buffer.entries;
  if (entries.length === 0) return null;
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
  const b = r.batch;
  if (b) times.push(b.phase === 'judging' ? b.leaseUntil : b.phase === 'waiting' ? b.until : b.at);
  if (!b || b.phase !== 'judging') {
    const d = deadline(policy, r.buffer);
    if (d) times.push(d.at);
  }
  for (const lane of LANES) {
    const head = r.outbox[laneHead(r, lane)];
    if (head) times.push(head.phase === 'inFlight' ? head.leaseUntil : head.at);
  }
  return times.length === 0 ? null : Math.min(...times);
}

// ─── the sweep: everything time-driven, in a fixed order ─────────────────────

/** The restart contract: one tick resolves every expiry in this order. */
function sweep(ctx: Ctx, r: ScopeRecord): ScopeRecord {
  const b = r.batch;
  if (b?.phase === 'judging' && b.leaseUntil <= ctx.now) r = failJudge(ctx, r, b, 'judge lease expired', false);
  r = pumpOutbox(ctx, r);
  prune(ctx, r);
  r = settle(ctx, r);
  const held = r.batch;
  if (held?.phase === 'retrying' && held.at <= ctx.now) r = beginJudging(ctx, r, windowOf(held), held.attempt, held.offerWait);
  else if (held?.phase === 'waiting' && held.until <= ctx.now) r = beginJudging(ctx, r, windowOf(held), 0, false);
  return r;
}

/**
 * Move the buffer into the batch slot once its deadline has passed. Allowed when the slot
 * is empty or holds a waiting or retrying batch (the two merge, keeping waits and attempts).
 * Never while judging: the buffer stays open and settles the moment `judged` lands.
 */
function settle(ctx: Ctx, r: ScopeRecord): ScopeRecord {
  const b = r.batch;
  if (b?.phase === 'judging') return r;
  const buf = r.buffer;
  if (buf.entries.length === 0) {
    if (buf.forcedAt === null) return r;
    r.buffer.forcedAt = null;
    return b ? beginJudging(ctx, r, windowOf(b), b.phase === 'retrying' ? b.attempt : 0, false) : r;
  }
  const due = deadline(ctx.config.policy, buf);
  if (!due || due.at > ctx.now) return r;
  let events = buf.entries;
  let dropped = buf.dropped;
  let gap = buf.gap;
  let waits = 0;
  let attempt = 0;
  if (b) {
    events = [...b.events, ...events];
    dropped += b.dropped;
    gap = gap || b.gap;
    waits = b.waits;
    attempt = b.phase === 'retrying' ? b.attempt : 0;
  }
  ({ entries: events, dropped } = shed(ctx, r.scope, events, dropped));
  const window: Window = {
    id: hashIds(events.map((e) => e.id)),
    events,
    reason: due.reason,
    waits,
    dropped,
    gap,
    settledAt: ctx.now,
  };
  r.buffer = emptyBuffer();
  trace(ctx, { kind: 'settled', scope: r.scope, batchId: window.id, reason: due.reason, count: events.length, merged: b !== null });
  const offerWait = waits < ctx.config.policy.maxWaits && due.reason !== 'flush' && due.reason !== 'forced';
  return beginJudging(ctx, r, window, attempt, offerWait);
}

function windowOf(b: Batch): Window {
  return { id: b.id, events: b.events, reason: b.reason, waits: b.waits, dropped: b.dropped, gap: b.gap, settledAt: b.settledAt };
}

function beginJudging(ctx: Ctx, r: ScopeRecord, window: Window, priorAttempts: number, offerWait: boolean): ScopeRecord {
  const attempt = priorAttempts + 1;
  const leaseUntil = ctx.now + ctx.config.policy.judgeTimeoutMs * 2;
  r.batch = { ...window, phase: 'judging', attempt, leaseUntil, offerWait };
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

function liveBatch(ctx: Ctx, r: ScopeRecord, batchId: string, attempt: number, input: Input['kind']): Extract<Batch, { phase: 'judging' }> | null {
  const b = r.batch;
  if (b && b.phase === 'judging' && b.id === batchId && b.attempt === attempt) return b;
  trace(ctx, { kind: 'stale', scope: r.scope, input, why: `no live judge attempt ${attempt} for batch ${batchId}` });
  return null;
}

function onJudged(ctx: Ctx, r: ScopeRecord, input: Extract<Input, { kind: 'judged' }>): ScopeRecord {
  const b = liveBatch(ctx, r, input.batchId, input.attempt, 'judged');
  if (!b) return r;
  const verdict = validate(input.judgment, offeredRoutes(b.offerWait));
  if (verdict === null) return failJudge(ctx, r, b, `route not offered: ${String(input.judgment.route)}`, false);
  const window = windowOf(b);
  if (verdict.route === 'wait') {
    const until = ctx.now + ctx.config.policy.waitHorizonMs;
    const waits = b.waits + 1;
    r.batch = { ...window, waits, phase: 'waiting', until };
    trace(ctx, { kind: 'waiting', scope: r.scope, batchId: b.id, waits, until });
    return r;
  }
  r.recent = keepLast([...r.recent, { id: b.id, route: verdict.route, at: ctx.now }], ctx.config.policy.recentCount);
  trace(ctx, { kind: 'judged', scope: r.scope, batchId: b.id, route: verdict.route });
  if (verdict.route !== 'skip') {
    const decision: Decision = {
      id: b.id,
      scope: r.scope,
      route: verdict.route,
      probabilities: verdict.probabilities,
      predicates: verdict.predicates,
      window,
      at: ctx.now,
    };
    r.outbox.push({ id: b.id, kind: 'decision', decision, phase: 'retrying', attempt: 0, at: ctx.now });
  }
  r.batch = null;
  return r;
}

interface Validated {
  route: Route;
  probabilities: Record<Route, number>;
  predicates: Record<string, number>;
}

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

function failJudge(ctx: Ctx, r: ScopeRecord, b: Extract<Batch, { phase: 'judging' }>, message: string, permanent: boolean): ScopeRecord {
  const window = windowOf(b);
  const delay = permanent ? undefined : ctx.config.policy.retryBackoffMs[b.attempt - 1];
  if (delay !== undefined) {
    const at = ctx.now + delay;
    r.batch = { ...window, phase: 'retrying', attempt: b.attempt, at, offerWait: b.offerWait };
    trace(ctx, { kind: 'retrying', scope: r.scope, stage: 'judge', id: b.id, attempt: b.attempt, at, message });
    return r;
  }
  const letter: DeadLetter = {
    id: b.id,
    scope: r.scope,
    stage: 'judge',
    window,
    decision: null,
    message,
    permanent,
    attempts: b.attempt,
    at: ctx.now,
  };
  r.outbox.push({ id: b.id, kind: 'deadLetter', letter, phase: 'retrying', attempt: 0, at: ctx.now });
  trace(ctx, { kind: 'deadLettered', scope: r.scope, letter });
  r.batch = null;
  return r;
}

// ─── deliveries ──────────────────────────────────────────────────────────────

function liveDelivery(ctx: Ctx, r: ScopeRecord, deliveryId: string, attempt: number, input: Input['kind']): number | null {
  const i = r.outbox.findIndex((d) => d.id === deliveryId && d.phase === 'inFlight' && d.attempt === attempt);
  if (i !== -1) return i;
  trace(ctx, { kind: 'stale', scope: r.scope, input, why: `no live delivery attempt ${attempt} for ${deliveryId}` });
  return null;
}

function onDelivered(ctx: Ctx, r: ScopeRecord, input: Extract<Input, { kind: 'delivered' }>): ScopeRecord {
  const i = liveDelivery(ctx, r, input.deliveryId, input.attempt, 'delivered');
  if (i === null) return r;
  r.outbox.splice(i, 1);
  trace(ctx, { kind: 'delivered', scope: r.scope, deliveryId: input.deliveryId });
  return r;
}

function itemOf(d: Delivery): DeliveryItem {
  return d.kind === 'decision' ? { id: d.id, kind: 'decision', decision: d.decision } : { id: d.id, kind: 'deadLetter', letter: d.letter };
}

/** Backoff for a dead letter: the retry list, then doubling, capped. It never runs out. */
function deadLetterDelay(policy: SettlePolicy, attempt: number): number {
  const list = policy.retryBackoffMs;
  const cap = policy.deadLetterBackoffMaxMs;
  const base = list[Math.min(attempt, list.length) - 1] ?? cap;
  const beyond = Math.max(0, attempt - list.length);
  return Math.min(cap, base * 2 ** beyond);
}

/**
 * A failed attempt at `r.outbox[i]`: a decision retries through the backoff list and then
 * becomes a dead letter, leaving the decision lane; a dead letter retries with capped
 * backoff, whatever the error.
 */
function failDelivery(ctx: Ctx, r: ScopeRecord, i: number, message: string, permanent: boolean): ScopeRecord {
  const d = r.outbox[i];
  if (!d || d.phase !== 'inFlight') return r;
  const item = itemOf(d);
  if (d.kind === 'decision') {
    const delay = permanent ? undefined : ctx.config.policy.retryBackoffMs[d.attempt - 1];
    if (delay !== undefined) {
      const at = ctx.now + delay;
      r.outbox[i] = { ...item, phase: 'retrying', attempt: d.attempt, at };
      trace(ctx, { kind: 'retrying', scope: r.scope, stage: 'sink', id: d.id, attempt: d.attempt, at, message });
      return r;
    }
    const letter: DeadLetter = {
      id: d.id,
      scope: r.scope,
      stage: 'sink',
      window: d.decision.window,
      decision: d.decision,
      message,
      permanent,
      attempts: d.attempt,
      at: ctx.now,
    };
    r.outbox[i] = { id: d.id, kind: 'deadLetter', letter, phase: 'retrying', attempt: d.attempt, at: ctx.now };
    trace(ctx, { kind: 'deadLettered', scope: r.scope, letter });
    return r;
  }
  const at = ctx.now + deadLetterDelay(ctx.config.policy, d.attempt);
  r.outbox[i] = { ...item, phase: 'retrying', attempt: d.attempt, at };
  trace(ctx, { kind: 'retrying', scope: r.scope, stage: 'deadLetter', id: d.id, attempt: d.attempt, at, message });
  return r;
}

function pumpOutbox(ctx: Ctx, r: ScopeRecord): ScopeRecord {
  for (const lane of LANES) r = pumpLane(ctx, r, lane);
  return r;
}

/** One delivery per lane in flight at a time, oldest first. An expired lease is one failed attempt. */
function pumpLane(ctx: Ctx, r: ScopeRecord, lane: Lane): ScopeRecord {
  let i = laneHead(r, lane);
  const head = r.outbox[i];
  if (!head) return r;
  if (head.phase === 'inFlight') {
    if (head.leaseUntil > ctx.now) return r;
    r = failDelivery(ctx, r, i, 'delivery lease expired', false);
    // A decision that just spent its last attempt is now a dead letter, so this lane's head moved.
    i = laneHead(r, lane);
  }
  const next = r.outbox[i];
  if (!next || next.phase !== 'retrying' || next.at > ctx.now) return r;
  const attempt = next.attempt + 1;
  const inFlight: InFlightDelivery = { ...itemOf(next), phase: 'inFlight', attempt, leaseUntil: ctx.now + ctx.config.policy.deliveryTimeoutMs };
  r.outbox[i] = inFlight;
  ctx.out.push({ kind: 'deliver', scope: r.scope, delivery: inFlight });
  trace(ctx, { kind: 'delivering', scope: r.scope, deliveryId: next.id, attempt });
  return r;
}

// ─── flush ───────────────────────────────────────────────────────────────────

function onFlush(ctx: Ctx, r: ScopeRecord): ScopeRecord {
  const held = r.batch !== null && r.batch.phase !== 'judging';
  if (r.buffer.entries.length === 0 && !held) {
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

/** FNV-1a 64 over the UTF-8 bytes of the ids joined by newlines, as 16 hex characters. Portable and zero-dep. */
export function hashIds(ids: readonly string[]): string {
  let h = FNV_OFFSET;
  for (const byte of new TextEncoder().encode(ids.join('\n'))) {
    h ^= BigInt(byte);
    h = (h * FNV_PRIME) & MASK_64;
  }
  return h.toString(16).padStart(16, '0');
}
