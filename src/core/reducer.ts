import type {
  ApplyConfig, ApplyOutput, BatchPhase, DeadLetter, Decision, Entry, EventsInput, FnvState,
  HeldBatch, InFlightDelivery, Input, JudgeEffect, Judgment, LoggedInput, OutboxLane, Route, ScopeState,
  SettlePolicy, SettleReason, Window,
} from './types.js';
import { ROUTES } from './types.js';
import { fnvHex, fnvStart, foldId } from './ids.js';
import { seenAdmit, seenRelease, seenTrim } from './seen.js';
import { estimateRenderedBytes, openWindow, shedEntries, windowAppend, windowCompact, windowDeadline, windowPrune, windowShed, windowSize } from './window.js';

/** Bumped whenever a release changes a transition. Stored with every snapshot. */
export const REDUCER_VERSION = 1;

/** An empty output. One output may collect several inputs: `apply` only ever sets `changed`. */
export function newOutput(): ApplyOutput {
  return { effects: [], appends: [], changed: false };
}

interface Ctx {
  config: ApplyConfig;
  now: number;
  out: ApplyOutput;
}

const LANES: readonly OutboxLane[] = ['decisions', 'deadLetters'];
type Judging = HeldBatch & { phase: Extract<BatchPhase, { phase: 'judging' }> };
type Counters = { attempt: number; failures?: number };

/**
 * Applies one logged input to `state` in place: today's transitions, plus `end`, `judgeDeferred`
 * and the `size` trigger. Deterministic in (state, logged, config). Pushes effects and decision
 * rows onto `out`, and sets `out.changed` if anything changed. Never reads a clock and never calls
 * app code. `events`, `flush`, `end` and the releases still yield traces only.
 */
export function apply(config: ApplyConfig, state: ScopeState, logged: LoggedInput, out: ApplyOutput): void {
  const ctx: Ctx = { config, now: logged.now, out };
  const { input } = logged;
  let swept = false;
  switch (input.kind) {
    case 'events': ingest(ctx, state, input); break;
    case 'flush':
      if (windowSize(state.open) === 0 && state.batch === null) {
        if (config.traces) out.effects.push({ kind: 'trace', trace: { kind: 'stale', scope: state.scope, input: 'flush', why: 'nothing to settle' } });
      } else if (!Object.is(state.open.forcedAt, logged.now)) {
        state.open.forcedAt = logged.now;
        out.changed = true;
      }
      break;
    case 'end':
      if (windowSize(state.open) === 0) {
        if (config.traces) out.effects.push({ kind: 'trace', trace: { kind: 'stale', scope: state.scope, input: 'end', why: 'nothing to settle' } });
      } else if (!Object.is(state.open.endedAt, logged.now)) {
        state.open.endedAt = logged.now;
        out.changed = true;
      }
      break;
    case 'judgeReleased':
    case 'judgeDeferred': {
      const b = liveBatch(ctx, state, input.batchId, input.attempt, input.kind);
      if (b) {
        state.batch = { window: b.window, phase: { phase: 'retrying', attempt: b.phase.attempt - 1, failures: b.phase.failures,
          at: input.kind === 'judgeDeferred' ? input.until : logged.now, offerWait: b.phase.offerWait }, hash: b.hash };
        out.changed = true;
        if (config.traces) out.effects.push({ kind: 'trace', trace: input.kind === 'judgeDeferred'
          ? { kind: 'deferred', scope: state.scope, batchId: input.batchId, attempt: input.attempt, until: input.until }
          : { kind: 'released', scope: state.scope, stage: 'judge', id: b.window.id, attempt: b.phase.attempt } });
      }
      break;
    }
    case 'judged': onJudged(ctx, state, input); swept = true; break;
    case 'judgeFailed': {
      const b = liveBatch(ctx, state, input.batchId, input.attempt, input.kind);
      if (b) failJudge(ctx, state, b, input.message, input.permanent);
      swept = true;
      break;
    }
    case 'delivered':
    case 'deliveryFailed':
    case 'deliveryReleased': {
      const lane = liveDelivery(ctx, state, input.deliveryId, input.attempt, input.kind);
      if (lane !== null) {
        if (input.kind === 'deliveryFailed') failDelivery(ctx, state, lane, input.message, input.permanent);
        else if (input.kind === 'delivered') {
          state.outbox[lane].shift();
          out.changed = true;
          if (config.traces) out.effects.push({ kind: 'trace', trace: { kind: 'delivered', scope: state.scope, deliveryId: input.deliveryId } });
        } else {
          const d = state.outbox[lane][0];
          if (d && d.phase.phase === 'inFlight') {
            const { attempt, failures } = d.phase;
            state.outbox[lane][0] = { item: d.item, phase: { phase: 'retrying', attempt: attempt - 1, failures, at: logged.now } };
            out.changed = true;
            if (config.traces) out.effects.push({ kind: 'trace', trace: { kind: 'released', scope: state.scope,
              stage: d.item.kind === 'decision' ? 'sink' : 'deadLetter', id: d.item.id, attempt } });
          }
        }
      }
      swept = input.kind !== 'deliveryReleased';
      break;
    }
    case 'tick': swept = true; break;
    default: input satisfies never;
  }
  if (swept) sweep(ctx, state);
  const due = nextDue(config.policy, state);
  if (!Object.is(state.dueAt, due)) {
    state.dueAt = due;
    out.changed = true;
  }
}

function removed(ctx: Ctx, state: ScopeState, entries: Entry[] | null, why: 'age' | 'overflow'): void {
  if (entries === null) return;
  for (const entry of entries) seenRelease(state, entry.id);
  ctx.out.changed = true;
  if (ctx.config.traces) ctx.out.effects.push({ kind: 'trace', trace: { kind: 'dropped', scope: state.scope, entries, why } });
}

function prune(ctx: Ctx, state: ScopeState): void {
  removed(ctx, state, windowPrune(state.open, ctx.config.policy.maxWindowMs, ctx.now), 'age');
  if (windowSize(state.open) === 0) state.open.endedAt = null;
}

function ingest(ctx: Ctx, state: ScopeState, input: EventsInput): void {
  let duplicates = 0;
  for (const event of input.events) {
    if (!seenAdmit(state, event.id)) { duplicates += 1; continue; }
    const entry = { ...event, seenAt: ctx.now };
    windowAppend(state.open, entry, estimateRenderedBytes(entry));
    ctx.out.changed = true;
  }
  if (state.seen.size > Math.max(0, ctx.config.policy.seenCount)) ctx.out.changed = true;
  seenTrim(state, ctx.config.policy.seenCount);
  prune(ctx, state);
  removed(ctx, state, windowShed(state.open, ctx.config.policy.maxEvents), 'overflow');
  if (windowSize(state.open) === 0) state.open.endedAt = null;
  const source = input.source ?? null;
  if (source !== null && input.cursor !== undefined
    && (!Object.hasOwn(state.cursors, source) || !Object.is(state.cursors[source], input.cursor))) {
    state.cursors = { ...state.cursors, [source]: input.cursor };
    ctx.out.changed = true;
  }
  if (input.truncated) {
    if (!state.open.gap) { state.open.gap = true; ctx.out.changed = true; }
    if (ctx.config.traces) ctx.out.effects.push({ kind: 'trace', trace: { kind: 'gap', scope: state.scope, source,
      cursor: source === null ? null : (state.cursors[source] ?? null) } });
  }
  if (ctx.config.traces) ctx.out.effects.push({ kind: 'trace', trace: { kind: 'ingested', scope: state.scope, count: input.events.length - duplicates, duplicates } });
}

/** The instant the state next wants a tick: the batch's time, the open window's deadline unless judging, and each lane head's time. */
export function nextDue(policy: SettlePolicy, state: ScopeState): number | null {
  let at: number | null = null;
  const phase = state.batch?.phase;
  if (phase) at = phase.phase === 'judging' ? phase.leaseUntil : phase.phase === 'waiting' ? phase.until : phase.at;
  if (phase?.phase !== 'judging') {
    const due = windowDeadline(policy, state.open);
    if (due) at = at === null ? due.at : Math.min(at, due.at);
  }
  for (const lane of LANES) {
    const head = state.outbox[lane][0]?.phase;
    if (head) {
      const time = head.phase === 'inFlight' ? head.leaseUntil : head.at;
      at = at === null ? time : Math.min(at, time);
    }
  }
  return at;
}

function sweep(ctx: Ctx, state: ScopeState): void {
  const b = state.batch;
  if (b && b.phase.phase === 'judging' && b.phase.leaseUntil <= ctx.now) failJudge(ctx, state, { ...b, phase: b.phase }, 'judge lease expired', false);
  for (const lane of LANES) pumpLane(ctx, state, lane);
  prune(ctx, state);
  if (state.batch?.phase.phase !== 'judging') {
    const due = windowDeadline(ctx.config.policy, state.open);
    if (due && !(due.at > ctx.now)) settleBuffer(ctx, state, due.reason, true);
  }
  const held = state.batch;
  if (!held) return;
  if (held.phase.phase === 'retrying' && held.phase.at <= ctx.now) beginJudging(ctx, state, held.window, held.phase, held.phase.offerWait, held.hash);
  else if (held.phase.phase === 'waiting' && held.phase.until <= ctx.now) {
    if (windowSize(state.open) === 0) beginJudging(ctx, state, held.window, held.phase, false, held.hash);
    else settleBuffer(ctx, state, held.window.reason, false);
  }
}

function settleReason(events: Entry[], forced: boolean, fired: SettleReason): SettleReason {
  if (events.some((entry) => entry.kind === 'flush')) return 'flush';
  return forced ? 'forced' : fired;
}

function settleBuffer(ctx: Ctx, state: ScopeState, fired: SettleReason, mayWait: boolean): void {
  const held = state.batch;
  const open = state.open;
  const entries = windowCompact(open);
  let forced = open.forcedAt !== null;
  if (entries.length === 0) {
    if (open.forcedAt !== null) { open.forcedAt = null; ctx.out.changed = true; }
    if (held) beginJudging(ctx, state, { ...held.window, reason: settleReason(held.window.events, forced, held.window.reason) }, held.phase, false, held.hash);
    return;
  }
  const events = held ? [...held.window.events, ...entries] : entries;
  let dropped = open.dropped + (held?.window.dropped ?? 0);
  const gap = open.gap || (held?.window.gap ?? false);
  const waits = held?.window.waits ?? 0;
  forced = forced || held?.window.reason === 'forced';
  const gone = shedEntries(events, ctx.config.policy.maxEvents);
  removed(ctx, state, gone, 'overflow');
  dropped += gone?.length ?? 0;
  let hash: FnvState;
  if (!held && gone === null && open.hashValid) hash = open.hash;
  else if (held?.hash && gone === null) {
    hash = { ...held.hash };
    for (const entry of entries) foldId(hash, entry.id);
  } else {
    hash = fnvStart(state.scope);
    for (const entry of events) foldId(hash, entry.id);
  }
  const reason = settleReason(events, forced, fired);
  const window: Window = { id: fnvHex(hash), events, reason, waits, dropped, gap, settledAt: ctx.now };
  state.open = openWindow(state.scope);
  if (ctx.config.traces) ctx.out.effects.push({ kind: 'trace', trace: { kind: 'settled', scope: state.scope,
    batchId: window.id, reason, count: events.length, merged: held !== null } });
  beginJudging(ctx, state, window, held?.phase ?? { attempt: 0 }, mayWait && reason !== 'flush' && reason !== 'forced' && waits < ctx.config.policy.maxWaits, hash);
}

function beginJudging(ctx: Ctx, state: ScopeState, window: Window, prior: Counters, offerWait: boolean, hash: FnvState | null): void {
  const attempt = prior.attempt + 1;
  const leaseUntil = ctx.now + ctx.config.policy.judgeTimeoutMs * 2;
  state.batch = { window, phase: { phase: 'judging', attempt, failures: prior.failures ?? 0, leaseUntil, offerWait }, hash };
  ctx.out.changed = true;
  ctx.out.effects.push({ kind: 'judge', scope: state.scope, batchId: window.id, attempt, window,
    offered: offeredRoutes(offerWait), recent: state.recent as JudgeEffect['recent'], leaseUntil });
  if (ctx.config.traces) ctx.out.effects.push({ kind: 'trace', trace: { kind: 'judging', scope: state.scope, batchId: window.id, attempt, offerWait } });
}

/** Every route, with `wait` only when it is offered. */
export function offeredRoutes(offerWait: boolean): Route[] {
  return ROUTES.filter((route) => offerWait || route !== 'wait');
}

function liveBatch(ctx: Ctx, state: ScopeState, batchId: string, attempt: number, input: Input['kind']): Judging | null {
  const b = state.batch;
  if (b && b.phase.phase === 'judging' && b.window.id === batchId && b.phase.attempt === attempt) return { ...b, phase: b.phase };
  if (ctx.config.traces) ctx.out.effects.push({ kind: 'trace', trace: { kind: 'stale', scope: state.scope, input, why: `no live judge attempt ${attempt} for batch ${batchId}` } });
  return null;
}

function releaseHeld(state: ScopeState): void {
  if (state.batch) for (const entry of state.batch.window.events) seenRelease(state, entry.id);
  state.batch = null;
}

function onJudged(ctx: Ctx, state: ScopeState, input: Extract<Input, { kind: 'judged' }>): void {
  const b = liveBatch(ctx, state, input.batchId, input.attempt, 'judged');
  if (!b) return;
  const verdict = validate(input.judgment, offeredRoutes(b.phase.offerWait));
  if (verdict === null) { failJudge(ctx, state, b, `route not offered: ${String(input.judgment.route)}`, false); return; }
  const { window } = b;
  ctx.out.changed = true;
  if (verdict.route === 'wait') {
    const until = ctx.now + ctx.config.policy.waitHorizonMs;
    const waits = window.waits + 1;
    state.batch = { window: { ...window, waits }, phase: { phase: 'waiting', attempt: b.phase.attempt, until }, hash: b.hash };
    if (ctx.config.traces) ctx.out.effects.push({ kind: 'trace', trace: { kind: 'waiting', scope: state.scope, batchId: window.id, waits, until } });
    return;
  }
  const recent = [...state.recent, { id: window.id, route: verdict.route, at: ctx.now }];
  state.recent = ctx.config.policy.recentCount <= 0 ? [] : recent.slice(-ctx.config.policy.recentCount);
  if (ctx.config.traces) ctx.out.effects.push({ kind: 'trace', trace: { kind: 'judged', scope: state.scope, batchId: window.id, route: verdict.route } });
  if (verdict.route !== 'skip') {
    const decision: Decision = { id: window.id, scope: state.scope, route: verdict.route, probabilities: verdict.probabilities,
      predicates: verdict.predicates, confidence: verdict.confidence, window, at: ctx.now };
    ctx.out.appends.push({ kind: 'decision', id: decision.id, scope: state.scope, at: decision.at, route: decision.route, body: decision });
    if (ctx.config.decisions === 'lane') state.outbox.decisions.push({ item: { id: window.id, kind: 'decision', decision },
      phase: { phase: 'retrying', attempt: 0, failures: 0, at: ctx.now } });
  }
  releaseHeld(state);
}

function unit(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 && value <= 1 ? value : null;
}

/**
 * Validate the judge's answer at the boundary. The route must be offered, or the answer is
 * null. Probabilities outside [0, 1] or for routes not offered count as 0 and the vector is
 * renormalised; when it sums to 0 the chosen route gets 1. Predicates and a confidence outside
 * [0, 1] are dropped.
 */
export function validate(judgment: Judgment, offered: Route[]): (Pick<Decision, 'probabilities' | 'predicates' | 'confidence'> & { route: Route }) | null {
  const route = offered.find((candidate) => candidate === judgment.route);
  if (route === undefined) return null;
  const raw: Record<Route, number> = { interrupt: 0, queue: 0, note: 0, ask: 0, skip: 0, wait: 0 };
  let sum = 0;
  for (const candidate of offered) {
    const p = unit(judgment.probabilities?.[candidate]);
    if (p !== null) { raw[candidate] = p; sum += p; }
  }
  const probabilities = { ...raw };
  if (sum > 0) for (const candidate of ROUTES) probabilities[candidate] = raw[candidate] / sum;
  else probabilities[route] = 1;
  const predicates: Record<string, number> = {};
  for (const [name, value] of Object.entries(judgment.predicates ?? {})) {
    const p = unit(value);
    if (p !== null) predicates[name] = p;
  }
  return { route, probabilities, predicates, confidence: unit(judgment.confidence) };
}

function deadLetter(ctx: Ctx, state: ScopeState, letter: DeadLetter, attempt: number): void {
  ctx.out.appends.push({ kind: 'deadLetter', id: letter.id, scope: state.scope, at: letter.at, route: letter.decision?.route ?? null, body: letter });
  if (ctx.config.deadLetters === 'lane') state.outbox.deadLetters.push({ item: { id: letter.id, kind: 'deadLetter', letter },
    phase: { phase: 'retrying', attempt, failures: 0, at: ctx.now } });
  if (ctx.config.traces) ctx.out.effects.push({ kind: 'trace', trace: { kind: 'deadLettered', scope: state.scope, letter } });
  ctx.out.changed = true;
}

function failJudge(ctx: Ctx, state: ScopeState, b: Judging, message: string, permanent: boolean): void {
  const { window, phase, hash } = b;
  const failures = phase.failures + 1;
  const delay = permanent ? undefined : ctx.config.policy.retryBackoffMs[failures - 1];
  ctx.out.changed = true;
  if (delay !== undefined) {
    const at = ctx.now + delay;
    state.batch = { window, phase: { phase: 'retrying', attempt: phase.attempt, failures, at, offerWait: phase.offerWait }, hash };
    if (ctx.config.traces) ctx.out.effects.push({ kind: 'trace', trace: { kind: 'retrying', scope: state.scope, stage: 'judge', id: window.id, attempt: phase.attempt, at, message } });
    return;
  }
  deadLetter(ctx, state, { id: window.id, scope: state.scope, stage: 'judge', window, decision: null,
    message, permanent, attempts: phase.attempt, at: ctx.now }, 0);
  releaseHeld(state);
}

function liveDelivery(ctx: Ctx, state: ScopeState, deliveryId: string, attempt: number, input: Input['kind']): OutboxLane | null {
  for (const lane of LANES) {
    const head = state.outbox[lane][0];
    if (head && head.item.id === deliveryId && head.phase.phase === 'inFlight' && head.phase.attempt === attempt) return lane;
  }
  if (ctx.config.traces) ctx.out.effects.push({ kind: 'trace', trace: { kind: 'stale', scope: state.scope, input, why: `no live delivery attempt ${attempt} for ${deliveryId}` } });
  return null;
}

function deadLetterDelay(policy: SettlePolicy, failures: number): number {
  const list = policy.retryBackoffMs;
  const cap = policy.deadLetterBackoffMaxMs;
  const base = list[Math.min(failures, list.length) - 1] ?? cap;
  return Math.min(cap, base * 2 ** Math.max(0, failures - list.length));
}

function failDelivery(ctx: Ctx, state: ScopeState, lane: OutboxLane, message: string, permanent: boolean): void {
  const d = state.outbox[lane][0];
  if (!d || d.phase.phase !== 'inFlight') return;
  const { item } = d;
  const { attempt } = d.phase;
  const failures = d.phase.failures + 1;
  ctx.out.changed = true;
  const delay = item.kind === 'deadLetter' ? deadLetterDelay(ctx.config.policy, failures)
    : permanent ? undefined : ctx.config.policy.retryBackoffMs[failures - 1];
  if (delay !== undefined) {
    const at = ctx.now + delay;
    state.outbox[lane][0] = { item, phase: { phase: 'retrying', attempt, failures, at } };
    if (ctx.config.traces) ctx.out.effects.push({ kind: 'trace', trace: { kind: 'retrying', scope: state.scope,
      stage: item.kind === 'deadLetter' ? 'deadLetter' : 'sink', id: item.id, attempt, at, message } });
    return;
  }
  if (item.kind === 'decision') {
    state.outbox[lane].shift();
    deadLetter(ctx, state, { id: item.id, scope: state.scope, stage: 'sink', window: item.decision.window,
      decision: item.decision, message, permanent, attempts: attempt, at: ctx.now }, attempt);
  }
}

function pumpLane(ctx: Ctx, state: ScopeState, lane: OutboxLane): void {
  const head = state.outbox[lane][0];
  if (!head) return;
  if (head.phase.phase === 'inFlight') {
    if (head.phase.leaseUntil > ctx.now) return;
    failDelivery(ctx, state, lane, 'delivery lease expired', false);
  }
  const next = state.outbox[lane][0];
  if (!next || next.phase.phase !== 'retrying' || next.phase.at > ctx.now) return;
  const attempt = next.phase.attempt + 1;
  const inFlight: InFlightDelivery = { item: next.item, phase: { phase: 'inFlight', attempt, failures: next.phase.failures,
    leaseUntil: ctx.now + ctx.config.policy.deliveryTimeoutMs } };
  state.outbox[lane][0] = inFlight;
  ctx.out.changed = true;
  ctx.out.effects.push({ kind: 'deliver', scope: state.scope, delivery: inFlight });
  if (ctx.config.traces) ctx.out.effects.push({ kind: 'trace', trace: { kind: 'delivering', scope: state.scope, deliveryId: next.item.id, attempt } });
}
