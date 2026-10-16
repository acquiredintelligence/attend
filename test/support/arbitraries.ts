import fc from 'fast-check';
import { initialRecord, step } from '../oracle/step.js';
import { DEFAULT_POLICY, KINDS, ROUTES } from '../oracle/types.js';
import type { EventsInput, Input, Judgment, Kind, ScopeRecord, SettlePolicy, Stepped } from '../oracle/types.js';

export const EVENT_NAMES: readonly string[] = ['edit', 'close', 'flush', 'view', 'output', 'context'];
export const ID_POOL: readonly string[] = [
  '', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's',
  't', 'u', 'v', 'w', 'x', 'y', 'z', 'é', 'ünï', '東京', '🌍x', '🚀', 'a/b', 'a:b', 'a%b', 'a_b', '0', '1', '2', '3',
];

const timer = fc.integer({ min: 1, max: 500 });
export const policyArbitrary: fc.Arbitrary<SettlePolicy> = fc.record({
  closingGraceMs: timer,
  streamGapMs: timer,
  passiveQuietMs: timer,
  waitHorizonMs: timer,
  maxWaits: fc.integer({ min: 0, max: 3 }),
  maxEvents: fc.integer({ min: 1, max: 20 }),
  maxWindowMs: timer,
  retryBackoffMs: fc.array(timer, { maxLength: 3, size: 'max' }),
  judgeTimeoutMs: timer,
  deliveryTimeoutMs: timer,
  recentCount: fc.integer({ min: 0, max: 4 }),
  seenCount: fc.integer({ min: 0, max: 30 }),
  maxOutbox: fc.constant(DEFAULT_POLICY.maxOutbox),
  maxDeadLetters: fc.constant(DEFAULT_POLICY.maxDeadLetters),
  deadLetterBackoffMaxMs: timer,
});

export const kindsArbitrary: fc.Arbitrary<Record<string, Kind>> = fc.tuple(...EVENT_NAMES.map(() => fc.constantFrom(...KINDS)))
  .map((kinds) => Object.fromEntries(EVENT_NAMES.map((name, i) => [name, kinds[i] ?? 'context'])));

type Target = 'live' | 'attempt' | 'id' | 'missing';
interface EventChoice {
  id: number;
  name: number;
  reuse: 'pool' | 'seen' | 'open' | 'held' | 'batch';
  text: boolean;
}

/** Random choices only; the driver resolves targets against the current oracle state. */
export type PlanStep =
  | { kind: 'events'; dt: number; events: EventChoice[]; source: 'log:a' | 'mcp:b' | null; cursor: string | null | undefined; truncated: boolean }
  | { kind: 'tick'; dt: number; when: 'delta' | 'due-1' | 'due' | 'due+1' }
  | { kind: 'flush'; dt: number }
  | { kind: 'judged'; dt: number; target: Target; judgment: Judgment }
  | { kind: 'judgeFailed'; dt: number; target: Target; permanent: boolean }
  | { kind: 'judgeReleased'; dt: number; target: Target }
  | { kind: 'delivered'; dt: number; target: Target; lane: number }
  | { kind: 'deliveryFailed'; dt: number; target: Target; lane: number; permanent: boolean }
  | { kind: 'deliveryReleased'; dt: number; target: Target; lane: number };

export interface Plan {
  scope: string;
  policy: SettlePolicy;
  kinds: Record<string, Kind>;
  start: number;
  steps: PlanStep[];
}

const dt = fc.oneof(
  { weight: 14, arbitrary: fc.integer({ min: 0, max: 30 }) },
  { weight: 5, arbitrary: timer },
  { weight: 1, arbitrary: fc.integer({ min: -100, max: -1 }) },
);
const target: fc.Arbitrary<Target> = fc.oneof(
  { weight: 12, arbitrary: fc.constant('live') },
  { weight: 1, arbitrary: fc.constant('attempt') },
  { weight: 1, arbitrary: fc.constant('id') },
  { weight: 1, arbitrary: fc.constant('missing') },
);
const probability = fc.constantFrom(-0.1, 0, 0.1, 0.5, 1, 1.1);
const judgment: fc.Arbitrary<Judgment> = fc.record({
  route: fc.constantFrom(...ROUTES),
  probabilities: fc.option(fc.record({ interrupt: probability, queue: probability, note: probability, ask: probability, skip: probability, wait: probability }), { nil: undefined }),
  predicates: fc.option(fc.record({ relevant: probability, urgent: probability }), { nil: undefined }),
  confidence: fc.option(probability, { nil: undefined }),
}).map(({ route, probabilities, predicates, confidence }) => ({
  route, ...(probabilities === undefined ? {} : { probabilities }),
  ...(predicates === undefined ? {} : { predicates }), ...(confidence === undefined ? {} : { confidence }),
}));
const eventChoice: fc.Arbitrary<EventChoice> = fc.record({
  id: fc.integer({ min: 0, max: ID_POOL.length - 1 }),
  name: fc.integer({ min: 0, max: EVENT_NAMES.length - 1 }),
  reuse: fc.constantFrom('pool', 'pool', 'pool', 'seen', 'open', 'held', 'batch'),
  text: fc.boolean(),
});
const planStep: fc.Arbitrary<PlanStep> = fc.oneof(
  { weight: 12, arbitrary: fc.record({ kind: fc.constant('events'), dt, events: fc.array(eventChoice, { minLength: 1, maxLength: 20, size: 'max' }), source: fc.constantFrom('log:a', 'mcp:b', null), cursor: fc.constantFrom('c1', 'c2', null, undefined), truncated: fc.integer({ min: 0, max: 19 }).map((n) => n === 0) }) },
  { weight: 12, arbitrary: fc.record({ kind: fc.constant('tick'), dt, when: fc.constantFrom('delta', 'due-1', 'due', 'due+1') }) },
  { weight: 2, arbitrary: fc.record({ kind: fc.constant('flush'), dt }) },
  { weight: 8, arbitrary: fc.record({ kind: fc.constant('judged'), dt, target, judgment }) },
  { weight: 3, arbitrary: fc.record({ kind: fc.constant('judgeFailed'), dt, target, permanent: fc.boolean() }) },
  { weight: 2, arbitrary: fc.record({ kind: fc.constant('judgeReleased'), dt, target }) },
  { weight: 5, arbitrary: fc.record({ kind: fc.constant('delivered'), dt, target, lane: fc.integer({ min: 0, max: 1 }) }) },
  { weight: 3, arbitrary: fc.record({ kind: fc.constant('deliveryFailed'), dt, target, lane: fc.integer({ min: 0, max: 1 }), permanent: fc.boolean() }) },
  { weight: 2, arbitrary: fc.record({ kind: fc.constant('deliveryReleased'), dt, target, lane: fc.integer({ min: 0, max: 1 }) }) },
);

/** Builds shrinkable plans, including arrays hundreds of inputs long at the default bound. */
export function planArbitrary(options?: { maxSteps?: number }): fc.Arbitrary<Plan> {
  return fc.record({
    scope: fc.constantFrom('s', 'a/b', 'ünï 🌍'), policy: policyArbitrary, kinds: kindsArbitrary,
    start: fc.integer({ min: 1000, max: 10_000 }),
    steps: fc.array(planStep, { maxLength: options?.maxSteps ?? 300, size: 'max' }),
  }).map((plan) => {
    const steps: PlanStep[] = [];
    walk(plan, (choice, record) => {
      const judgeResult = choice.kind === 'judged' || choice.kind === 'judgeFailed' || choice.kind === 'judgeReleased';
      const deliveryResult = choice.kind === 'delivered' || choice.kind === 'deliveryFailed' || choice.kind === 'deliveryReleased';
      const judging = record.batch?.phase.phase === 'judging';
      const delivering = [record.outbox.decisions[0], record.outbox.deadLetters[0]].some((head) => head?.phase.phase === 'inFlight');
      const next: PlanStep = (judgeResult && !judging) || (deliveryResult && !delivering)
        ? { kind: 'tick', dt: choice.dt, when: 'delta' }
        : choice;
      steps.push(next);
      return next;
    });
    return { ...plan, steps };
  });
}

export interface Driven {
  input: Input;
  now: number;
  before: ScopeRecord;
  after: Stepped;
  edges: string[];
}

function choose<T>(items: readonly T[], index: number): T | undefined {
  return items[index % items.length];
}

function judgeTarget(record: ScopeRecord, target: Target): { batchId: string; attempt: number } {
  const batch = record.batch;
  if (target === 'missing' || batch?.phase.phase !== 'judging') return { batchId: 'missing:batch', attempt: -1 };
  return { batchId: target === 'id' ? 'wrong:batch' : batch.window.id, attempt: batch.phase.attempt + (target === 'attempt' ? 100 : 0) };
}

function deliveryTarget(record: ScopeRecord, target: Target, lane: number): { deliveryId: string; attempt: number } {
  const heads = [record.outbox.decisions[0], record.outbox.deadLetters[0]].filter((head) => head?.phase.phase === 'inFlight');
  const head = choose(heads, lane);
  if (target === 'missing' || head?.phase.phase !== 'inFlight') return { deliveryId: 'missing:delivery', attempt: -1 };
  return { deliveryId: target === 'id' ? 'wrong:delivery' : head.item.id, attempt: head.phase.attempt + (target === 'attempt' ? 100 : 0) };
}

/** Resolves a plan without randomness or clocks; records and effects come only from the frozen reducer. */
export function drive(plan: Plan): Driven[] {
  return walk(plan);
}

function walk(plan: Plan, prepare?: (choice: PlanStep, record: ScopeRecord) => PlanStep): Driven[] {
  let record = initialRecord(plan.scope);
  let now = plan.start;
  const encountered = new Set<string>();
  const driven: Driven[] = [];
  for (const planned of plan.steps) {
    const before = record;
    const choice = prepare?.(planned, before) ?? planned;
    const previousNow = now;
    const edges = new Set<string>();
    now += choice.dt;
    let input: Input;
    switch (choice.kind) {
      case 'events': {
        const seen = new Set(before.seen);
        const open = new Set(before.buffer.entries.map((entry) => entry.id));
        const held = new Set(before.batch?.window.events.map((entry) => entry.id) ?? []);
        const known = new Set([...seen, ...open, ...held]);
        const batch = new Set<string>();
        const events: EventsInput['events'] = [];
        for (const event of choice.events) {
          const fallback = ID_POOL[event.id] ?? '';
          const pool = event.reuse === 'seen' ? [...seen] : event.reuse === 'open' ? [...open] : event.reuse === 'held' ? [...held] : event.reuse === 'batch' ? [...batch] : [];
          const id = choose(pool, event.id) ?? fallback;
          if (batch.has(id)) edges.add('dup:batch');
          if (seen.has(id)) edges.add('dup:seen');
          if (open.has(id)) edges.add('dup:open');
          if (held.has(id)) edges.add('dup:held');
          if (!known.has(id) && !batch.has(id)) edges.add(encountered.has(id) ? 'id:readmitted' : 'id:new');
          encountered.add(id);
          batch.add(id);
          const name = EVENT_NAMES[event.name] ?? 'context';
          events.push({ id, at: now, name, kind: plan.kinds[name] ?? 'context', data: { n: event.id, nested: [true, null] }, ...(event.text ? { text: `event ${id}` } : {}) });
        }
        input = {
          kind: 'events', events,
          ...(choice.source === null ? {} : { source: choice.source }),
          ...(choice.cursor === undefined ? {} : { cursor: choice.cursor }),
          ...(choice.truncated ? { truncated: true } : {}),
        };
        if (input.source !== undefined) edges.add('events:source');
        if (input.source !== undefined && input.cursor === null) edges.add('events:cursorNull');
        if (input.truncated) edges.add('events:truncated');
        break;
      }
      case 'tick': {
        if (choice.when !== 'delta' && before.dueAt !== null) {
          const offset = choice.when === 'due-1' ? -1 : choice.when === 'due+1' ? 1 : 0;
          now = Math.max(previousNow, before.dueAt + offset);
        }
        if (before.dueAt !== null) {
          if (now === before.dueAt - 1) edges.add('tick:due-1');
          if (now === before.dueAt) edges.add('tick:due');
          if (now === before.dueAt + 1) edges.add('tick:due+1');
        }
        input = { kind: 'tick' };
        break;
      }
      case 'flush': input = { kind: 'flush' }; break;
      case 'judged': input = { kind: 'judged', ...judgeTarget(before, choice.target), judgment: choice.judgment }; break;
      case 'judgeFailed': input = { kind: 'judgeFailed', ...judgeTarget(before, choice.target), message: 'judge failed', permanent: choice.permanent }; break;
      case 'judgeReleased': input = { kind: 'judgeReleased', ...judgeTarget(before, choice.target) }; break;
      case 'delivered': input = { kind: 'delivered', ...deliveryTarget(before, choice.target, choice.lane) }; break;
      case 'deliveryFailed': input = { kind: 'deliveryFailed', ...deliveryTarget(before, choice.target, choice.lane), message: 'delivery failed', permanent: choice.permanent }; break;
      case 'deliveryReleased': input = { kind: 'deliveryReleased', ...deliveryTarget(before, choice.target, choice.lane) }; break;
    }
    if (now < previousNow) edges.add('time:backward');
    if ('batchId' in input) {
      const batch = before.batch;
      const live = batch?.phase.phase === 'judging' && batch.window.id === input.batchId && batch.phase.attempt === input.attempt;
      edges.add(live ? 'result:live' : 'result:stale');
      if (input.kind === 'judged' && live && batch.phase.phase === 'judging' && input.judgment.route === 'wait' && !batch.phase.offerWait) edges.add('judged:notOffered');
    }
    if ('deliveryId' in input) {
      const live = [before.outbox.decisions[0], before.outbox.deadLetters[0]].some((head) => head?.phase.phase === 'inFlight' && head.item.id === input.deliveryId && head.phase.attempt === input.attempt);
      edges.add(live ? 'result:live' : 'result:stale');
    }
    const after = step({ policy: plan.policy }, before, input, now);
    if (input.kind === 'events' && before.seen.some((id) => !after.record.seen.includes(id))) edges.add('seen:evicted');
    for (const effect of after.effects) {
      if (effect.kind !== 'trace') {
        edges.add(`effect:${effect.kind}`);
        continue;
      }
      const trace = effect.trace;
      edges.add(`trace:${trace.kind}`);
      switch (trace.kind) {
        case 'ingested': if (trace.duplicates > 0) edges.add('trace:ingested:duplicates'); break;
        case 'dropped': edges.add(`trace:dropped:${trace.why}`); break;
        case 'settled':
          edges.add(`trace:settled:${trace.reason}`);
          if (trace.merged) edges.add('trace:settled:merged');
          break;
        case 'judging': edges.add(trace.offerWait ? 'trace:judging:offerWait' : 'trace:judging:noWait'); break;
        case 'judged': edges.add(`trace:judged:${trace.route}`); break;
        case 'retrying': edges.add(`trace:retrying:${trace.stage}`); break;
        case 'released': edges.add(`trace:released:${trace.stage}`); break;
        case 'deadLettered': edges.add(`trace:deadLettered:${trace.letter.stage}`); break;
        case 'stale': edges.add(`trace:stale:${trace.input}`); break;
      }
    }
    record = after.record;
    driven.push({ input, now, before, after, edges: [...edges] });
  }
  return driven;
}

export interface Coverage {
  plans: number;
  steps: number;
  inputs: Record<string, number>;
  edges: Record<string, number>;
}

/** Counts each edge once per driven step. */
export function coverage(runs: readonly (readonly Driven[])[]): Coverage {
  const result: Coverage = { plans: runs.length, steps: 0, inputs: {}, edges: {} };
  for (const run of runs) for (const driven of run) {
    result.steps += 1;
    result.inputs[driven.input.kind] = (result.inputs[driven.input.kind] ?? 0) + 1;
    for (const edge of driven.edges) result.edges[edge] = (result.edges[edge] ?? 0) + 1;
  }
  return result;
}
