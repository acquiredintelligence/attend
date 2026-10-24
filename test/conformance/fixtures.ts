/**
 * Builders for the pure fixtures of the shared conformance suite: reducer traces from seeded
 * random plans, window ids, the act threshold and `validate`, and `defaultNotice`. Each builder
 * computes its expected values with `src`, so `pnpm conformance:update` records today's behaviour
 * and the TypeScript runner fails if it ever changes without the fixtures changing with it.
 */
import { createHash } from 'node:crypto';
import fc from 'fast-check';
import { defaultNotice } from '../../src/attend.js';
import { initialRecord, step, validate, windowId } from '../../src/step.js';
import { utf8Length } from '../../src/core/ids.js';
import { suggestThreshold, thresholdRoute } from '../../src/core/threshold.js';
import { stableJson } from '../../src/internal/json.js';
import { KINDS, ROUTES } from '../../src/types.js';
import type { Decision, DeliveredRoute, Effect, Input, Judgment, Kind, Route, ScopeRecord, SettlePolicy, StepConfig } from '../../src/types.js';
import { EVENT_NAMES, ID_POOL, planArbitrary, type PlanStep } from '../support/arbitraries.js';

/** sha256 of `stableJson(value)`, as 16 hex characters. */
export function digest(value: unknown): string {
  return createHash('sha256').update(stableJson(value)).digest('hex').slice(0, 16);
}

/** mulberry32: a seeded generator, so a fixture never depends on Math.random. */
function prng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export interface StepFixtureStep {
  input: Input;
  now: number;
  effects: Effect[];
  /** `digest(record)` after this step. */
  digest: string;
  /** The full record after this step, every `FULL_EVERY` steps and after the last. */
  record?: ScopeRecord;
}

export interface StepFixturePlan {
  name: string;
  scope: string;
  config: Required<StepConfig>;
  steps: StepFixtureStep[];
}

const FULL_EVERY = 16;
type Target = 'live' | 'attempt' | 'id' | 'missing';

function judgeTarget(record: ScopeRecord, target: Target): { batchId: string; attempt: number } {
  const batch = record.batch;
  if (target === 'missing' || batch?.phase.phase !== 'judging') return { batchId: 'missing:batch', attempt: -1 };
  return { batchId: target === 'id' ? 'wrong:batch' : batch.window.id, attempt: batch.phase.attempt + (target === 'attempt' ? 100 : 0) };
}

function deliveryTarget(record: ScopeRecord, target: Target, lane: number): { deliveryId: string; attempt: number } {
  const heads = [record.outbox.decisions[0], record.outbox.deadLetters[0]].filter((head) => head?.phase.phase === 'inFlight');
  const head = heads[lane % Math.max(1, heads.length)];
  if (target === 'missing' || head?.phase.phase !== 'inFlight') return { deliveryId: 'missing:delivery', attempt: -1 };
  return { deliveryId: target === 'id' ? 'wrong:delivery' : head.item.id, attempt: head.phase.attempt + (target === 'attempt' ? 100 : 0) };
}

/**
 * Drives one plan through `src` `step`, resolving judge and delivery targets against the live
 * record the way `test/support/arbitraries.ts` does, and splicing in `end` and `judgeDeferred`
 * inputs, which the plan arbitrary does not generate.
 */
function drivePlan(name: string, plan: { scope: string; policy: SettlePolicy; kinds: Record<string, Kind>; start: number; steps: PlanStep[] }, config: Required<StepConfig>, random: () => number): StepFixturePlan {
  let record = initialRecord(plan.scope);
  let now = plan.start;
  const steps: StepFixtureStep[] = [];
  const apply = (input: Input) => {
    const after = step(config, record, input, now);
    record = after.record;
    const entry: StepFixtureStep = { input, now, effects: after.effects, digest: digest(record) };
    if (steps.length % FULL_EVERY === FULL_EVERY - 1) entry.record = record;
    steps.push(JSON.parse(JSON.stringify(entry)) as StepFixtureStep);
  };
  for (const choice of plan.steps) {
    const before = record;
    const previous = now;
    now += choice.dt;
    let input: Input;
    switch (choice.kind) {
      case 'events': {
        const seen = [...before.seen];
        const open = before.buffer.entries.map((entry) => entry.id);
        const held = before.batch?.window.events.map((entry) => entry.id) ?? [];
        const batch: string[] = [];
        const events = choice.events.map((event) => {
          const pool = event.reuse === 'seen' ? seen : event.reuse === 'open' ? open : event.reuse === 'held' ? held : event.reuse === 'batch' ? batch : [];
          const id = pool.length > 0 ? pool[event.id % pool.length]! : (ID_POOL[event.id] ?? '');
          batch.push(id);
          const eventName = EVENT_NAMES[event.name] ?? 'context';
          return { id, at: now, name: eventName, kind: plan.kinds[eventName] ?? 'context', data: { n: event.id, nested: [true, null], '10': 'ten', '9': { b: 1, a: [{ '2': 2, '1': 1 }] } }, ...(event.text ? { text: `event ${id} ${'é🌍'.repeat(event.id % 3)}` } : {}) };
        });
        input = { kind: 'events', events, ...(choice.source === null ? {} : { source: choice.source }), ...(choice.cursor === undefined ? {} : { cursor: choice.cursor }), ...(choice.truncated ? { truncated: true } : {}) };
        break;
      }
      case 'tick':
        if (choice.when !== 'delta' && before.dueAt !== null) now = Math.max(previous, before.dueAt + (choice.when === 'due-1' ? -1 : choice.when === 'due+1' ? 1 : 0));
        input = { kind: 'tick' };
        break;
      case 'flush': input = random() < 0.4 ? { kind: 'end' } : { kind: 'flush' }; break;
      case 'judged': input = { kind: 'judged', ...judgeTarget(before, choice.target), judgment: choice.judgment }; break;
      case 'judgeFailed': input = { kind: 'judgeFailed', ...judgeTarget(before, choice.target), message: 'judge failed', permanent: choice.permanent }; break;
      case 'judgeReleased':
        input = random() < 0.5
          ? { kind: 'judgeDeferred', ...judgeTarget(before, choice.target), until: now + Math.floor(random() * 200) }
          : { kind: 'judgeReleased', ...judgeTarget(before, choice.target) };
        break;
      case 'delivered': input = { kind: 'delivered', ...deliveryTarget(before, choice.target, choice.lane) }; break;
      case 'deliveryFailed': input = { kind: 'deliveryFailed', ...deliveryTarget(before, choice.target, choice.lane), message: 'delivery failed', permanent: choice.permanent }; break;
      case 'deliveryReleased': input = { kind: 'deliveryReleased', ...deliveryTarget(before, choice.target, choice.lane) }; break;
    }
    apply(input);
    if (random() < 0.05) apply({ kind: 'end' });
  }
  const last = steps.at(-1);
  if (last) last.record = JSON.parse(JSON.stringify(record)) as ScopeRecord;
  return { name, scope: plan.scope, config, steps };
}

/** Seeded plans of up to `maxSteps` inputs. A third get a byte budget, and a quarter run in log modes. */
export function stepFixtures(seed: number, count: number, maxSteps: number): StepFixturePlan[] {
  const plans = fc.sample(planArbitrary({ maxSteps }), { seed, numRuns: count });
  const random = prng(seed);
  return plans.map((plan, index) => {
    const policy: SettlePolicy = { ...plan.policy };
    if (index % 3 === 1) policy.maxWindowBytes = 60 + Math.floor(random() * 600);
    const config: Required<StepConfig> = {
      policy,
      decisions: index % 4 === 2 ? 'log' : 'lane',
      deadLetters: index % 4 === 2 || index % 4 === 3 ? 'log' : 'lane',
    };
    return drivePlan(`seed ${seed} plan ${index}`, plan, config, random);
  });
}

const ID_TEXT = fc.oneof(
  fc.constantFrom('', 'ascii', 'é東京', '🌍🚀', '\ud800', '\udfff', '\ud800a\udfff', 'a/b', 'scope'),
  fc.array(fc.integer({ min: 0, max: 0xffff }), { maxLength: 24 }).map((units) => String.fromCharCode(...units)),
  fc.string({ unit: 'binary', maxLength: 24 }),
  fc.string({ unit: 'grapheme', maxLength: 12 }),
);

/** Window ids for pinned and random scopes and ids, lone surrogates included, and UTF-8 lengths. */
export function idFixtures(seed: number, count: number) {
  const pinned: [string, string[]][] = [
    ['scope', ['a']], ['scope', ['e']], ['scope', ['f']], ['scope', ['b', 'd']], ['scope', ['a', 'b']], ['s', ['a']], ['s', ['a', 'b', 'c']],
    ['m/x', ['m/x/a']], ['m/y', ['m/y/b', 'm/y/d']], ['', []], ['', ['']], ['ünï 🌍', ['東京', 'é']],
  ];
  const random = fc.sample(fc.tuple(ID_TEXT, fc.array(ID_TEXT, { maxLength: 6 })), { seed, numRuns: count });
  const cases = [...pinned, ...random].map(([scope, ids]) => ({ scope, ids, id: windowId(scope, ids) }));
  const texts = fc.sample(ID_TEXT, { seed: seed + 1, numRuns: count });
  return { cases, utf8: texts.map((text) => ({ text, length: utf8Length(text) })) };
}

const PROBABILITY = fc.constantFrom(-0.1, 0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.7, 1, 1.1, Number.NaN);

/** `thresholdRoute`, `validate` and `suggestThreshold` over random judgments and offers. NaN is written as null. */
export function thresholdFixtures(seed: number, count: number) {
  const judgment: fc.Arbitrary<Judgment> = fc.record({
    route: fc.constantFrom(...ROUTES),
    probabilities: fc.option(fc.dictionary(fc.constantFrom(...ROUTES), PROBABILITY, { maxKeys: 6 }), { nil: undefined }),
    predicates: fc.option(fc.dictionary(fc.constantFrom('asked', 'urgent', 'odd'), PROBABILITY, { maxKeys: 3 }), { nil: undefined }),
    confidence: fc.option(PROBABILITY, { nil: undefined }),
  }).map(({ route, probabilities, predicates, confidence }) => ({
    route, ...(probabilities === undefined ? {} : { probabilities }), ...(predicates === undefined ? {} : { predicates }), ...(confidence === undefined ? {} : { confidence }),
  }));
  const offered = fc.subarray([...ROUTES] as Route[]).map((routes) => ROUTES.filter((r) => routes.includes(r)));
  const thresholds = fc.constantFrom(0.1, 0.3, 0.5, 0.6, 0.8, 1);
  const json = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;
  const routeCases = fc.sample(fc.tuple(judgment, offered, thresholds), { seed, numRuns: count })
    .map(([j, o, t]) => json({ judgment: j, offered: o, threshold: t, result: thresholdRoute(json(j), o, t) }));
  const validateCases = fc.sample(fc.tuple(judgment, offered), { seed: seed + 1, numRuns: count })
    .map(([j, o]) => json({ judgment: j, offered: o, result: validate(json(j), o) }));
  const suggestCases = fc.sample(fc.tuple(fc.array(judgment, { maxLength: 12 }), fc.constantFrom(600_000, 3_600_000), fc.constantFrom(0, 2, 6, 60)), { seed: seed + 2, numRuns: Math.ceil(count / 4) })
    .map(([js, durationMs, maxActsPerHour]) => json({ judgments: js, options: { durationMs, maxActsPerHour }, result: suggestThreshold(json(js), { durationMs, maxActsPerHour }) }));
  return { thresholdRoute: routeCases, validate: validateCases, suggestThreshold: suggestCases };
}

/** `defaultNotice` over windows with context, gaps, drops, data with integer-like keys and line limits. */
export function noticeFixtures() {
  const decision = (route: DeliveredRoute, events: Decision['window']['events'], extra: Partial<Decision['window']> = {}): Decision => ({
    id: 'w', scope: 's', route, probabilities: { interrupt: 0, queue: 0, note: 0, ask: 0, skip: 0, wait: 0 }, predicates: {}, confidence: null,
    window: { id: 'w', events, reason: 'closing', waits: 0, dropped: 0, gap: false, settledAt: 5, ...extra }, at: 5,
  });
  const entry = (id: string, kind: Kind, extra: Partial<Decision['window']['events'][number]> = {}) => ({ id, at: 0, name: `${kind}.${id}`, data: {}, kind, seenAt: 1, ...extra });
  const many = Array.from({ length: 25 }, (_, i) => entry(`e${i}`, KINDS[i % KINDS.length]!, i % 2 === 0 ? { text: `line ${i}` } : { data: { i, '10': 'ten', '2': 'two', b: [1.5, -0, 1e21, 1e-7, 0.000001] } }));
  const cases: { decision: Decision; options?: { maxLines: number }; text: string }[] = [];
  const add = (d: Decision, options?: { maxLines: number }) => cases.push({ decision: d, ...(options ? { options } : {}), text: defaultNotice(d, options) });
  add(decision('queue', [entry('a', 'closing', { text: 'close a' }), entry('b', 'context', { data: { words: 2 } }), entry('c', 'consequence', { data: { ok: true } })], { dropped: 2, gap: true }));
  add(decision('interrupt', [entry('a', 'closing', { text: 'close a' }), entry('b', 'context'), entry('c', 'consequence', { data: { ok: true } })], { dropped: 2, gap: true }), { maxLines: 1 });
  for (const route of ['interrupt', 'queue', 'note', 'ask'] as const) add(decision(route, many));
  add(decision('note', many, { dropped: 3 }), { maxLines: 5 });
  add(decision('ask', [entry('u', 'flush', { data: { s: 'ünï 🌍 "q" \\ \n\t\u0001', nested: { z: null, a: [{}] } } })]));
  add(decision('queue', []));
  return { cases: JSON.parse(JSON.stringify(cases)) as typeof cases };
}

export type { Kind };
