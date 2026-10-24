/**
 * The runtime half of the shared conformance suite: a language-neutral scenario (timed pushes,
 * flushes and ends per scope, a settle policy, scripted judge, sink and dead-letter answers) run
 * through `createAttend` on a fake clock over the memory store. `python/tests/conformance.py`
 * runs the same scenarios through the Python port, and both must observe exactly what the
 * fixture records.
 */
import { createAttend, PermanentError } from '../../src/attend.js';
import { memoryStore } from '../../src/memory.js';
import { drain, fakeClock } from '../../src/testing.js';
import type {
  DeadLetter, Decision, DecisionRow, DeliveredRoute, JsonObject, JsonValue, Judge, JudgeBudget, JudgeImage, JudgeInput, Judgment, Kind, Route, ScopeRecord,
  SettlePolicy, Trace,
} from '../../src/types.js';

/** What a scripted judge does on one call. */
export type Answer =
  | Judgment
  | { fail: string; permanent?: boolean }
  /** Rejects with `Error('aborted')` once its signal aborts. */
  | { hang: true }
  /** Never settles and ignores its signal. */
  | { ignore: true }
  /** Settles when a `release` step names it. */
  | { hold: string }
  /** Resolves with this value, which is not a judgment. */
  | { malformed: JsonValue };

/** The first rule whose conditions all hold answers. `once` fires at most once per window id, and `first` only on a scope's first call. */
export interface Rule {
  has?: string[];
  lacks?: string[];
  offers?: Route;
  once?: boolean;
  first?: boolean;
  answer: Answer;
}

/** Answers in order with the last one repeating, or the first matching rule. */
export type JudgeScript = { answers: Answer[] } | { rules: Rule[] };

/** What a scripted sink or dead-letter handler does on one attempt. */
export type Attempt = 'ok' | { fail: string; permanent?: boolean } | { hang: true } | { hold: string };

/** Attempts in order, then `ok`; or the first rule matching the decision's route. */
export type HandlerScript = { attempts: Attempt[] } | { rules: { route?: DeliveredRoute; answer: Attempt }[] };

/** What `capture` returns on one call, in order with the last one repeating. */
export type CaptureAnswer = JudgeImage | JudgeImage[] | null | { fail: string } | { hang: true };

export interface EventSpec {
  id: string;
  name: string;
  at?: number;
  data?: JsonObject;
  text?: string;
}

export type Step =
  | { push: string; events: EventSpec[] }
  | { advance: number }
  | { flush: string }
  | { end: string }
  | { release: string; answer?: Judgment; fail?: string; permanent?: boolean }
  | { inspect: string };

export interface Scenario {
  name: string;
  /** The TypeScript test this scenario mirrors, when there is one. */
  from?: string;
  /** The fake clock's start. Default 1000. */
  start?: number;
  /** Default 1000. */
  sweepMs?: number;
  settle?: Partial<SettlePolicy>;
  /** Every event name a scenario uses, mapped to its kind. */
  kinds: Record<string, Kind>;
  /** By scope, with `*` for every scope without its own script. */
  judge: Record<string, JudgeScript>;
  /** Default `{ '*': { attempts: [] } }`: every delivery succeeds. */
  sink?: 'log' | Record<string, HandlerScript>;
  /** Default `log`. */
  deadLetter?: 'log' | Record<string, HandlerScript>;
  capture?: Record<string, CaptureAnswer[]>;
  config?: {
    guidance?: string;
    criteria?: Partial<Record<Route, string>>;
    predicates?: Record<string, string>;
    /** `context()` facts by scope, with `*` as the fallback. */
    facts?: Record<string, JsonObject>;
    /** Predicates `context()` adds, for every scope. */
    contextPredicates?: Record<string, string>;
    actThreshold?: number;
    windowShare?: number;
    judgeConcurrency?: number;
    budget?: JudgeBudget;
    captureTimeoutMs?: number;
  };
  steps: Step[];
}

export interface Observed {
  judgeCalls: Record<string, JudgeInput[]>;
  deliveries: Record<string, { decision: Decision; outcome: string }[]>;
  deadLetterCalls: Record<string, { letter: DeadLetter; outcome: string }[]>;
  traces: Record<string, Trace[]>;
  /** One per push, flush and end step: null when it resolved, else the error's name and message. */
  results: { step: number; error: { name: string; message: string } | null }[];
  inspections: { step: number; scope: string; record: ScopeRecord | null }[];
  records: Record<string, ScopeRecord | null>;
  log: DecisionRow[];
}

/** Trace kinds left out of the comparison: they report process-wide intake, not a scope's behaviour. */
export const UNCOMPARED_TRACES = new Set(['backpressure']);

function pick<T>(scripts: Record<string, T> | undefined, scope: string): T | undefined {
  return scripts?.[scope] ?? scripts?.['*'];
}

function outcomeOf(attempt: Attempt): string {
  if (attempt === 'ok') return 'ok';
  if ('fail' in attempt) return `${attempt.permanent ? 'permanent' : 'fail'}: ${attempt.fail}`;
  if ('hang' in attempt) return 'hang';
  return `hold: ${attempt.hold}`;
}

function push<T>(map: Record<string, T[]>, key: string, value: T): void {
  (map[key] ??= []).push(value);
}

const json = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;

/** Runs one scenario to the end, stops the runner, and returns everything it observed as plain JSON. */
export async function runScenario(scenario: Scenario): Promise<Observed> {
  const clock = fakeClock(scenario.start ?? 1000);
  const store = memoryStore();
  const observed: Observed = { judgeCalls: {}, deliveries: {}, deadLetterCalls: {}, traces: {}, results: [], inspections: [], records: {}, log: [] };
  const holds = new Map<string, { resolve: (value: unknown) => void; reject: (error: unknown) => void }>();
  const answered = new Map<string, number>();
  const fired = new Set<string>();
  const attempts = new Map<string, number>();
  const captures = new Map<string, number>();

  const held = (name: string) => new Promise<unknown>((resolve, reject) => { holds.set(name, { resolve, reject }); });
  const failure = (message: string, permanent?: boolean) => (permanent ? new PermanentError(message) : new Error(message));
  const settle = (answer: Answer, signal: AbortSignal): Promise<unknown> => {
    if ('fail' in answer) return Promise.reject(failure(answer.fail, answer.permanent));
    if ('hang' in answer) return new Promise((_, reject) => signal.addEventListener('abort', () => reject(new Error('aborted')), { once: true }));
    if ('ignore' in answer) return new Promise(() => undefined);
    if ('hold' in answer) return held(answer.hold);
    if ('malformed' in answer) return Promise.resolve(answer.malformed);
    return Promise.resolve(structuredClone(answer));
  };

  const judge: Judge = async (input, signal) => {
    push(observed.judgeCalls, input.scope, json(input));
    const script = pick(scenario.judge, input.scope);
    if (script === undefined) throw new Error(`no judge script for scope ${input.scope}`);
    let answer: Answer | undefined;
    const n = answered.get(input.scope) ?? 0;
    answered.set(input.scope, n + 1);
    if ('answers' in script) {
      answer = script.answers[Math.min(n, script.answers.length - 1)];
    } else {
      const ids = new Set(input.window.events.map((event) => event.id));
      for (const [index, rule] of script.rules.entries()) {
        if (rule.has && !rule.has.every((id) => ids.has(id))) continue;
        if (rule.lacks && rule.lacks.some((id) => ids.has(id))) continue;
        if (rule.offers && !input.routes.some((route) => route.route === rule.offers)) continue;
        if (rule.first && n > 0) continue;
        const key = `${index}:${input.window.id}`;
        if (rule.once && fired.has(key)) continue;
        if (rule.once) fired.add(key);
        answer = rule.answer;
        break;
      }
    }
    if (answer === undefined) throw new Error('no scripted answer');
    return (await settle(answer, signal)) as Judgment;
  };
  if (scenario.config?.budget) judge.budget = scenario.config.budget;

  const handler = (scripts: Record<string, HandlerScript>, lane: 'sink' | 'deadLetter') =>
    async (scope: string, route: DeliveredRoute | null, signal: AbortSignal): Promise<string> => {
      const script = pick(scripts, scope);
      let attempt: Attempt = 'ok';
      if (script && 'attempts' in script) {
        const key = `${lane}:${scope}`;
        const n = attempts.get(key) ?? 0;
        attempts.set(key, n + 1);
        attempt = script.attempts[n] ?? 'ok';
      } else if (script) {
        attempt = script.rules.find((rule) => rule.route === undefined || rule.route === route)?.answer ?? 'ok';
      }
      const outcome = outcomeOf(attempt);
      if (attempt !== 'ok') return settle(attempt, signal).then(() => outcome);
      return outcome;
    };

  const sinkScripts = scenario.sink ?? { '*': { attempts: [] } };
  const deliver = sinkScripts === 'log' ? null : handler(sinkScripts, 'sink');
  const letterScripts = scenario.deadLetter ?? 'log';
  const handle = letterScripts === 'log' ? null : handler(letterScripts, 'deadLetter');
  const config = scenario.config ?? {};

  const attend = createAttend({
    store,
    clock,
    kind: scenario.kinds,
    judge,
    sink: deliver === null ? 'log' : async (decision, signal) => {
      const entry = { decision: json(decision), outcome: 'pending' };
      push(observed.deliveries, decision.scope, entry);
      try { entry.outcome = await deliver(decision.scope, decision.route, signal); }
      catch (error) { entry.outcome = error instanceof Error ? `${error.name}: ${error.message}` : String(error); throw error; }
    },
    deadLetter: handle === null ? 'log' : async (letter, signal) => {
      const entry = { letter: json(letter), outcome: 'pending' };
      push(observed.deadLetterCalls, letter.scope, entry);
      try { entry.outcome = await handle(letter.scope, letter.decision?.route ?? null, signal); }
      catch (error) { entry.outcome = error instanceof Error ? `${error.name}: ${error.message}` : String(error); throw error; }
    },
    observe: (trace) => {
      if (UNCOMPARED_TRACES.has(trace.kind) || !('scope' in trace)) return;
      push(observed.traces, trace.scope, json(trace));
    },
    ...(config.facts || config.contextPredicates ? {
      context: (scope: string) => {
        const facts = pick(config.facts, scope);
        return { ...(facts ? { facts } : {}), ...(config.contextPredicates ? { predicates: config.contextPredicates } : {}) };
      },
    } : {}),
    ...(scenario.capture ? {
      capture: async (scope: string, signal: AbortSignal) => {
        const answers = pick(scenario.capture, scope) ?? [null];
        const n = captures.get(scope) ?? 0;
        captures.set(scope, n + 1);
        const answer = answers[Math.min(n, answers.length - 1)] ?? null;
        if (answer !== null && !Array.isArray(answer) && 'fail' in answer) throw new Error(answer.fail);
        if (answer !== null && !Array.isArray(answer) && 'hang' in answer) return new Promise<never>(() => { void signal; });
        return answer;
      },
    } : {}),
    ...(config.guidance === undefined ? {} : { guidance: config.guidance }),
    ...(config.criteria === undefined ? {} : { criteria: config.criteria }),
    ...(config.predicates === undefined ? {} : { predicates: config.predicates }),
    ...(config.actThreshold === undefined ? {} : { actThreshold: config.actThreshold }),
    ...(config.windowShare === undefined ? {} : { windowShare: config.windowShare }),
    ...(config.judgeConcurrency === undefined ? {} : { judgeConcurrency: config.judgeConcurrency }),
    ...(config.captureTimeoutMs === undefined ? {} : { captureTimeoutMs: config.captureTimeoutMs }),
    ...(scenario.settle ? { settle: scenario.settle } : {}),
  });

  const controller = new AbortController();
  const running = attend.run({ signal: controller.signal, sweepMs: scenario.sweepMs ?? 1000 });
  const scopes = new Set<string>();
  const result = async (step: number, work: () => Promise<void>) => {
    try {
      await work();
      observed.results.push({ step, error: null });
    } catch (error) {
      observed.results.push({ step, error: error instanceof Error ? { name: error.name, message: error.message } : { name: 'thrown', message: String(error) } });
    }
  };

  for (const [index, step] of scenario.steps.entries()) {
    if ('push' in step) {
      scopes.add(step.push);
      const events = step.events.map((e) => ({ id: e.id, at: e.at ?? 0, name: e.name, data: e.data ?? {}, ...(e.text === undefined ? {} : { text: e.text }) }));
      await result(index, () => attend.push(step.push, events));
    } else if ('advance' in step) {
      await clock.advance(step.advance);
    } else if ('flush' in step) {
      scopes.add(step.flush);
      await result(index, () => attend.flush(step.flush));
    } else if ('end' in step) {
      scopes.add(step.end);
      await result(index, () => attend.end(step.end));
    } else if ('release' in step) {
      const hold = holds.get(step.release);
      if (!hold) throw new Error(`nothing holds ${step.release}`);
      holds.delete(step.release);
      if (step.fail !== undefined) hold.reject(failure(step.fail, step.permanent));
      else hold.resolve(step.answer ?? 'ok');
    } else {
      scopes.add(step.inspect);
      observed.inspections.push({ step: index, scope: step.inspect, record: json(await attend.inspect(step.inspect)) });
    }
    await drain();
  }
  controller.abort();
  await running;
  for (const scope of [...scopes].sort()) observed.records[scope] = json(await attend.inspect(scope));
  observed.log = json((await store.log.read({ after: null, limit: 10_000 })).rows);
  return observed;
}
