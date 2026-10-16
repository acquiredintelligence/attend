/**
 * The shell. Thin: load, step, compare-and-swap, arm a timer from `dueAt`, execute
 * effects, feed results back as inputs. All policy is in ./step.ts.
 */
import { initialRecord, step, type StepConfig } from './step.js';
import { isPermanent, OutboxFullError } from './errors.js';
import type {
  Clock,
  DeadLetter,
  Decision,
  DeliveredRoute,
  DeliverEffect,
  Effect,
  Event,
  EventLog,
  Input,
  Judge,
  JudgeEffect,
  JudgeInput,
  Judgment,
  JsonObject,
  Kinded,
  Route,
  ScopeRecord,
  SettlePolicy,
  Sink,
  Source,
  StateStore,
  Trace,
} from './types.js';
import { DEFAULT_POLICY, ROUTES } from './types.js';

export { OutboxFullError, PermanentError } from './errors.js';

/** Live context fetched when the judge is called, and again on every retry. */
export interface JudgeContext {
  facts?: JsonObject;
  /** Questions the judge answers beside the route, by name. Merged over `AttendConfig.predicates`. */
  predicates?: Record<string, string>;
}

export interface AttendConfig {
  store: StateStore;
  judge: Judge;
  sink: Sink;
  /** Pure. Called once per event at ingest; the result is stored on the entry. */
  kind: (event: Event) => Kinded;
  /** Receives windows no judge or sink could take. Required: there is no silent default. */
  deadLetter: (letter: DeadLetter, signal: AbortSignal) => void | Promise<void>;
  settle?: Partial<SettlePolicy>;
  /** Who the agent is and what matters to its user. Passed to the judge as given. */
  guidance?: string;
  /** Per-route criteria for the judge. Judges fall back to their own for routes left out. */
  criteria?: Partial<Record<Route, string>>;
  /** Static questions the judge answers beside the route, by name. */
  predicates?: Record<string, string>;
  context?: (scope: string, signal: AbortSignal) => JudgeContext | Promise<JudgeContext>;
  /** Every trace the reducer emits, plus the shell's own. Optional; nothing else observes. */
  observe?: (trace: Trace) => void;
  clock?: Clock;
}

export interface RunOptions {
  signal: AbortSignal;
  /** How often to ask the store for due scopes. The only path for scopes written by other processes. Default 1000. */
  sweepMs?: number;
  /** Scopes per sweep. Default 100. */
  sweepLimit?: number;
}

export interface Attend {
  /**
   * Ingest from a producer in this process. Persists with compare-and-swap and resolves
   * once persisted; never judges or delivers (the runner does, via `dueAt`). Safe from any
   * replica; idempotent on event ids. Rejects with `OutboxFullError` at `maxOutbox`.
   */
  push(scope: string, events: Event[]): Promise<void>;
  /**
   * Run a source into a scope from the record's cursor until `signal` or the run aborts.
   * Resumes after `OutboxFullError` with backoff; rejects on any other source error.
   */
  attach(scope: string, source: Source, options?: { signal?: AbortSignal }): Promise<void>;
  /** Settle the open window now, reason `forced`. */
  flush(scope: string): Promise<void>;
  /** The record as stored, for operators and tests. */
  inspect(scope: string): Promise<ScopeRecord | null>;
  /** The runner: in-process timers for scopes touched here, the sweep for everything. Resolves on abort. */
  run(options: RunOptions): Promise<void>;
}

const MAX_CAS_ATTEMPTS = 8;

/** Thrown once the compare-and-swap budget is spent. Safe to drop: every input is idempotent and leases recover in-flight work. */
class AbandonedError extends Error {}

export function createAttend(config: AttendConfig): Attend {
  const policy: SettlePolicy = { ...DEFAULT_POLICY, ...config.settle };
  const stepConfig: StepConfig = { kind: config.kind, policy };
  const clock = config.clock ?? systemClock();
  const lanes = new Map<string, Promise<void>>();
  const timers = new Map<string, () => void>();
  const sources = new Set<AbortController>();
  let running: { signal: AbortSignal } | null = null;

  function observe(trace: Trace): void {
    try {
      config.observe?.(trace);
    } catch {
      // An observer that throws must not take the runner down with it.
    }
  }

  /** Per-scope inputs are serialized in this process, so one process never races itself. */
  function apply(scope: string, input: Input): Promise<void> {
    const previous = lanes.get(scope) ?? Promise.resolve();
    const next = previous.then(() => applyNow(scope, input));
    const settled = next.catch(() => undefined).finally(() => {
      if (lanes.get(scope) === settled) lanes.delete(scope);
    });
    lanes.set(scope, settled);
    return next;
  }

  async function applyNow(scope: string, input: Input): Promise<void> {
    for (let attempt = 1; attempt <= MAX_CAS_ATTEMPTS; attempt += 1) {
      const current = (await config.store.get(scope)) ?? initialRecord(scope);
      if (input.kind === 'events' && input.events.length > 0 && current.outbox.length >= policy.maxOutbox) {
        throw new OutboxFullError(scope, current.outbox.length);
      }
      const { record, effects } = step(stepConfig, current, input, clock.now());
      const next = { ...record, version: current.version + 1 };
      if ((await config.store.put(next, current.version)) === 'conflict') {
        observe({ kind: 'conflict', scope, version: current.version });
        continue;
      }
      arm(scope, next.dueAt);
      for (const effect of effects) execute(scope, effect);
      return;
    }
    observe({ kind: 'abandoned', scope, input: input.kind });
    throw new AbandonedError(`attend: gave up on scope "${scope}" after ${MAX_CAS_ATTEMPTS} conflicts`);
  }

  function arm(scope: string, dueAt: number | null): void {
    timers.get(scope)?.();
    timers.delete(scope);
    if (!running || dueAt === null) return;
    timers.set(
      scope,
      clock.at(dueAt, () => {
        timers.delete(scope);
        tick(scope);
      }),
    );
  }

  function tick(scope: string): void {
    background(scope, 'tick', () => apply(scope, { kind: 'tick' }));
  }

  function background(scope: string, input: Input['kind'], work: () => Promise<void>): void {
    void work().catch((error: unknown) => {
      if (!(error instanceof AbandonedError)) observe({ kind: 'failed', scope, input, message: messageOf(error) });
    });
  }

  function execute(scope: string, effect: Effect): void {
    switch (effect.kind) {
      case 'trace':
        observe(effect.trace);
        return;
      case 'judge':
        background(scope, 'judged', () => runJudge(scope, effect));
        return;
      case 'deliver':
        background(scope, 'delivered', () => runDeliver(scope, effect));
        return;
      default:
        return assertNever(effect);
    }
  }

  async function runJudge(scope: string, effect: JudgeEffect): Promise<void> {
    const controller = new AbortController();
    const cancel = clock.at(clock.now() + policy.judgeTimeoutMs, () => controller.abort(new Error('attend: judge timed out')));
    const { batchId, attempt } = effect;
    let result: Input;
    try {
      const live = config.context ? await config.context(scope, controller.signal) : {};
      const input: JudgeInput = {
        scope,
        window: effect.window,
        routes: effect.offered.map((route) => {
          const criterion = config.criteria?.[route];
          return criterion === undefined ? { route } : { route, criterion };
        }),
        predicates: Object.entries({ ...config.predicates, ...live.predicates }).map(([name, question]) => ({ name, question })),
        facts: live.facts ?? {},
        recent: effect.recent,
        now: clock.now(),
      };
      if (config.guidance !== undefined) input.guidance = config.guidance;
      const judgment: unknown = await config.judge(input, controller.signal);
      result = isJudgment(judgment)
        ? { kind: 'judged', batchId, attempt, judgment }
        : { kind: 'judgeFailed', batchId, attempt, message: 'malformed judgment', permanent: false };
    } catch (error) {
      result = { kind: 'judgeFailed', batchId, attempt, message: messageOf(error), permanent: isPermanent(error) };
    } finally {
      cancel();
    }
    await apply(scope, result);
  }

  async function runDeliver(scope: string, effect: DeliverEffect): Promise<void> {
    const delivery = effect.delivery;
    const controller = new AbortController();
    const cancel = clock.at(delivery.leaseUntil, () => controller.abort(new Error('attend: delivery timed out')));
    let result: Input;
    try {
      if (delivery.kind === 'decision') await config.sink.deliver(delivery.decision, controller.signal);
      else await config.deadLetter(delivery.letter, controller.signal);
      result = { kind: 'delivered', deliveryId: delivery.id, attempt: delivery.attempt };
    } catch (error) {
      result = { kind: 'deliveryFailed', deliveryId: delivery.id, attempt: delivery.attempt, message: messageOf(error), permanent: isPermanent(error) };
    } finally {
      cancel();
    }
    await apply(scope, result);
  }

  function sleep(ms: number, signal: AbortSignal): Promise<void> {
    return new Promise((resolve) => {
      if (signal.aborted) {
        resolve();
        return;
      }
      const onAbort = () => {
        cancel();
        resolve();
      };
      const cancel = clock.at(clock.now() + ms, () => {
        signal.removeEventListener('abort', onAbort);
        resolve();
      });
      signal.addEventListener('abort', onAbort, { once: true });
    });
  }

  return {
    push: (scope, events) => apply(scope, { kind: 'events', events }),
    flush: (scope) => apply(scope, { kind: 'flush' }),
    inspect: (scope) => config.store.get(scope),

    async attach(scope, source, options) {
      const controller = new AbortController();
      const abort = () => controller.abort();
      options?.signal?.addEventListener('abort', abort, { once: true });
      sources.add(controller);
      try {
        let failures = 0;
        while (!controller.signal.aborted) {
          const cursor = (await config.store.get(scope))?.cursor ?? null;
          try {
            await source.subscribe(cursor, (batch) => apply(scope, { kind: 'events', ...batch }), controller.signal);
            return;
          } catch (error) {
            if (!(error instanceof OutboxFullError) || controller.signal.aborted) throw error;
            observe({ kind: 'backpressure', scope, outbox: error.size });
            await sleep(policy.retryBackoffMs[Math.min(failures, policy.retryBackoffMs.length - 1)] ?? policy.deliveryTimeoutMs, controller.signal);
            failures += 1;
          }
        }
      } finally {
        sources.delete(controller);
        options?.signal?.removeEventListener('abort', abort);
      }
    },

    async run({ signal, sweepMs = 1000, sweepLimit = 100 }) {
      if (running) throw new Error('attend: already running');
      running = { signal };
      try {
        while (!signal.aborted) {
          try {
            for (const scope of await config.store.due(clock.now(), sweepLimit)) tick(scope);
          } catch (error) {
            observe({ kind: 'failed', scope: '*', input: 'tick', message: messageOf(error) });
          }
          await sleep(sweepMs, signal);
        }
      } finally {
        running = null;
        for (const cancel of timers.values()) cancel();
        timers.clear();
        for (const controller of sources) controller.abort();
      }
    },
  };
}

function isJudgment(value: unknown): value is Judgment {
  if (typeof value !== 'object' || value === null || !('route' in value)) return false;
  if (!ROUTES.some((route) => route === value.route)) return false;
  const probabilities = 'probabilities' in value ? value.probabilities : undefined;
  const predicates = 'predicates' in value ? value.predicates : undefined;
  const optionalObject = (v: unknown) => v === undefined || (typeof v === 'object' && v !== null);
  return optionalObject(probabilities) && optionalObject(predicates);
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function assertNever(value: never): never {
  throw new Error(`attend: unexpected ${JSON.stringify(value)}`);
}

// ─── helpers that ship in the main export ────────────────────────────────────

export type RouteHandlers = {
  [R in DeliveredRoute]: (decision: Decision & { route: R }, signal: AbortSignal) => void | Promise<void>;
};

/**
 * A sink with one handler per delivered route. All four are required, so a route added to
 * the library is a compile error in every app that has not decided what it means for them.
 */
export function routes(handlers: RouteHandlers): Sink {
  return {
    async deliver(decision, signal) {
      switch (decision.route) {
        case 'interrupt':
          return handlers.interrupt({ ...decision, route: 'interrupt' }, signal);
        case 'queue':
          return handlers.queue({ ...decision, route: 'queue' }, signal);
        case 'note':
          return handlers.note({ ...decision, route: 'note' }, signal);
        case 'ask':
          return handlers.ask({ ...decision, route: 'ask' }, signal);
        default:
          return assertNever(decision.route);
      }
    },
  };
}

const ROUTE_HINT: Record<DeliveredRoute, string> = {
  interrupt: 'Attend to this now, even mid-task.',
  queue: 'Take this up when the current step is done.',
  note: 'Background only. No reply is needed.',
  ask: 'The user was offered a way to call you in. Stay quiet unless they do.',
};

/** The text an agent hears: the window's event lines oldest first, what was left out, and one line on the route. */
export function defaultNotice(decision: Decision, options?: { maxLines?: number }): string {
  const max = options?.maxLines ?? 20;
  const lines = decision.window.events.filter((e) => e.kind !== 'context').map((e) => e.text ?? `${e.name} ${JSON.stringify(e.data)}`);
  const shown = lines.slice(-max);
  const out: string[] = [];
  if (lines.length > shown.length) out.push(`(${lines.length - shown.length} earlier events left out)`);
  if (decision.window.gap) out.push('Some events before these were lost.');
  out.push(...shown);
  if (decision.window.dropped > 0) out.push(`(${decision.window.dropped} background events left out)`);
  out.push(ROUTE_HINT[decision.route]);
  return out.join('\n');
}

/**
 * A `Source` over an `EventLog`: reads after the cursor, emits, repeats. `blockMs` is passed
 * to the log; a log that cannot block returns at once, and `pollMs` then paces the loop.
 */
export function logSource(log: EventLog, scope: string, options?: { limit?: number; blockMs?: number; pollMs?: number }): Source {
  const limit = options?.limit ?? 100;
  const blockMs = options?.blockMs ?? 1000;
  const pollMs = options?.pollMs ?? 250;
  return {
    async subscribe(cursor, emit, signal) {
      let after = cursor;
      while (!signal.aborted) {
        const entries = await log.read(scope, after, { limit, blockMs });
        if (signal.aborted) return;
        const last = entries[entries.length - 1];
        if (last) {
          await emit({ events: entries.map((e) => e.event), cursor: last.position });
          after = last.position;
        } else {
          await new Promise<void>((resolve) => {
            const timer = setTimeout(() => {
              signal.removeEventListener('abort', onAbort);
              resolve();
            }, Math.max(0, pollMs));
            const onAbort = () => {
              clearTimeout(timer);
              resolve();
            };
            signal.addEventListener('abort', onAbort, { once: true });
          });
        }
      }
    },
  };
}

export function systemClock(): Clock {
  const MAX_DELAY = 2 ** 31 - 1;
  return {
    now: () => Date.now(),
    at(at, fn) {
      const timer = setTimeout(fn, Math.min(Math.max(0, at - Date.now()), MAX_DELAY));
      return () => clearTimeout(timer);
    },
  };
}
