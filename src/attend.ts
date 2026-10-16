/**
 * The shell. Thin: load, step, compare-and-swap, arm a timer from `dueAt`, execute
 * effects, feed results back as inputs. All policy is in ./step.ts.
 */
import { initialRecord, step, type StepConfig } from './step.js';
import { ConflictError, isPermanent, OutboxFullError } from './errors.js';
import { sleep, systemClock } from './internal/clock.js';
import { assertNever, messageOf } from './internal/shared.js';
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
  Kind,
  ReleasedInput,
  Route,
  ScopeRecord,
  SettlePolicy,
  Sink,
  Source,
  SourceBatch,
  StateStore,
  Trace,
} from './types.js';
import { DEFAULT_POLICY, ROUTES } from './types.js';

export { ConflictError, OutboxFullError, PermanentError } from './errors.js';

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
  kind: (event: Event) => Kind;
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
  /**
   * Aborting stops the sweep, the timers and the attached sources and starts no further judge,
   * sink or dead-letter call: one the reducer asks for after that is released at once, with no
   * failure counted, so the next runner takes it up on its first tick. `run` then resolves once
   * the calls already in flight have settled and persisted their results, or their leases have
   * expired, and once every write this instance had in flight has landed, so the store may be
   * closed as soon as it resolves. A call that outlived its lease is taken up by the next
   * runner when that lease expires.
   */
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
   * replica; idempotent on event ids. Rejects with `OutboxFullError` when either outbox lane
   * is at its cap, and with `ConflictError` after eight lost compare-and-swaps.
   */
  push(scope: string, events: Event[]): Promise<void>;
  /**
   * Run a source into a scope from that source's committed cursor until `signal` or the run
   * aborts. Any error from the source, `OutboxFullError` included, backs off through
   * `retryBackoffMs` (the last rung repeated) and resubscribes from the committed cursor; a
   * `PermanentError` rejects. A normal return from `subscribe` resolves. Throws if the same
   * `Source.key` is already attached to the scope in this process.
   */
  attach(scope: string, source: Source, options?: { signal?: AbortSignal }): Promise<void>;
  /**
   * Settle the open window at the runner's next tick, reason `forced`. A window already in
   * front of the judge is forced too: a `wait` verdict is judged again at once with `wait`
   * withheld. Persists the request and resolves at once, like `push`; rejects with
   * `ConflictError` after eight lost compare-and-swaps.
   */
  flush(scope: string): Promise<void>;
  /** The record as stored, for operators and tests. */
  inspect(scope: string): Promise<ScopeRecord | null>;
  /** The runner: in-process timers for scopes touched here, the sweep for everything. Resolves after an abort, once the calls in flight have settled. */
  run(options: RunOptions): Promise<void>;
}

const MAX_CAS_ATTEMPTS = 8;

/** A few milliseconds, growing with each lost compare-and-swap and jittered so two losers do not collide again. */
function conflictBackoffMs(attempt: number): number {
  return 2 * attempt + Math.random() * 4;
}

export function createAttend(config: AttendConfig): Attend {
  const policy: SettlePolicy = { ...DEFAULT_POLICY, ...config.settle };
  const stepConfig: StepConfig = { kind: config.kind, policy };
  const clock = config.clock ?? systemClock();
  const lanes = new Map<string, Promise<void>>();
  const timers = new Map<string, () => void>();
  const sources = new Set<AbortController>();
  const attached = new Set<string>();
  /** Judge, sink and dead-letter calls in flight, each with the lease the reducer gave it. */
  const calls = new Map<Promise<void>, number>();
  /** Release writes for calls no live runner would start. A stopping runner waits for them, so none is left for a closed store. */
  const releases = new Set<Promise<void>>();
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
      if (input.kind === 'events' && input.events.length > 0) {
        if (current.outbox.decisions.length >= policy.maxOutbox) throw new OutboxFullError(scope, 'decisions', current.outbox.decisions.length);
        if (current.outbox.deadLetters.length >= policy.maxDeadLetters) throw new OutboxFullError(scope, 'deadLetters', current.outbox.deadLetters.length);
      }
      const { record, effects } = step(stepConfig, current, input, clock.now());
      const next = { ...record, version: current.version + 1 };
      if ((await config.store.put(next, current.version)) === 'conflict') {
        observe({ kind: 'conflict', scope, version: current.version });
        // Real time, not the injected clock: the contention is with another process, and a fake clock would never advance for it.
        await new Promise((resolve) => setTimeout(resolve, conflictBackoffMs(attempt)));
        continue;
      }
      arm(scope, next.dueAt);
      for (const effect of effects) execute(scope, effect);
      return;
    }
    observe({ kind: 'abandoned', scope, input: input.kind });
    throw new ConflictError(scope, MAX_CAS_ATTEMPTS);
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
    void background(scope, 'tick', () => apply(scope, { kind: 'tick' }));
  }

  function background(scope: string, input: Input['kind'], work: () => Promise<void>): Promise<void> {
    return work().catch((error: unknown) => {
      if (!(error instanceof ConflictError)) observe({ kind: 'failed', scope, input, message: messageOf(error) });
    });
  }

  /**
   * Only a live runner starts a call. One it will not start is released instead, so the next
   * runner takes it up on its first tick rather than when the lease expires.
   */
  function call(scope: string, result: Input['kind'], leaseUntil: number, work: () => Promise<void>, release: ReleasedInput): void {
    if (!running) {
      const released = background(scope, release.kind, () => apply(scope, release)).finally(() => releases.delete(released));
      releases.add(released);
      return;
    }
    const done = background(scope, result, work).finally(() => calls.delete(done));
    calls.set(done, leaseUntil);
  }

  /** Resolves when the call settles or its lease expires, whichever comes first. */
  function settledOrExpired(done: Promise<void>, leaseUntil: number): Promise<void> {
    if (leaseUntil <= clock.now()) return Promise.resolve();
    return new Promise((resolve) => {
      const cancel = clock.at(leaseUntil, resolve);
      void done.then(() => {
        cancel();
        resolve();
      });
    });
  }

  function execute(scope: string, effect: Effect): void {
    switch (effect.kind) {
      case 'trace':
        observe(effect.trace);
        return;
      case 'judge':
        call(scope, 'judged', effect.leaseUntil, () => runJudge(scope, effect), { kind: 'judgeReleased', batchId: effect.batchId, attempt: effect.attempt });
        return;
      case 'deliver':
        call(scope, 'delivered', effect.delivery.phase.leaseUntil, () => runDeliver(scope, effect), {
          kind: 'deliveryReleased',
          deliveryId: effect.delivery.item.id,
          attempt: effect.delivery.phase.attempt,
        });
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
    const { item, phase } = effect.delivery;
    const controller = new AbortController();
    const cancel = clock.at(phase.leaseUntil, () => controller.abort(new Error('attend: delivery timed out')));
    let result: Input;
    try {
      if (item.kind === 'decision') await config.sink.deliver(item.decision, controller.signal);
      else await config.deadLetter(item.letter, controller.signal);
      result = { kind: 'delivered', deliveryId: item.id, attempt: phase.attempt };
    } catch (error) {
      result = { kind: 'deliveryFailed', deliveryId: item.id, attempt: phase.attempt, message: messageOf(error), permanent: isPermanent(error) };
    } finally {
      cancel();
    }
    await apply(scope, result);
  }

  /** After a start, arm timers for scopes not yet due, so recovery is on time rather than at the next sweep. */
  async function warm(limit: number): Promise<void> {
    try {
      const dueNow = new Set(await config.store.due(clock.now(), limit));
      for (const scope of await config.store.due(Number.MAX_SAFE_INTEGER, limit)) {
        if (dueNow.has(scope)) continue;
        const record = await config.store.get(scope);
        if (record) arm(scope, record.dueAt);
      }
    } catch (error) {
      observe({ kind: 'failed', scope: '*', input: 'tick', message: messageOf(error) });
    }
  }

  return {
    push: (scope, events) => apply(scope, { kind: 'events', events }),
    flush: (scope) => apply(scope, { kind: 'flush' }),
    inspect: (scope) => config.store.get(scope),

    async attach(scope, source, options) {
      const slot = `${scope}\n${source.key}`;
      if (attached.has(slot)) throw new Error(`attend: source "${source.key}" is already attached to scope "${scope}"`);
      attached.add(slot);
      const controller = new AbortController();
      const abort = () => controller.abort();
      options?.signal?.addEventListener('abort', abort, { once: true });
      if (options?.signal?.aborted) abort();
      sources.add(controller);
      let failures = 0;
      const emit = async (batch: SourceBatch) => {
        await apply(scope, { kind: 'events', source: source.key, ...batch });
        failures = 0;
      };
      try {
        while (!controller.signal.aborted) {
          try {
            const cursor = (await config.store.get(scope))?.cursors[source.key] ?? null;
            await source.subscribe(cursor, emit, controller.signal);
            return;
          } catch (error) {
            if (controller.signal.aborted) return;
            if (isPermanent(error)) throw error;
            failures += 1;
            const delay = policy.retryBackoffMs[Math.min(failures, policy.retryBackoffMs.length) - 1] ?? policy.deliveryTimeoutMs;
            observe({ kind: 'retrying', scope, stage: 'source', id: source.key, attempt: failures, at: clock.now() + delay, message: messageOf(error) });
            await sleep(clock, delay, controller.signal);
          }
        }
      } finally {
        attached.delete(slot);
        sources.delete(controller);
        options?.signal?.removeEventListener('abort', abort);
      }
    },

    async run({ signal, sweepMs = 1000, sweepLimit = 100 }) {
      if (running) throw new Error('attend: already running');
      running = { signal };
      try {
        await warm(sweepLimit);
        while (!signal.aborted) {
          try {
            for (const scope of await config.store.due(clock.now(), sweepLimit)) tick(scope);
          } catch (error) {
            observe({ kind: 'failed', scope: '*', input: 'tick', message: messageOf(error) });
          }
          await sleep(clock, sweepMs, signal);
        }
      } finally {
        running = null;
        for (const cancel of timers.values()) cancel();
        timers.clear();
        for (const controller of sources) controller.abort();
        // Only the writes in flight now, not a producer's later pushes: those persist on their own and do not hold the runner.
        const writes = [...lanes.values()];
        await Promise.all([...calls].map(([done, leaseUntil]) => settledOrExpired(done, leaseUntil)));
        await Promise.all(writes);
        while (releases.size > 0) await Promise.all(releases);
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
  if (lines.length > shown.length) out.push(`(${lines.length - shown.length} older events not shown)`);
  if (decision.window.gap) out.push('Some events before these were lost.');
  out.push(...shown);
  if (decision.window.dropped > 0) out.push(`(${decision.window.dropped} background events dropped from this window)`);
  out.push(ROUTE_HINT[decision.route]);
  return out.join('\n');
}

/**
 * A `Source` over an `EventLog`, keyed `log:<scope>`: reads after the cursor, emits, repeats.
 * `blockMs` is passed to the log; a log that cannot block returns at once, and `pollMs` then
 * paces the loop on `clock`.
 */
export function logSource(log: EventLog, scope: string, options?: { limit?: number; blockMs?: number; pollMs?: number; clock?: Clock }): Source {
  const limit = options?.limit ?? 100;
  const blockMs = options?.blockMs ?? 1000;
  const pollMs = options?.pollMs ?? 250;
  const clock = options?.clock ?? systemClock();
  return {
    key: `log:${scope}`,
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
          await sleep(clock, pollMs, signal);
        }
      }
    },
  };
}
