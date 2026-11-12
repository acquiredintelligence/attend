import { ConflictError, IntakeError, isPermanent } from './errors.js';
import { foldEvents, type EventFold } from './fold.js';
import { sleep, systemClock } from './internal/clock.js';
import { arrayLength, assertNever, messageOf } from './internal/shared.js';
import { DEFAULT_POLICY, KINDS, ROUTES } from './types.js';
import type { Clock, Source, SourceBatch, DeadLetter, Decision, DeliveredRoute, DeliverEffect, Effect, Entry, Event, EventLog, EventsInput, Input, JudgeEffect, JudgeImage, JudgeInput, Judgment, Kind, ReleasedInput, SettlePolicy, Sink, Trace } from './types.js';
import type { Attend, AttendConfig, EventInput } from './shell/types.js';
import { memoryStore } from './store/memory.js';
import { createScopes } from './shell/scope.js';
import { createScheduler } from './shell/scheduler.js';
import { copyDecision, copyEventSized, copyLetter, copyWindowSliced } from './shell/copy.js';
import { windowBytesFor } from './shell/budget.js';
import { readRecord } from './shell/read.js';
import { readDecisions } from './shell/decisions.js';
import { thresholdRoute } from './core/threshold.js';
import { IntakeQueue, registerIntakes } from './shell/intake.js';

/** Errors raised by intake and effect handlers. */
export { BackpressureError, ConflictError, OutboxFullError, PermanentError } from './errors.js';
/** Public configuration and runner contracts. */
export type { AdmissionOptions, Attend, AttendConfig, DeadLetterHandler, DecisionCursorPage, DecisionFilter, DecisionReadOptions, EventInput, JudgeContext, RunOptions } from './shell/types.js';

/** Creates a scope-owning shell over a log and snapshot store. */
export function createAttend(config: AttendConfig): Attend {
  return createAttendInternal(config);
}

/** Internal snapshot kill switch for recovery and differential verification. */
export function createAttendInternal(config: AttendConfig, snapshot: 'ratio' | 'always' = 'ratio'): Attend {
  const share = config.windowShare ?? 0.5;
  if (!Number.isFinite(share) || share <= 0 || share > 1) throw new RangeError('attend: windowShare must be in (0, 1]');
  if (config.actThreshold !== undefined && (!Number.isFinite(config.actThreshold) || config.actThreshold <= 0 || config.actThreshold > 1)) {
    throw new RangeError('attend: actThreshold must be in (0, 1]');
  }
  if (config.judgeConcurrency !== undefined && (!Number.isInteger(config.judgeConcurrency) || config.judgeConcurrency <= 0)) {
    throw new RangeError('attend: judgeConcurrency must be a positive integer');
  }
  const policy: SettlePolicy = { ...DEFAULT_POLICY, ...config.settle };
  if (policy.continuation !== undefined && policy.continuation !== 'separate' && policy.continuation !== 'accumulate') {
    throw new TypeError('attend: continuation must be separate or accumulate');
  }
  if (config.judge.budget) {
    const bytes = windowBytesFor(config.judge.budget, share);
    if (!Number.isFinite(bytes) || bytes <= 0) throw new RangeError('attend: judge window budget must be positive and finite');
    if (config.settle?.maxWindowBytes === undefined) policy.maxWindowBytes = bytes;
  }
  const clock = config.clock ?? systemClock();
  const store = config.store ?? memoryStore();
  const kindOf = kindFunction(config.kind);
  const sink: Sink | 'log' = typeof config.sink === 'function' ? functionSink(config.sink) : config.sink;
  const deadLetter = config.deadLetter ?? 'log';
  const fill = { id: () => globalThis.crypto.randomUUID(), at: () => clock.now() };
  const modes = { decisions: sink === 'log' ? 'log' : 'lane', deadLetters: deadLetter === 'log' ? 'log' : 'lane' } as const;
  const sources = new Set<AbortController>();
  const attached = new Map<string, Set<string>>();
  const intakes = new Map<string, IntakeQueue>();
  const intakeIdentity = {};
  const sizedEvents = new WeakMap<Event, number>();
  const calls = new Map<Promise<void>, number>();
  const releases = new Set<Promise<void>>();
  let running: { signal: AbortSignal } | null = null;
  let stopper: AbortController | null = null;
  let finished: Promise<void> | null = null;
  let judges = 0;
  const scheduler = createScheduler({ clock, onDue(scope) {
    const due = scopes.due(scope);
    if (due === null) return;
    if (due > clock.now()) scheduler.arm(scope, due);
    else tick(scope);
  } });
  const scopes = createScopes({ store, clock, config: { policy, ...modes, traces: config.observe !== undefined },
    admission: config.admission, idleMs: config.cache?.idleMs ?? 300000, snapshot, scheduler,
    eventBytes: (event) => sizedEvents.get(event), running: () => running !== null, execute, observe });
  const submit = scopes.submit;

  function observe(trace: Trace): void {
    try { config.observe?.(trace.kind === 'deadLettered' || trace.kind === 'dropped' ? structuredClone(trace) : trace); }
    catch {}
  }

  function classify(input: EventInput | readonly EventInput[]): EventsInput['events'] {
    if (typeof input !== 'object' || input === null) throw new TypeError('attend: events must be an event or an array of events');
    const events: readonly EventInput[] = Array.isArray(input) ? input : [input as EventInput];
    const length = arrayLength(events.length);
    const classified: EventsInput['events'] = new Array(length);
    for (let i = 0; i < length; i += 1) {
      const { event, bytes } = copyEventSized(events[i]! as Event, fill);
      if (event.observedAt !== undefined && event.observedAt > clock.now()) throw new TypeError('attend: observedAt must not be in the future');
      const kind = kindOf(event);
      if (!KINDS.includes(kind)) throw new TypeError('attend: invalid event kind');
      if (event.quietForMs !== undefined && kind !== 'continuous') throw new TypeError('attend: quietForMs requires a continuous event');
      const entry = { ...event, kind };
      sizedEvents.set(entry, bytes);
      classified[i] = entry;
    }
    return classified;
  }

  function tick(scope: string): void {
    const intake = intakes.get(scope);
    if (intake) { intake.wake(); return; }
    void background(scope, 'tick', () => submit(scope, { kind: 'tick' }));
  }

  function background(scope: string, input: Input['kind'], work: () => Promise<void>): Promise<void> {
    const intake = intakes.get(scope);
    return (intake ? intake.track(work) : work()).catch((error: unknown) => {
      if (!(error instanceof ConflictError)) observe({ kind: 'failed', scope, input, message: messageOf(error) });
    });
  }

  function call(scope: string, result: Input['kind'], leaseUntil: number, work: () => Promise<void>, release: ReleasedInput): void {
    if (!running || intakes.get(scope)?.closed) {
      const released = background(scope, release.kind, () => submit(scope, release)).finally(() => releases.delete(released));
      releases.add(released);
      return;
    }
    const done = background(scope, result, work).finally(() => calls.delete(done));
    calls.set(done, leaseUntil);
  }

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
        if (running && !intakes.get(scope)?.closed && judges >= (config.judgeConcurrency ?? Infinity)) {
          void background(scope, 'judgeDeferred', () => submit(scope, { kind: 'judgeDeferred', batchId: effect.batchId, attempt: effect.attempt, until: clock.now() + 100 }));
          return;
        }
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

  function effectController(scope: string, at: number, message: string) {
    const controller = new AbortController();
    const cancelTimer = clock.at(at, () => controller.abort(new Error(message)));
    const signal = intakes.get(scope)?.completionSignal;
    const abort = () => controller.abort(signal?.reason);
    signal?.addEventListener('abort', abort, { once: true });
    if (signal?.aborted) abort();
    return { controller, cancel() { cancelTimer(); signal?.removeEventListener('abort', abort); } };
  }

  function submitCompletion(scope: string, result: Extract<Input, { kind: 'judged' | 'judgeFailed' | 'delivered' | 'deliveryFailed' }>): Promise<void> {
    const intake = intakes.get(scope);
    if (!intake) return submit(scope, result);
    const release: ReleasedInput = result.kind === 'judged' || result.kind === 'judgeFailed'
      ? { kind: 'judgeReleased', batchId: result.batchId, attempt: result.attempt }
      : { kind: 'deliveryReleased', deliveryId: result.deliveryId, attempt: result.attempt };
    if (intake.closed) return submit(scope, release);
    const signal = intake.completionSignal;
    return submit(scope, result, false, signal).catch((error: unknown) => {
      if (signal.aborted && error === signal.reason) return submit(scope, release);
      throw error;
    });
  }

  async function runJudge(scope: string, effect: JudgeEffect): Promise<void> {
    judges += 1;
    const { controller, cancel } = effectController(scope, clock.now() + policy.judgeTimeoutMs, 'attend: judge timed out');
    const { batchId, attempt } = effect;
    let result: Extract<Input, { kind: 'judged' | 'judgeFailed' }>;
    try {
      if (intakes.has(scope)) controller.signal.throwIfAborted();
      const live = config.context ? await config.context(scope, controller.signal) : {};
      const window = copyWindowSliced(effect.window);
      const input: JudgeInput = {
        scope,
        window: window instanceof Promise ? await window : window,
        routes: effect.offered.map((route) => {
          const criterion = config.criteria?.[route];
          return criterion === undefined ? { route } : { route, criterion };
        }),
        predicates: Object.entries({ ...config.predicates, ...live.predicates }).map(([name, question]) => ({ name, question })),
        facts: live.facts ?? {},
        recent: structuredClone(effect.recent),
        now: clock.now(),
      };
      if (config.guidance !== undefined) input.guidance = config.guidance;
      if (intakes.has(scope)) controller.signal.throwIfAborted();
      const images = config.capture ? await capture(config.capture, scope, batchId, controller.signal) : [];
      if (images.length > 0) input.images = images;
      if (intakes.has(scope)) controller.signal.throwIfAborted();
      const judgment: unknown = await config.judge(input, controller.signal);
      if (intakes.has(scope)) controller.signal.throwIfAborted();
      if (isJudgment(judgment)) {
        const threshold = config.actThreshold === undefined ? null : thresholdRoute(judgment, effect.offered, config.actThreshold);
        if (threshold) observe({ kind: 'routeThresholded', scope, batchId, from: judgment.route, to: threshold.route, act: threshold.act });
        result = { kind: 'judged', batchId, attempt, judgment: structuredClone(threshold ? { ...judgment, route: threshold.route } : judgment) };
      } else result = { kind: 'judgeFailed', batchId, attempt, message: 'malformed judgment', permanent: false };
    } catch (error) {
      result = { kind: 'judgeFailed', batchId, attempt, message: messageOf(error), permanent: isPermanent(error) };
    } finally {
      cancel();
    }
    judges -= 1;
    await submitCompletion(scope, result);
  }

  async function capture(fn: NonNullable<AttendConfig['capture']>, scope: string, batchId: string, judgeSignal: AbortSignal): Promise<JudgeImage[]> {
    const controller = new AbortController();
    const onJudgeAbort = () => controller.abort(judgeSignal.reason);
    judgeSignal.addEventListener('abort', onJudgeAbort, { once: true });
    const cancel = clock.at(clock.now() + (config.captureTimeoutMs ?? 500), () => controller.abort(new Error('attend: capture timed out')));
    const timedOut = new Promise<never>((_, reject) => controller.signal.addEventListener('abort', () => reject(controller.signal.reason), { once: true }));
    try {
      const result = await Promise.race([fn(scope, controller.signal), timedOut]);
      return result === null ? [] : Array.isArray(result) ? result : [result];
    } catch (error) {
      observe({ kind: 'captureFailed', scope, batchId, message: messageOf(error) });
      return [];
    } finally {
      cancel();
      judgeSignal.removeEventListener('abort', onJudgeAbort);

      timedOut.catch(() => undefined);
    }
  }

  async function runDeliver(scope: string, effect: DeliverEffect): Promise<void> {
    const { item, phase } = effect.delivery;
    const { controller, cancel } = effectController(scope, phase.leaseUntil, 'attend: delivery timed out');
    let result: Extract<Input, { kind: 'delivered' | 'deliveryFailed' }>;
    try {
      if (intakes.has(scope)) controller.signal.throwIfAborted();
      if (item.kind === 'decision' && sink !== 'log') {
        const decision = copyDecision(item.decision);
        const copied = decision instanceof Promise ? await decision : decision;
        if (intakes.has(scope)) controller.signal.throwIfAborted();
        await sink.deliver(copied, controller.signal);
      } else if (item.kind === 'deadLetter' && deadLetter !== 'log') {
        const letter = copyLetter(item.letter);
        const copied = letter instanceof Promise ? await letter : letter;
        if (intakes.has(scope)) controller.signal.throwIfAborted();
        await deadLetter(copied, controller.signal);
      }
      result = { kind: 'delivered', deliveryId: item.id, attempt: phase.attempt };
    } catch (error) {
      result = { kind: 'deliveryFailed', deliveryId: item.id, attempt: phase.attempt, message: messageOf(error), permanent: isPermanent(error) };
    } finally {
      cancel();
    }
    await submitCompletion(scope, result);
  }

  async function sweep(now: number, limit: number, warm: boolean): Promise<void> {
    try {
      let after: import('./store/types.js').DueKey | undefined;
      while (running) {
        const page = await store.due(now, limit, after);
        for (const key of page) {
          if (!running) break;
          const due = await scopes.touch(key.scope);
          if (!warm || (due !== null && due <= clock.now())) tick(key.scope);
        }
        if (page.length < limit || page.length === 0) break;
        after = page.at(-1);
      }
    } catch (error) { observe({ kind: 'failed', scope: '*', input: 'tick', message: messageOf(error) }); }
  }

  const attend: Attend = {
    push(scope, events) {
      try {
        if (intakes.has(scope)) throw new IntakeError('attend: scope has exclusive intake');
        return submit(scope, { kind: 'events', events: classify(events) }, true);
      }
      catch (error) { return Promise.reject(error); }
    },
    flush: (scope) => intakes.has(scope) ? Promise.reject(new IntakeError('attend: scope has exclusive intake')) : submit(scope, { kind: 'flush' }, true),
    end: (scope) => intakes.has(scope) ? Promise.reject(new IntakeError('attend: scope has exclusive intake')) : submit(scope, { kind: 'end' }, true),
    inspect: (scope) => readRecord(store, scope, modes),
    decisions: (filter, options) => readDecisions(store.log, clock, filter, options),

    async attach(scope, source, options) {
      if (intakes.has(scope)) throw new IntakeError('attend: scope has exclusive intake');
      const keys = attached.get(scope) ?? new Set<string>();
      if (keys.has(source.key)) throw new Error(`attend: source "${source.key}" is already attached to scope "${scope}"`);
      keys.add(source.key);
      attached.set(scope, keys);
      const controller = new AbortController();
      const abort = () => controller.abort();
      options?.signal?.addEventListener('abort', abort, { once: true });
      if (options?.signal?.aborted) abort();
      sources.add(controller);
      let failures = 0;
      try {
        while (!controller.signal.aborted) {
          const subscription = new AbortController();
          const signal = AbortSignal.any([controller.signal, subscription.signal]);
          let pending: Promise<void> | undefined;
          const write = async (batch: SourceBatch) => {
            try {
              signal.throwIfAborted();
              await submit(scope, { kind: 'events', ...batch, source: source.key, events: classify(batch.events) }, true, signal, (error) => subscription.abort(error));
              if (!signal.aborted) failures = 0;
            } catch (error) {
              subscription.abort(error);
              throw error;
            }
          };
          const emit = (batch: SourceBatch) => {
            pending = write(batch);

            pending.catch(() => undefined);
            return pending;
          };
          try {
            const cursor = (await readRecord(store, scope, modes))?.cursors[source.key] ?? null;
            controller.signal.throwIfAborted();
            await source.subscribe(cursor, emit, signal);
            await pending;
            signal.throwIfAborted();
            return;
          } catch (error) {
            if (controller.signal.aborted) return;
            const failure = subscription.signal.aborted ? subscription.signal.reason : error;
            subscription.abort(failure);
            if (isPermanent(failure)) throw failure;
            failures += 1;
            const delay = policy.retryBackoffMs[Math.min(failures, policy.retryBackoffMs.length) - 1] ?? policy.deliveryTimeoutMs;
            observe({ kind: 'retrying', scope, stage: 'source', id: source.key, attempt: failures, at: clock.now() + delay, message: messageOf(failure) });
            await sleep(clock, delay, controller.signal);
          } finally {
            subscription.abort();
          }
        }
      } finally {
        keys.delete(source.key);
        if (keys.size === 0) attached.delete(scope);
        sources.delete(controller);
        options?.signal?.removeEventListener('abort', abort);
      }
    },

    run(options = {}) {
      if (running) return Promise.reject(new Error('attend: already running'));
      const own = new AbortController();
      stopper = own;
      const done = runLoop(options.signal ? AbortSignal.any([options.signal, own.signal]) : own.signal, options.sweepMs ?? 1000, options.sweepLimit ?? 100);
      finished = done.then(() => undefined, () => undefined);
      return done;
    },

    async stop() {
      stopper?.abort();
      await finished;
    },
  };

  registerIntakes(attend, (scope, options, context) => {
    const capacity = options?.capacity ?? 20000;
    if (!Number.isSafeInteger(capacity) || capacity <= 0) throw new RangeError('attend: intake capacity must be a positive safe integer');
    if (intakes.has(scope) || scopes.has(scope) || attached.has(scope)) throw new IntakeError('attend: scope already active');
    const intake = new IntakeQueue(scope, { identity: intakeIdentity, scopes, clock, classify, running: () => running !== null, observe }, context, capacity);
    intakes.set(scope, intake);
    return intake;
  });
  return attend;

  async function runLoop(signal: AbortSignal, sweepMs: number, sweepLimit: number): Promise<void> {
    running = { signal };
    const onAbort = () => {
      for (const intake of intakes.values()) void intake.fence();
      running = null; scheduler.stop(); for (const source of sources) source.abort();
    };
    signal.addEventListener('abort', onAbort, { once: true });
    try {
      if (signal.aborted) return;
      scopes.armAll();
      await sweep(Number.MAX_SAFE_INTEGER, sweepLimit, true);
      while (!signal.aborted) {
        await sweep(clock.now(), sweepLimit, false);
        scopes.evict();
        await sleep(clock, sweepMs, signal);
      }
    } finally {
      onAbort();
      signal.removeEventListener('abort', onAbort);
      const writes = scopes.pending();
      await Promise.all([...calls].map(([done, leaseUntil]) => settledOrExpired(done, leaseUntil)));
      await writes;
      while (releases.size > 0) await Promise.all(releases);
      await Promise.allSettled([...intakes.values()].map((intake) => intake.fence()));
    }
  }
}

function kindFunction(kind: AttendConfig['kind']): (event: Event) => Kind {
  if (typeof kind === 'function') return kind;
  if (typeof kind !== 'object' || kind === null || Array.isArray(kind)) throw new TypeError('attend: kind must be a function or a map of event name to kind');
  const map = new Map<string, Kind>();
  for (const [name, value] of Object.entries(kind)) {
    if (!KINDS.includes(value)) throw new RangeError(`attend: the kind for "${name}" must be one of ${KINDS.join(', ')}`);
    map.set(name, value);
  }
  return (event) => map.get(event.name) ?? 'consequence';
}

function functionSink(deliver: (decision: Decision, signal: AbortSignal) => void | Promise<void>): Sink {
  return { async deliver(decision, signal) { await deliver(decision, signal); } };
}

function isJudgment(value: unknown): value is Judgment {
  if (typeof value !== 'object' || value === null || !('route' in value)) return false;
  if (!ROUTES.some((route) => route === value.route)) return false;
  const probabilities = 'probabilities' in value ? value.probabilities : undefined;
  const predicates = 'predicates' in value ? value.predicates : undefined;
  const optionalObject = (v: unknown) => v === undefined || (typeof v === 'object' && v !== null);
  return optionalObject(probabilities) && optionalObject(predicates);
}

/** One delivery handler per delivered route. */
export type RouteHandlers = {
  [R in DeliveredRoute]: (decision: Decision & { route: R }, signal: AbortSignal) => void | Promise<void>;
};

/** A sink with a required handler for each delivered route. */
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

/**
 * The text an agent hears: the window's event lines oldest first, what was left out, and one line on the route.
 *
 * A run of events that read the same becomes one line with its count. A run of `continuous` events under one name is the
 * steps of one act, so it becomes its first and last step with the count. `fold: false` gives one line per event.
 */
export function defaultNotice(decision: Decision, options?: { maxLines?: number; fold?: boolean }): string {
  const max = options?.maxLines ?? 20;
  const lines = noticeLines(decision.window.events, options?.fold ?? true);
  const shown = lines.slice(-max);
  const older = lines.slice(0, lines.length - shown.length).reduce((sum, line) => sum + line.events, 0);
  const out: string[] = [];
  if (older > 0) out.push(`(${older} older events not shown)`);
  if (decision.window.gap) out.push('Some events before these were lost.');
  out.push(...shown.map((line) => line.text));
  if (decision.window.dropped > 0) out.push(`(${decision.window.dropped} background events dropped from this window)`);
  out.push(ROUTE_HINT[decision.route]);
  return out.join('\n');
}

/** The notice's event lines, each with how many events it stands for. Context entries are left out. */
function noticeLines(entries: readonly Entry[], fold: boolean): { text: string; events: number }[] {
  return foldEvents(entries.filter((entry) => entry.kind !== 'context'), {
    repeats: (first, next) => eventLine(first) === eventLine(next),
    continues: (first, next) => first.kind === 'continuous' && next.kind === 'continuous' && first.name === next.name,
    fold,
  }).map((run) => ({ text: runLine(run), events: run.count }));
}

const eventLine = (e: Entry): string => e.text ?? `${e.name} ${JSON.stringify(e.data)}`;

function runLine({ first, last, count, mode }: EventFold<Entry>): string {
  const line = eventLine(first);
  if (mode === 'single') return line;
  if (mode === 'repeat') return `${line} (${count} times)`;
  const end = first.text === undefined && last.text === undefined ? JSON.stringify(last.data) : eventLine(last);
  return `${line} → ${end} (${count} steps)`;
}

/** A resumable source over an event log, with optional polling and blocking waits. */
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
