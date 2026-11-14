import type { Clock, Event, EventsInput, SourceBatch, Trace } from '../core/types.js';
import { BackpressureError, IntakeError } from '../errors.js';
import { messageOf } from '../internal/shared.js';
import type { createScopes } from './scope.js';
import type { Attend, Intake, IntakeOptions, Prepare } from './types.js';

export interface IntakeOwner { readonly engine: object; readonly scope: string }
export interface IntakeContext {
  getStore(): IntakeOwner | undefined;
  run<T>(owner: IntakeOwner, callback: () => T): T;
}

type Factory = (scope: string, options: IntakeOptions | undefined, context: IntakeContext) => Intake;
const factories = new WeakMap<Attend, Factory>();

/** The optional Node entry uses this capability without adding Node imports to the core. */
export function registerIntakes(attend: Attend, factory: Factory): void { factories.set(attend, factory); }

export function openRegisteredIntake(attend: Attend, scope: string, options: IntakeOptions | undefined, context: IntakeContext): Intake {
  const factory = factories.get(attend);
  if (!factory) throw new IntakeError('attend: intake requires an engine from this package instance');
  return factory(scope, options, context);
}

interface Receipt {
  promise: Promise<void>;
  resolve(): void;
  reject(error: unknown): void;
}

function receipt(): Receipt {
  // The native Promise constructor calls its executor before returning.
  let resolve!: () => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<void>((yes, no) => { resolve = yes; reject = no; });
  void promise.catch(() => undefined);
  return { promise, resolve, reject };
}

interface Operation {
  prepare: Prepare;
  source: string | undefined;
  controller: AbortController;
  done: Receipt;
  unlink(): void;
  releaseCapacity(): void;
}

interface Activity {
  operation: Operation | null;
  finished: Receipt;
  failure: { error: unknown } | null;
}

interface Runtime {
  identity: object;
  scopes: ReturnType<typeof createScopes>;
  clock: Clock;
  classify(events: readonly Event[]): EventsInput['events'];
  running(): boolean;
  observe(trace: Trace): void;
}

/** Owns local raw ordering; reducer records remain the only window state. */
export class IntakeQueue implements Intake {
  private readonly owner: IntakeOwner;
  private readonly queue = new Set<Operation>();
  private readonly preparations = new Set<Promise<void>>();
  private active: Activity | null = null;
  private timerPending = false;
  private work = 0;
  private workDone: Receipt | null = null;
  private closing: Receipt | null = null;
  private readonly completion = new AbortController();

  constructor(readonly scope: string, private readonly runtime: Runtime, private readonly context: IntakeContext, private readonly capacity: number) {
    this.owner = { engine: runtime.identity, scope };
  }

  get closed(): boolean { return this.closing !== null; }
  get completionSignal(): AbortSignal { return this.completion.signal; }

  ownsCurrentCallback(): boolean { return this.context.getStore()?.engine === this.owner.engine; }

  private checkContext(): void {
    const active = this.context.getStore();
    if (active?.engine === this.owner.engine && active.scope === this.scope) throw new IntakeError('attend: cannot await this intake from its own callback');
  }

  submit(prepare: Prepare, options?: { source?: string; signal?: AbortSignal }): Promise<void> {
    this.checkContext();
    if (this.closed) throw new IntakeError('attend: intake closed');
    if (!this.runtime.running()) throw new IntakeError('attend: intake requires a running engine');
    const count = this.queue.size + (this.active?.operation ? 1 : 0);
    if (count >= this.capacity) throw new BackpressureError(100, count);
    options?.signal?.throwIfAborted();
    const releaseCapacity = this.runtime.scopes.reserveInput();
    const controller = new AbortController();
    const abort = () => controller.abort(options?.signal?.reason);
    const operation: Operation = {
      prepare, source: options?.source, controller, done: receipt(), releaseCapacity,
      unlink: () => options?.signal?.removeEventListener('abort', abort),
    };
    this.queue.add(operation);
    controller.signal.addEventListener('abort', () => {
      if (this.queue.delete(operation)) {
        operation.unlink(); operation.releaseCapacity(); operation.done.reject(controller.signal.reason);
      }
    }, { once: true });
    options?.signal?.addEventListener('abort', abort, { once: true });
    if (options?.signal?.aborted) abort();
    this.pump();
    return operation.done.promise;
  }

  wake(): void {
    if (!this.closed) { this.timerPending = true; this.pump(); }
  }

  private pump(): void {
    if (this.closed || this.active !== null) return;
    const next = this.queue.values().next();
    const operation = next.done ? null : next.value;
    if (operation) this.queue.delete(operation);
    else {
      if (!this.timerPending) return;
      this.timerPending = false;
      const due = this.runtime.scopes.due(this.scope);
      if (due === null || due > this.runtime.clock.now()) return;
    }
    const activity: Activity = { operation, finished: receipt(), failure: null };
    this.active = activity;
    queueMicrotask(() => { void this.context.run(this.owner, () => this.perform(activity)); });
  }

  private prepare(operation: Operation): Promise<SourceBatch> {
    const signal = operation.controller.signal;
    signal.throwIfAborted();
    return new Promise((resolve, reject) => {
      const abort = () => { signal.removeEventListener('abort', abort); reject(signal.reason); };
      const success = (batch: SourceBatch) => { signal.removeEventListener('abort', abort); resolve(batch); };
      const failure = (error: unknown) => { signal.removeEventListener('abort', abort); reject(error); };
      signal.addEventListener('abort', abort, { once: true });
      try {
        const preparation = operation.prepare(signal).then(success, failure);
        this.preparations.add(preparation);
        void preparation.then(() => { this.preparations.delete(preparation); });
      }
      catch (error) { failure(error); }
    });
  }

  private async settle(activity: Activity): Promise<void> {
    try { await this.runtime.scopes.submit(this.scope, { kind: 'tick' }); }
    catch (error) { activity.failure = { error }; throw error; }
    await this.workDone?.promise;
    if (activity.failure) throw activity.failure.error;
  }

  private async perform(activity: Activity): Promise<void> {
    const operation = activity.operation;
    try {
      if (!operation) { await this.settle(activity); return; }
      const signal = operation.controller.signal;
      const batch = await this.prepare(operation);
      signal.throwIfAborted();
      const events = this.runtime.classify(batch.events);
      const input: EventsInput = { kind: 'events', events };
      if (operation.source !== undefined) {
        input.source = operation.source;
        if (batch.cursor !== undefined) input.cursor = batch.cursor;
      }
      if (batch.truncated) input.truncated = true;
      // Transfer the reservation without yielding or charging the input twice.
      operation.releaseCapacity();
      try { await this.runtime.scopes.submit(this.scope, input, true, signal, undefined, false); }
      catch (error) { if (!signal.aborted) activity.failure = { error }; throw error; }
      if (events.length > 0) await this.settle(activity);
      operation.done.resolve();
    } catch (error) {
      if (operation) operation.done.reject(error);
      else this.runtime.observe({ kind: 'failed', scope: this.scope, input: 'tick', message: messageOf(error) });
    } finally {
      if (operation) { operation.unlink(); operation.releaseCapacity(); }
      this.active = null;
      activity.finished.resolve();
      this.pump();
    }
  }

  track(work: () => Promise<void>): Promise<void> {
    if (this.work === 0) this.workDone = receipt();
    this.work += 1;
    const activity = this.active;
    const finish = (failure: { error: unknown } | null) => {
      if (failure && activity && !activity.failure) activity.failure = failure;
      this.work -= 1;
      if (this.work === 0) this.workDone?.resolve();
    };
    let pending: Promise<void>;
    try { pending = this.context.run(this.owner, work); }
    catch (error) { finish({ error }); return Promise.reject(error); }
    return pending.then(() => finish(null), (error: unknown) => { finish({ error }); throw error; });
  }

  fence(): Promise<void> {
    if (this.closing) return this.closing.promise;
    const closing = receipt();
    this.closing = closing;
    this.timerPending = false;
    const reason = new IntakeError('attend: intake closed');
    this.completion.abort(reason);
    for (const operation of this.queue) operation.controller.abort(reason);
    const activity = this.active;
    activity?.operation?.controller.abort(reason);
    void (async () => {
      await activity?.finished.promise;
      await this.workDone?.promise;
      await Promise.all([...this.preparations]);
      if (activity?.failure) throw activity.failure.error;
    })().then(closing.resolve, closing.reject);
    return closing.promise;
  }

  close(): Promise<void> { this.checkContext(); return this.fence(); }
}
