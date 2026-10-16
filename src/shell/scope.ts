import { BackpressureError, ConflictError, OutboxFullError } from '../errors.js';
import { apply, newOutput, nextDue, REDUCER_VERSION } from '../core/reducer.js';
import { initialState, quiescent, toRecord } from '../core/state.js';
import { estimateRenderedBytes } from '../core/window.js';
import type { Event, DecisionAppend, ApplyConfig, Clock, Effect, Input, LoggedInput, ScopeState, Trace } from '../core/types.js';
import { stableJson } from '../internal/json.js';
import type { CommitOutcome, ScopeStore, ScopeWrite, SerializedWrite } from '../store/types.js';
import type { AdmissionOptions, Scheduler, RawInput as Raw, ScopeSlot as Slot, StagedWrite as Staged } from './types.js';
import { recordBytes } from './copy.js';
import { replay } from './read.js';

interface Options {
  store: ScopeStore;
  clock: Clock;
  config: ApplyConfig;
  admission: AdmissionOptions | undefined;
  idleMs: number;
  snapshot: 'ratio' | 'always';
  scheduler: Scheduler;
  eventBytes(event: Event): number | undefined;
  running(): boolean;
  execute(scope: string, effect: Effect): void;
  observe(trace: Trace): void;
}

/** Owns each scope's mutable state and commits immutable groups across scopes. */
export function createScopes(options: Options) {
  const { store, clock, config, scheduler } = options;
  const slots = new Map<string, Slot>();
  const staged = new Set<Slot>();
  const pending = new Set<Promise<void>>();
  let queued = 0;
  let committing = false;
  let scheduled = false;

  function slotFor(scope: string): Slot {
    let slot = slots.get(scope);
    if (!slot) {
      slot = { scope, state: null, loading: null, staged: null, inflight: null, queued: [], tailBytes: 0, tailCount: 0,
        snapshotBytes: 0, snapshotNeeded: true, forceWrite: false, storedDue: undefined, lastTouch: clock.now() };
      slots.set(scope, slot);
    }
    return slot;
  }

  function reject(raw: Raw, error: unknown): void {
    raw.rejectHook?.(error);
    raw.reject(error);
  }

  function arm(slot: Slot): void {
    if (options.running()) scheduler.arm(slot.scope, slot.state?.dueAt ?? null);
  }

  function load(slot: Slot): Promise<void> {
    if (slot.loading) return slot.loading;
    if (slot.state) return Promise.resolve();
    slot.loading = (async () => {
      try {
        const stored = await store.load(slot.scope);
        slot.state = stored === null ? initialState(slot.scope, config.policy) : replay(slot.scope, stored, config);
        slot.tailCount = stored?.tail.length ?? 0;
        slot.tailBytes = stored?.tail.reduce((sum, logged) => sum + inputBytes(logged), 0) ?? 0;
        slot.snapshotBytes = stored?.snapshot ? bytes(stored.snapshot.record) : 0;
        slot.snapshotNeeded = !stored?.snapshot;
        slot.forceWrite = false;
        slot.storedDue = undefined;
        if (stored?.snapshot) {
          const upgraded = stored.snapshot.reducer !== REDUCER_VERSION;
          if (upgraded || stableJson(stored.snapshot.policy) !== stableJson(config.policy)) {
            slot.state.dueAt = nextDue(config.policy, slot.state);
            slot.snapshotNeeded = true;
            slot.forceWrite = true;
          }
          if (upgraded) options.observe({ kind: 'replayedAcrossUpgrade', scope: slot.scope, from: stored.snapshot.reducer, to: REDUCER_VERSION });
        }
        arm(slot);
      } catch (error) {
        slot.state = null;
        const raws = slot.queued.splice(0);
        for (const raw of raws) { queued -= raw.count; reject(raw, error); }
      } finally {
        slot.loading = null;
      }
      pump(slot);
    })();
    return slot.loading;
  }

  function bytes(value: unknown): number {
    return store.durable ? Buffer.byteLength(JSON.stringify(value)) : recordBytes(value);
  }

  function inputBytes(logged: LoggedInput): number {
    if (store.durable) return Buffer.byteLength(JSON.stringify(logged));
    return 64 + (logged.input.kind === 'events'
      ? logged.input.events.reduce((sum, event) => sum + (options.eventBytes(event) ?? estimateRenderedBytes(event)) + event.id.length, 0) : 0);
  }

  function requestFlush(): void {
    if (committing || scheduled || staged.size === 0) return;
    scheduled = true;
    queueMicrotask(() => { scheduled = false; void flush(); });
  }

  function stage(slot: Slot, state: ScopeState): Staged {
    if (slot.staged) return slot.staged;
    const serialized = store.durable ? { inputs: [], snapshot: null, appends: [] } satisfies SerializedWrite : undefined;
    const write = { scope: slot.scope, expectedVersion: state.version, inputs: [], snapshot: null, appends: [] } satisfies ScopeWrite;
    const result: Staged = { write, raw: [], effects: [], bytes: 0, snapshotBytes: 0, state, serialized };
    slot.staged = result;
    staged.add(slot);
    return result;
  }

  function transition(slot: Slot, state: ScopeState, input: Input, now: number): Staged | null {
    const out = newOutput();
    const logged = { input, now };
    apply(config, state, logged, out);
    if (out.changed || slot.forceWrite) {
      const current = stage(slot, state);
      current.write.inputs.push(logged);
      state.version += 1;
      if (current.serialized) {
        const text = JSON.stringify(logged);
        current.serialized.inputs.push(text);
        current.bytes += Buffer.byteLength(text);
        for (const append of out.appends) current.serialized.appends.push(JSON.stringify(append.body));
      } else current.bytes += inputBytes(logged);
      current.write.appends.push(...out.appends);
      current.effects.push(...out.effects);
      slot.forceWrite = false;
      return current;
    }
    if (slot.staged) slot.staged.effects.push(...out.effects);
    else for (const effect of out.effects) options.execute(slot.scope, effect);
    return slot.staged;
  }

  function applyRaw(slot: Slot, raw: Raw): void {
    const state = slot.state!;
    slot.lastTouch = clock.now();
    try {
      raw.signal?.throwIfAborted();
      const input = raw.input;
      if (input.kind === 'events' && input.events.length > 0) {
        if (state.outbox.decisions.length >= config.policy.maxOutbox) throw new OutboxFullError(slot.scope, 'decisions', state.outbox.decisions.length);
        if (state.outbox.deadLetters.length >= config.policy.maxDeadLetters) throw new OutboxFullError(slot.scope, 'deadLetters', state.outbox.deadLetters.length);
      }
      const now = clock.now();
      if (options.running() && (input.kind === 'events' || input.kind === 'flush' || input.kind === 'end') && state.dueAt !== null && state.dueAt < now) {
        transition(slot, state, { kind: 'tick' }, now);
      }
      const current = transition(slot, state, input, now);
      if (current) current.raw.push(raw);
      else raw.resolve();
      arm(slot);
      requestFlush();
    } catch (error) {
      reject(raw, error);
      if (!(error instanceof OutboxFullError) && !raw.signal?.aborted) slot.state = null;
      requestFlush();
    }
  }

  function pump(slot: Slot): void {
    if (slot.inflight || slot.loading) return;
    if (!slot.state) {
      if (slot.queued.length > 0 && !slot.staged) void load(slot);
      return;
    }
    while (slot.queued.length > 0 && slot.state && !slot.inflight && !slot.loading) {
      const raw = slot.queued.shift()!;
      queued -= raw.count;
      applyRaw(slot, raw);
    }
  }

  function submit(scope: string, input: Input, external = false, signal?: AbortSignal, rejectHook?: (error: unknown) => void): Promise<void> {
    if (external) {
      const lagMs = scheduler.lagMs();
      const overQueue = queued >= (options.admission?.maxQueuedInputs ?? 20000);
      const overLag = options.admission?.maxLagMs !== undefined && lagMs > options.admission.maxLagMs;
      if (overQueue || overLag) {
        const error = new BackpressureError(overLag ? Math.ceil(lagMs) : 100, queued);
        options.observe({ kind: 'backpressure', queued, lagMs });
        rejectHook?.(error);
        return Promise.reject(error);
      }
    }
    const slot = slotFor(scope);
    const last = slot.queued.at(-1);
    if (last && input.kind === 'events' && last.input.kind === 'events' && last.input.source === input.source && last.signal === signal) {
      last.input.events.push(...input.events);
      if (input.cursor !== undefined) last.input.cursor = input.cursor;
      if (input.truncated) last.input.truncated = true;
      last.count += 1;
      queued += 1;
      return last.done;
    }
    if (last && input.kind === 'tick' && last.input.kind === 'tick') return last.done;
    let resolve!: () => void;
    let fail!: (error: unknown) => void;
    const done = new Promise<void>((yes, no) => { resolve = yes; fail = no; });
    const raw: Raw = { input, signal, rejectHook, done, resolve, reject: fail, count: 1, conflicts: 0 };
    pending.add(done);
    void done.then(() => pending.delete(done), () => pending.delete(done));
    slot.queued.push(raw);
    queued += 1;
    pump(slot);
    return done;
  }

  function prepare(slot: Slot): Staged {
    const current = slot.staged!;
    const { write, state } = current;
    if (slot.snapshotNeeded || options.snapshot === 'always' || slot.tailCount + write.inputs.length > 4096
      || slot.tailBytes + current.bytes >= Math.max(slot.snapshotBytes, 4096)) {
      write.snapshot = { version: state.version, record: toRecord(state), policy: config.policy, reducer: REDUCER_VERSION };
      if (current.serialized) current.serialized.snapshot = JSON.stringify(write.snapshot.record);
      current.snapshotBytes = current.serialized?.snapshot ? Buffer.byteLength(current.serialized.snapshot) : recordBytes(write.snapshot.record);
    }
    const due = state.dueAt === null ? null : Math.ceil(state.dueAt / 1000) * 1000;
    if (due !== slot.storedDue) write.due = due;
    else delete write.due;
    if (current.serialized) write.serialized = current.serialized;
    return current;
  }

  async function outcome(slot: Slot, current: Staged, result: CommitOutcome | unknown, failed = false): Promise<void> {
    slot.inflight = null;
    if (failed) {
      slot.state = null;
      for (const raw of current.raw) reject(raw, result);
    } else if (result === 'conflict') {
      options.observe({ kind: 'conflict', scope: slot.scope, version: current.write.expectedVersion });
      slot.state = null;
      const attempt = 1 + Math.max(0, ...current.raw.map((raw) => raw.conflicts));
      const retry: Raw[] = [];
      for (const raw of current.raw) {
        raw.conflicts = attempt;
        if (attempt >= 8) {
          options.observe({ kind: 'abandoned', scope: slot.scope, input: raw.input.kind });
          reject(raw, new ConflictError(slot.scope, 8));
        } else retry.push(raw);
      }
      slot.queued.unshift(...retry);
      queued += retry.reduce((sum, raw) => sum + raw.count, 0);
      slot.loading = new Promise<void>((resolve) => setTimeout(resolve, 2 * attempt + Math.random() * 4));
      await slot.loading;
      slot.loading = null;
    } else {
      if (current.write.snapshot) {
        slot.tailBytes = 0;
        slot.tailCount = 0;
        slot.snapshotBytes = current.snapshotBytes;
        slot.snapshotNeeded = false;
      } else {
        slot.tailBytes += current.bytes;
        slot.tailCount += current.write.inputs.length;
      }
      if (current.write.due !== undefined) slot.storedDue = current.write.due;
      for (const effect of current.effects) options.execute(slot.scope, effect);
      for (const raw of current.raw) raw.resolve();
    }
    pump(slot);
  }

  async function commit(group: { slot: Slot; current: Staged }[]): Promise<void> {
    try {
      const results = await store.commit(group.map(({ current }) => current.write));
      await Promise.all(group.map(({ slot, current }, i) => outcome(slot, current, results[i])));
    } catch (error) {
      if (group.length === 1) await outcome(group[0]!.slot, group[0]!.current, error, true);
      else {
        const middle = Math.floor(group.length / 2);
        await commit(group.slice(0, middle));
        await commit(group.slice(middle));
      }
    }
  }

  async function flush(): Promise<void> {
    if (committing || staged.size === 0) return;
    committing = true;
    const group: { slot: Slot; current: Staged }[] = [];
    let size = 0;
    for (const slot of staged) {
      const current = prepare(slot);
      const estimate = current.bytes + current.snapshotBytes;
      if (group.length > 0 && (group.length >= 512 || size + estimate > 4 * 1024 * 1024)) break;
      slot.inflight = current;
      slot.staged = null;
      staged.delete(slot);
      size += estimate;
      group.push({ slot, current });
    }
    group.sort((a, b) => a.slot.scope < b.slot.scope ? -1 : a.slot.scope > b.slot.scope ? 1 : 0);
    try { await commit(group); }
    finally { committing = false; requestFlush(); }
  }

  return {
    submit,
    async touch(scope: string): Promise<number | null> {
      const slot = slotFor(scope);
      if (!slot.state) await load(slot);
      arm(slot);
      return slot.state?.dueAt ?? null;
    },
    due(scope: string): number | null { return slots.get(scope)?.state?.dueAt ?? null; },
    armAll(): void { for (const slot of slots.values()) arm(slot); },
    evict(): void {
      for (const [scope, slot] of slots) {
        if (slot.state && quiescent(slot.state) && !slot.staged && !slot.inflight && !slot.loading && slot.queued.length === 0
          && slot.lastTouch + options.idleMs <= clock.now()) slots.delete(scope);
      }
    },
    pending(): Promise<void> {
      return Promise.all([...pending].map((done) => done.catch(() => undefined))).then(() => undefined);
    },
  };
}
