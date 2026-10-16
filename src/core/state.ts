import type { Buffer, Entry, FnvState, ScopeRecord, ScopeState, SettlePolicy } from './types.js';
import { fnvStart, foldId } from './ids.js';
import { seenFrom, seenIds } from './seen.js';
import { assertWindow, openWindow, windowFrom } from './window.js';
import { nextDue } from './reducer.js';

/** An empty, owned scope at version zero. */
export function initialState(scope: string, _policy: SettlePolicy): ScopeState {
  return {
    scope, version: 0, cursors: {}, ...seenFrom([], [], []), open: openWindow(scope),
    batch: null, outbox: { decisions: [], deadLetters: [] }, recent: [], dueAt: null,
  };
}

/** Builds owned state from a snapshot. Copies every array it will mutate and never mutates `record`. O(W + S + outbox). */
export function fromRecord(record: ScopeRecord, policy: SettlePolicy): ScopeState {
  const held = record.batch;
  const state: ScopeState = {
    scope: record.scope, version: record.version, cursors: record.cursors,
    ...seenFrom(record.seen, record.buffer.entries, held?.window.events ?? []),
    open: windowFrom(record.scope, record.buffer),
    batch: held ? { window: held.window, phase: held.phase, hash: heldHash(record.scope, held.window.events) } : null,
    outbox: { decisions: [...record.outbox.decisions], deadLetters: [...record.outbox.deadLetters] },
    recent: record.recent, dueAt: null,
  };
  state.dueAt = nextDue(policy, state);
  return state;
}

function heldHash(scope: string, events: readonly Entry[]): FnvState {
  const hash = fnvStart(scope);
  for (const entry of events) foldId(hash, entry.id);
  return hash;
}

/** The state as a fresh `ScopeRecord`, deep-equal to the copying reducer's. Shares immutable parts. O(W + S + outbox). */
export function toRecord(state: ScopeState): ScopeRecord {
  const open = state.open;
  const buffer: Buffer = { entries: [...open.entries], dropped: open.dropped, gap: open.gap, forcedAt: open.forcedAt };
  if (open.endedAt !== null) buffer.endedAt = open.endedAt;
  return {
    version: state.version, scope: state.scope, cursors: state.cursors, seen: seenIds(state.seen), buffer,
    batch: state.batch ? { window: state.batch.window, phase: state.batch.phase } : null,
    outbox: { decisions: [...state.outbox.decisions], deadLetters: [...state.outbox.deadLetters] },
    recent: state.recent as ScopeRecord['recent'], dueAt: state.dueAt,
  };
}

/** True when the scope holds nothing: an empty window, no batch, empty lanes and `dueAt === null`. Such a scope may be evicted from the cache. */
export function quiescent(state: ScopeState): boolean {
  return state.open.entries.length === 0 && state.batch === null && state.outbox.decisions.length === 0
    && state.outbox.deadLetters.length === 0 && state.dueAt === null;
}

/** Checks the state's invariants after an apply, in tests and debug builds. Throws, naming the broken invariant. */
export function assertInvariants(state: ScopeState, policy: SettlePolicy): void {
  const ring = state.seen;
  if (ring.size > Math.max(0, policy.seenCount) || ring.ids.length !== ring.capacity || ring.head < 0
    || (ring.capacity > 0 && ring.head >= ring.capacity) || ring.size < 0 || ring.size > ring.capacity) {
    throw new Error('I2: seen ring');
  }
  const expected = new Map<string, number>();
  for (const id of seenIds(ring)) expected.set(id, (expected.get(id) ?? 0) + 1);
  const windows = new Set<string>();
  for (const entries of [state.open.entries, state.batch?.window.events ?? []]) {
    for (const entry of entries) {
      if (windows.has(entry.id)) throw new Error('I1: duplicate window id');
      windows.add(entry.id);
      expected.set(entry.id, (expected.get(entry.id) ?? 0) + 1);
    }
  }
  if (expected.size !== state.known.size) throw new Error('I1: known ids');
  for (const [id, count] of expected) if (state.known.get(id) !== count) throw new Error('I1: known count');
  assertWindow(state.scope, state.open);
  if (state.batch?.hash) {
    const hash = heldHash(state.scope, state.batch.window.events);
    if (hash.hi !== state.batch.hash.hi || hash.lo !== state.batch.hash.lo) throw new Error('I5: held hash');
  }
  if (state.open.endedAt !== null && state.open.entries.length === 0) throw new Error('I3: endedAt on an empty window');
  if (!Object.is(state.dueAt, nextDue(policy, state))) throw new Error('I6: dueAt');
  for (const lane of [state.outbox.decisions, state.outbox.deadLetters]) {
    for (let i = 1; i < lane.length; i += 1) if (lane[i]?.phase.phase === 'inFlight') throw new Error('outbox: more than one in flight in a lane');
  }
}
