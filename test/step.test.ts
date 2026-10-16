import { describe, expect, test } from 'vitest';
import { deepFreeze } from './support/deep-freeze.js';
import { deadline, initialRecord, offeredRoutes, step as reduce, validate, windowId, type StepConfig } from '../src/step.js';
import type { DeadLetter, Decision, Delivery, Entry, Event, EventsInput, FlushInput, Input, Kind, ReleasedInput, ScopeRecord, Stepped, TraceEffect, Window } from '../src/types.js';
import { DEFAULT_POLICY } from '../src/types.js';

/** Every record this suite steps is frozen through, so a reducer that mutates its input throws here (modules are strict). */
function step(config: StepConfig, record: ScopeRecord, input: EventsInput | FlushInput | ReleasedInput, now: number): Stepped<TraceEffect>;
function step(config: StepConfig, record: ScopeRecord, input: Input, now: number): Stepped;
function step(config: StepConfig, record: ScopeRecord, input: Input, now: number): Stepped {
  return reduce(config, deepFreeze(record), input, now);
}

const kind = (e: Event): Kind => {
  switch (e.name) {
    case 'close':
      return 'closing';
    case 'say':
      return 'flush';
    case 'drag':
      return 'continuous';
    case 'look':
      return 'passive';
    case 'out':
      return 'consequence';
    default:
      return 'context';
  }
};

const cfg: StepConfig = { policy: DEFAULT_POLICY };
const scope = 's';

const base = (): ScopeRecord => initialRecord(scope);
const ev = (id: string, name: string): Event & { kind: Kind } => ({ id, at: 0, name, data: {}, kind: kind({ id, at: 0, name, data: {} }) });
const closing = (id: string, seenAt: number): Entry => ({ id, at: 0, name: 'close', data: {}, kind: 'closing', seenAt });
const consequence = (id: string, seenAt: number): Entry => ({ id, at: 0, name: 'out', data: {}, kind: 'consequence', seenAt });
const continuous = (id: string, seenAt: number): Entry => ({ id, at: 0, name: 'drag', data: {}, kind: 'continuous', seenAt });
const passive = (id: string, seenAt: number): Entry => ({ id, at: 0, name: 'look', data: {}, kind: 'passive', seenAt });
const flush = (id: string, seenAt: number): Entry => ({ id, at: 0, name: 'say', data: {}, kind: 'flush', seenAt });

const A = closing('a', 100);
const ID_A = '3ecbd4f2ef91c78f';
const ID_B = '3ecbd5f2ef91c942';
const ID_AB = 'f9df8ba2df127ca6';
const ID_X = '3ecbcbf2ef91b844';
const ID_AC = 'f9df8ca2df127e59';
const ID_ABC = '1a353cc02182e85a';
const ID_AC1 = '7dda02b866ecd347';
const NO_WAIT = ['interrupt', 'queue', 'note', 'ask', 'skip'];

const windowA: Window = { id: ID_A, events: [A], reason: 'closing', waits: 0, dropped: 0, gap: false, settledAt: 600 };

const judgingA = (): ScopeRecord => ({
  ...base(),
  seen: ['a'],
  batch: { window: windowA, phase: { phase: 'judging', attempt: 1, failures: 0, leaseUntil: 30_600, offerWait: true } },
  dueAt: 30_600,
});

const decisionA: Decision = {
  id: ID_A,
  scope,
  route: 'queue',
  probabilities: { interrupt: 0, queue: 1, note: 0, ask: 0, skip: 0, wait: 0 },
  predicates: {},
  confidence: null,
  window: windowA,
  at: 700,
};

const decisionItem = (decision: Decision) => ({ id: decision.id, kind: 'decision' as const, decision });
const letterItem = (letter: DeadLetter) => ({ id: letter.id, kind: 'deadLetter' as const, letter });

describe('ingest', () => {
  test('appends kinded entries, records ids, derives dueAt and emits only traces', () => {
    const { record, effects } = step(cfg, base(), { kind: 'events', events: [ev('a', 'close'), ev('b', 'out')] }, 100);
    expect(record).toEqual({
      ...base(),
      seen: ['a', 'b'],
      buffer: { entries: [A, consequence('b', 100)], dropped: 0, gap: false, forcedAt: null },
      dueAt: 600,
    });
    expect(effects).toEqual([{ kind: 'trace', trace: { kind: 'ingested', scope, count: 2, duplicates: 0 } }]);
  });

  test('dedups by id against the seen ring, the buffer and the batch', () => {
    const before: ScopeRecord = { ...judgingA(), seen: ['z'], buffer: { entries: [consequence('b', 100)], dropped: 0, gap: false, forcedAt: null } };
    const { record, effects } = step(cfg, before, { kind: 'events', events: [ev('z', 'close'), ev('b', 'out'), ev('a', 'close')] }, 200);
    expect(record).toEqual(before);
    expect(effects).toEqual([{ kind: 'trace', trace: { kind: 'ingested', scope, count: 0, duplicates: 3 } }]);
  });

  test("moves the source's cursor and a truncated batch marks the window with a gap", () => {
    const { record, effects } = step(cfg, base(), { kind: 'events', events: [ev('a', 'close')], source: 'log:s', cursor: 'c9', truncated: true }, 100);
    expect(record).toEqual({ ...base(), cursors: { 'log:s': 'c9' }, seen: ['a'], buffer: { entries: [A], dropped: 0, gap: true, forcedAt: null }, dueAt: 600 });
    expect(effects).toEqual([
      { kind: 'trace', trace: { kind: 'gap', scope, source: 'log:s', cursor: 'c9' } },
      { kind: 'trace', trace: { kind: 'ingested', scope, count: 1, duplicates: 0 } },
    ]);
  });

  test('each source keeps its own cursor and a push without a source touches none', () => {
    const first = step(cfg, base(), { kind: 'events', events: [ev('a', 'out')], source: 'log:s', cursor: '7' }, 100).record;
    const second = step(cfg, first, { kind: 'events', events: [ev('b', 'out')], source: 'mcp:alpha:{}', cursor: 'x1' }, 100).record;
    const pushed = step(cfg, second, { kind: 'events', events: [ev('c', 'out')], cursor: 'ignored' }, 100).record;
    expect(pushed.cursors).toEqual({ 'log:s': '7', 'mcp:alpha:{}': 'x1' });
    const moved = step(cfg, pushed, { kind: 'events', events: [], source: 'log:s', cursor: '8' }, 100).record;
    expect(moved.cursors).toEqual({ 'log:s': '8', 'mcp:alpha:{}': 'x1' });
  });

  test('a gap with no open window marks the next one', () => {
    const { record } = step(cfg, base(), { kind: 'events', events: [], source: 'src', cursor: null, truncated: true }, 100);
    expect(record).toEqual({ ...base(), cursors: { src: null }, buffer: { entries: [], dropped: 0, gap: true, forcedAt: null } });
    const settled = step(cfg, step(cfg, record, { kind: 'events', events: [ev('a', 'close')] }, 100).record, { kind: 'tick' }, 600);
    expect(settled.record.batch?.window).toMatchObject({ id: ID_A, gap: true });
  });

  test('a flush is due at once but still emits no effect on ingest', () => {
    const { record, effects } = step(cfg, base(), { kind: 'events', events: [ev('a', 'say')] }, 100);
    expect(record.dueAt).toBe(100);
    expect(effects).toEqual([{ kind: 'trace', trace: { kind: 'ingested', scope, count: 1, duplicates: 0 } }]);
  });

  test('overflow drops the oldest non-act first and never an act', () => {
    const small: StepConfig = { policy: { ...DEFAULT_POLICY, maxEvents: 2 } };
    const { record, effects } = step(small, base(), { kind: 'events', events: [ev('a', 'close'), ev('b', 'out'), ev('c', 'close')] }, 100);
    expect(record.buffer).toEqual({ entries: [A, closing('c', 100)], dropped: 1, gap: false, forcedAt: null });
    expect(effects[0]).toEqual({ kind: 'trace', trace: { kind: 'dropped', scope, entries: [consequence('b', 100)], why: 'overflow' } });
  });

  test('the seen ring is bounded by seenCount, oldest out', () => {
    const tiny: StepConfig = { policy: { ...DEFAULT_POLICY, seenCount: 2 } };
    const { record } = step(tiny, base(), { kind: 'events', events: [ev('a', 'out'), ev('b', 'out'), ev('c', 'out')] }, 100);
    expect(record.seen).toEqual(['b', 'c']);
  });
});

describe('deadline', () => {
  const buf = (entries: Entry[]) => ({ entries, dropped: 0, gap: false, forcedAt: null });

  test('closing grace', () => {
    expect(deadline(DEFAULT_POLICY, buf([A, consequence('b', 400)]))).toEqual({ at: 600, reason: 'closing' });
  });

  test('stream gap after the last continuous entry', () => {
    expect(deadline(DEFAULT_POLICY, buf([continuous('a', 100), continuous('b', 500)]))).toEqual({ at: 1500, reason: 'stream_quiet' });
  });

  test('passive quiet', () => {
    expect(deadline(DEFAULT_POLICY, buf([passive('a', 100)]))).toEqual({ at: 4100, reason: 'passive_quiet' });
  });

  test('a flush is due when it was seen', () => {
    expect(deadline(DEFAULT_POLICY, buf([passive('a', 100), flush('b', 300)]))).toEqual({ at: 300, reason: 'flush' });
  });

  test('a window of only acts past maxEvents is due at its last entry', () => {
    const policy = { ...DEFAULT_POLICY, maxEvents: 2 };
    expect(deadline(policy, buf([closing('a', 100), closing('b', 200), closing('c', 300)]))).toEqual({ at: 300, reason: 'overflow' });
  });

  test('age wins over a gesture that keeps the window busy', () => {
    expect(deadline(DEFAULT_POLICY, buf([continuous('a', 100), continuous('b', 59_800)]))).toEqual({ at: 60_100, reason: 'age' });
  });

  test('no act and no flush means no deadline', () => {
    expect(deadline(DEFAULT_POLICY, buf([consequence('a', 100), { ...consequence('b', 100), kind: 'context' }]))).toBeNull();
    expect(deadline(DEFAULT_POLICY, buf([]))).toBeNull();
  });

  test('a forced window is due at the flush call, even with nothing buffered', () => {
    expect(deadline(DEFAULT_POLICY, { ...buf([consequence('a', 100)]), forcedAt: 150 })).toEqual({ at: 150, reason: 'forced' });
    expect(deadline(DEFAULT_POLICY, { ...buf([]), forcedAt: 150 })).toEqual({ at: 150, reason: 'forced' });
  });
});

describe('settling', () => {
  const open = (): ScopeRecord => ({ ...base(), seen: ['a'], buffer: { entries: [A], dropped: 0, gap: false, forcedAt: null }, dueAt: 600 });

  test('a tick before the deadline changes nothing', () => {
    const { record, effects } = step(cfg, open(), { kind: 'tick' }, 599);
    expect(record).toEqual(open());
    expect(effects).toEqual([]);
  });

  test('a tick at the deadline settles into a judge call with wait offered', () => {
    const { record, effects } = step(cfg, open(), { kind: 'tick' }, 600);
    expect(record).toEqual(judgingA());
    expect(effects).toEqual([
      { kind: 'trace', trace: { kind: 'settled', scope, batchId: ID_A, reason: 'closing', count: 1, merged: false } },
      { kind: 'judge', scope, batchId: ID_A, attempt: 1, window: windowA, offered: ['interrupt', 'queue', 'note', 'ask', 'skip', 'wait'], recent: [], leaseUntil: 30_600 },
      { kind: 'trace', trace: { kind: 'judging', scope, batchId: ID_A, attempt: 1, offerWait: true } },
    ]);
  });

  test('a flush settles with wait withheld', () => {
    const before: ScopeRecord = { ...base(), seen: ['a'], buffer: { entries: [flush('a', 100)], dropped: 0, gap: false, forcedAt: null }, dueAt: 100 };
    const { record, effects } = step(cfg, before, { kind: 'tick' }, 100);
    expect(record.batch).toMatchObject({ window: { reason: 'flush' }, phase: { phase: 'judging', offerWait: false } });
    expect(effects[1]).toMatchObject({ kind: 'judge', offered: ['interrupt', 'queue', 'note', 'ask', 'skip'] });
  });

  test('a flush that lands after a closing deadline passed is judged as flush with wait withheld', () => {
    const C = flush('c', 700);
    const before: ScopeRecord = { ...base(), seen: ['a', 'c'], buffer: { entries: [A, C], dropped: 0, gap: false, forcedAt: null }, dueAt: 600 };
    const { record, effects } = step(cfg, before, { kind: 'tick' }, 700);
    expect(record.batch).toEqual({
      window: { id: ID_AC, events: [A, C], reason: 'flush', waits: 0, dropped: 0, gap: false, settledAt: 700 },
      phase: { phase: 'judging', attempt: 1, failures: 0, leaseUntil: 30_700, offerWait: false },
    });
    expect(effects[0]).toEqual({ kind: 'trace', trace: { kind: 'settled', scope, batchId: ID_AC, reason: 'flush', count: 2, merged: false } });
    expect(effects[1]).toMatchObject({ kind: 'judge', offered: NO_WAIT });
  });

  test('a forced window whose closing deadline had already passed settles as forced with wait withheld', () => {
    const before: ScopeRecord = { ...base(), seen: ['a'], buffer: { entries: [A], dropped: 0, gap: false, forcedAt: 700 }, dueAt: 600 };
    const { record, effects } = step(cfg, before, { kind: 'tick' }, 700);
    expect(record.batch).toEqual({
      window: { ...windowA, reason: 'forced', settledAt: 700 },
      phase: { phase: 'judging', attempt: 1, failures: 0, leaseUntil: 30_700, offerWait: false },
    });
    expect(effects[1]).toMatchObject({ kind: 'judge', offered: NO_WAIT });
  });

  test('an act-less window never fires', () => {
    const before: ScopeRecord = { ...base(), buffer: { entries: [consequence('a', 100)], dropped: 0, gap: false, forcedAt: null } };
    const { record, effects } = step(cfg, before, { kind: 'tick' }, 50_000);
    expect(record).toEqual({ ...before, dueAt: null });
    expect(effects).toEqual([]);
  });

  test('an all-act window past maxEvents settles as overflow', () => {
    const small: StepConfig = { policy: { ...DEFAULT_POLICY, maxEvents: 2 } };
    const entries = [closing('a', 100), closing('b', 100), closing('c', 100)];
    const before: ScopeRecord = { ...base(), buffer: { entries, dropped: 0, gap: false, forcedAt: null } };
    const { record } = step(small, before, { kind: 'tick' }, 100);
    expect(record.batch).toMatchObject({ window: { reason: 'overflow', events: entries }, phase: { phase: 'judging' } });
  });

  test('a window older than maxWindowMs settles as age', () => {
    const entries = [continuous('a', 100), continuous('b', 59_800)];
    const before: ScopeRecord = { ...base(), buffer: { entries, dropped: 0, gap: false, forcedAt: null } };
    const { record } = step(cfg, before, { kind: 'tick' }, 60_100);
    expect(record.batch?.window).toMatchObject({ reason: 'age', events: entries });
  });

  test('stale act-less entries before the first act are pruned, counted and traced', () => {
    const before: ScopeRecord = { ...base(), buffer: { entries: [consequence('x', 0), closing('a', 70_000)], dropped: 0, gap: false, forcedAt: null } };
    const { record, effects } = step(cfg, before, { kind: 'tick' }, 70_000);
    expect(record.buffer).toEqual({ entries: [closing('a', 70_000)], dropped: 1, gap: false, forcedAt: null });
    expect(effects).toEqual([{ kind: 'trace', trace: { kind: 'dropped', scope, entries: [consequence('x', 0)], why: 'age' } }]);
  });

  test('dueAt during a live judge is the lease expiry even when the buffer is due', () => {
    const before: ScopeRecord = { ...judgingA(), buffer: { entries: [closing('b', 100)], dropped: 0, gap: false, forcedAt: null } };
    const { record, effects } = step(cfg, before, { kind: 'tick' }, 1000);
    expect(record).toEqual({ ...before, dueAt: 30_600 });
    expect(effects).toEqual([]);
  });

  test('a window that settled while judging settles the moment judged lands', () => {
    const B = closing('b', 100);
    const before: ScopeRecord = { ...judgingA(), buffer: { entries: [B], dropped: 0, gap: false, forcedAt: null } };
    const { record, effects } = step(cfg, before, { kind: 'judged', batchId: ID_A, attempt: 1, judgment: { route: 'queue' } }, 700);
    const windowB: Window = { id: ID_B, events: [B], reason: 'closing', waits: 0, dropped: 0, gap: false, settledAt: 700 };
    const inFlight: Delivery = { item: decisionItem(decisionA), phase: { phase: 'inFlight', attempt: 1, failures: 0, leaseUntil: 10_700 } };
    expect(record).toEqual({
      ...base(),
      seen: ['a'],
      batch: { window: windowB, phase: { phase: 'judging', attempt: 1, failures: 0, leaseUntil: 30_700, offerWait: true } },
      outbox: { decisions: [inFlight], deadLetters: [] },
      recent: [{ id: ID_A, route: 'queue', at: 700 }],
      dueAt: 10_700,
    });
    expect(effects.map((e) => (e.kind === 'trace' ? e.trace.kind : e.kind))).toEqual(['judged', 'deliver', 'delivering', 'settled', 'judge', 'judging']);
    expect(effects[1]).toEqual({ kind: 'deliver', scope, delivery: inFlight });
  });
});

describe('judging', () => {
  test('a stale judged input is ignored', () => {
    const before = judgingA();
    const { record, effects } = step(cfg, before, { kind: 'judged', batchId: ID_A, attempt: 2, judgment: { route: 'queue' } }, 700);
    expect(record).toEqual(before);
    expect(effects).toEqual([{ kind: 'trace', trace: { kind: 'stale', scope, input: 'judged', why: `no live judge attempt 2 for batch ${ID_A}` } }]);
  });

  test('a skip is recorded but never delivered', () => {
    const { record, effects } = step(cfg, judgingA(), { kind: 'judged', batchId: ID_A, attempt: 1, judgment: { route: 'skip' } }, 700);
    expect(record).toEqual({ ...base(), seen: ['a'], recent: [{ id: ID_A, route: 'skip', at: 700 }], dueAt: null });
    expect(effects).toEqual([{ kind: 'trace', trace: { kind: 'judged', scope, batchId: ID_A, route: 'skip' } }]);
  });

  test('a wait holds the batch until the horizon, keeping the attempt count', () => {
    const { record, effects } = step(cfg, judgingA(), { kind: 'judged', batchId: ID_A, attempt: 1, judgment: { route: 'wait' } }, 700);
    expect(record).toEqual({ ...base(), seen: ['a'], batch: { window: { ...windowA, waits: 1 }, phase: { phase: 'waiting', attempt: 1, until: 10_700 } }, dueAt: 10_700 });
    expect(effects).toEqual([{ kind: 'trace', trace: { kind: 'waiting', scope, batchId: ID_A, waits: 1, until: 10_700 } }]);
  });

  test('a wait that was not offered is a retryable failure', () => {
    const before: ScopeRecord = { ...judgingA(), batch: { window: windowA, phase: { phase: 'judging', attempt: 1, failures: 0, leaseUntil: 30_600, offerWait: false } } };
    const { record } = step(cfg, before, { kind: 'judged', batchId: ID_A, attempt: 1, judgment: { route: 'wait' } }, 700);
    expect(record.batch).toEqual({ window: windowA, phase: { phase: 'retrying', attempt: 1, failures: 1, at: 1700, offerWait: false } });
  });

  test('the horizon judges the batch alone with wait withheld and the next attempt number', () => {
    const before: ScopeRecord = { ...base(), seen: ['a'], batch: { window: { ...windowA, waits: 1 }, phase: { phase: 'waiting', attempt: 1, until: 10_700 } }, dueAt: 10_700 };
    const { record, effects } = step(cfg, before, { kind: 'tick' }, 10_700);
    expect(record.batch).toEqual({ window: { ...windowA, waits: 1 }, phase: { phase: 'judging', attempt: 2, failures: 0, leaseUntil: 40_700, offerWait: false } });
    expect(effects[0]).toMatchObject({ kind: 'judge', attempt: 2, offered: ['interrupt', 'queue', 'note', 'ask', 'skip'] });
  });

  test('a judge result from before a wait is ignored after the horizon', () => {
    const waiting: ScopeRecord = { ...base(), seen: ['a'], batch: { window: { ...windowA, waits: 1 }, phase: { phase: 'waiting', attempt: 1, until: 10_700 } }, dueAt: 10_700 };
    const rejudging = step(cfg, waiting, { kind: 'tick' }, 10_700).record;
    const zombie = step(cfg, rejudging, { kind: 'judged', batchId: ID_A, attempt: 1, judgment: { route: 'interrupt' } }, 10_800);
    expect(zombie.record).toEqual(rejudging);
    expect(zombie.effects).toEqual([{ kind: 'trace', trace: { kind: 'stale', scope, input: 'judged', why: `no live judge attempt 1 for batch ${ID_A}` } }]);
    expect(zombie.record.outbox.decisions).toEqual([]);
  });

  test('the horizon merges what arrived while waiting, consequences included', () => {
    const a = passive('a', 100);
    const held: Window = { id: windowId(scope, ['a']), events: [a], reason: 'passive_quiet', waits: 1, dropped: 0, gap: false, settledAt: 4100 };
    const c1 = consequence('c1', 6000);
    const before: ScopeRecord = {
      ...base(),
      seen: ['a', 'c1'],
      buffer: { entries: [c1], dropped: 0, gap: false, forcedAt: null },
      batch: { window: held, phase: { phase: 'waiting', attempt: 1, until: 14_200 } },
      dueAt: 14_200,
    };
    const { record, effects } = step(cfg, before, { kind: 'tick' }, 14_200);
    expect(record.buffer).toEqual({ entries: [], dropped: 0, gap: false, forcedAt: null });
    expect(record.batch).toEqual({
      window: { id: ID_AC1, events: [a, c1], reason: 'passive_quiet', waits: 1, dropped: 0, gap: false, settledAt: 14_200 },
      phase: { phase: 'judging', attempt: 2, failures: 0, leaseUntil: 44_200, offerWait: false },
    });
    expect(effects[0]).toEqual({ kind: 'trace', trace: { kind: 'settled', scope, batchId: ID_AC1, reason: 'passive_quiet', count: 2, merged: true } });
    expect(effects[1]).toMatchObject({ kind: 'judge', window: { events: [{ id: 'a' }, { id: 'c1' }] }, offered: ['interrupt', 'queue', 'note', 'ask', 'skip'] });
  });

  test('a new settle while waiting merges and judges at once, keeping waits', () => {
    const B = closing('b', 1000);
    const before: ScopeRecord = {
      ...base(),
      seen: ['a', 'b'],
      buffer: { entries: [B], dropped: 0, gap: false, forcedAt: null },
      batch: { window: { ...windowA, waits: 1 }, phase: { phase: 'waiting', attempt: 1, until: 10_700 } },
      dueAt: 1500,
    };
    const { record, effects } = step(cfg, before, { kind: 'tick' }, 1500);
    expect(record.batch).toEqual({
      window: { id: ID_AB, events: [A, B], reason: 'closing', waits: 1, dropped: 0, gap: false, settledAt: 1500 },
      phase: { phase: 'judging', attempt: 2, failures: 0, leaseUntil: 31_500, offerWait: true },
    });
    expect(effects[0]).toEqual({ kind: 'trace', trace: { kind: 'settled', scope, batchId: ID_AB, reason: 'closing', count: 2, merged: true } });
  });

  test('a flush merged into a waiting window withholds wait although the act deadline fired first', () => {
    const B = closing('b', 1000);
    const C = flush('c', 1600);
    const before: ScopeRecord = {
      ...base(),
      seen: ['a', 'b', 'c'],
      buffer: { entries: [B, C], dropped: 0, gap: false, forcedAt: null },
      batch: { window: { ...windowA, waits: 1 }, phase: { phase: 'waiting', attempt: 1, until: 10_700 } },
      dueAt: 1500,
    };
    const { record, effects } = step(cfg, before, { kind: 'tick' }, 1600);
    expect(record.batch).toEqual({
      window: { id: ID_ABC, events: [A, B, C], reason: 'flush', waits: 1, dropped: 0, gap: false, settledAt: 1600 },
      phase: { phase: 'judging', attempt: 2, failures: 0, leaseUntil: 31_600, offerWait: false },
    });
    expect(effects[0]).toEqual({ kind: 'trace', trace: { kind: 'settled', scope, batchId: ID_ABC, reason: 'flush', count: 3, merged: true } });
    expect(effects[1]).toMatchObject({ kind: 'judge', offered: NO_WAIT });
  });

  test('a forced window stays forced when a later act merges into its retry', () => {
    const B = closing('b', 1000);
    const before: ScopeRecord = {
      ...base(),
      buffer: { entries: [B], dropped: 0, gap: false, forcedAt: null },
      batch: { window: { ...windowA, reason: 'forced' }, phase: { phase: 'retrying', attempt: 1, failures: 1, at: 4700, offerWait: false } },
    };
    const { record } = step(cfg, before, { kind: 'tick' }, 1500);
    expect(record.batch).toEqual({
      window: { id: ID_AB, events: [A, B], reason: 'forced', waits: 0, dropped: 0, gap: false, settledAt: 1500 },
      phase: { phase: 'judging', attempt: 2, failures: 1, leaseUntil: 31_500, offerWait: false },
    });
  });

  test('wait is withheld once waits reaches maxWaits', () => {
    const B = closing('b', 1000);
    const before: ScopeRecord = {
      ...base(),
      buffer: { entries: [B], dropped: 0, gap: false, forcedAt: null },
      batch: { window: { ...windowA, waits: 2 }, phase: { phase: 'waiting', attempt: 2, until: 10_700 } },
    };
    const { record } = step(cfg, before, { kind: 'tick' }, 1500);
    expect(record.batch).toMatchObject({ window: { waits: 2 }, phase: { offerWait: false } });
  });

  test('a retryable failure backs off, then retries with the next attempt', () => {
    const failed = step(cfg, judgingA(), { kind: 'judgeFailed', batchId: ID_A, attempt: 1, message: 'busy', permanent: false }, 700);
    expect(failed.record).toEqual({ ...base(), seen: ['a'], batch: { window: windowA, phase: { phase: 'retrying', attempt: 1, failures: 1, at: 1700, offerWait: true } }, dueAt: 1700 });
    expect(failed.effects).toEqual([{ kind: 'trace', trace: { kind: 'retrying', scope, stage: 'judge', id: ID_A, attempt: 1, at: 1700, message: 'busy' } }]);
    const retried = step(cfg, failed.record, { kind: 'tick' }, 1700);
    expect(retried.record.batch).toEqual({ window: windowA, phase: { phase: 'judging', attempt: 2, failures: 1, leaseUntil: 31_700, offerWait: true } });
    expect(retried.effects[0]).toMatchObject({ kind: 'judge', attempt: 2, offered: ['interrupt', 'queue', 'note', 'ask', 'skip', 'wait'] });
  });

  test('a new settle while retrying merges and judges at once, keeping attempts and failures', () => {
    const B = closing('b', 1000);
    const before: ScopeRecord = {
      ...base(),
      buffer: { entries: [B], dropped: 0, gap: false, forcedAt: null },
      batch: { window: windowA, phase: { phase: 'retrying', attempt: 2, failures: 2, at: 4700, offerWait: true } },
    };
    const { record } = step(cfg, before, { kind: 'tick' }, 1500);
    expect(record.batch).toMatchObject({ window: { id: ID_AB }, phase: { phase: 'judging', attempt: 3, failures: 2 } });
  });

  test('an expired judge lease counts as one retryable failure', () => {
    const { record } = step(cfg, judgingA(), { kind: 'tick' }, 30_600);
    expect(record.batch).toEqual({ window: windowA, phase: { phase: 'retrying', attempt: 1, failures: 1, at: 31_600, offerWait: true } });
  });

  test('a judge call the runner never started is released: it retries at once, costs no failure, and the retry takes its attempt number', () => {
    const due: Delivery = { item: decisionItem({ ...decisionA, id: ID_B }), phase: { phase: 'retrying', attempt: 0, failures: 0, at: 650 } };
    const before: ScopeRecord = { ...judgingA(), outbox: { decisions: [due], deadLetters: [] } };
    const { record, effects } = step(cfg, before, { kind: 'judgeReleased', batchId: ID_A, attempt: 1 }, 700);
    expect(record).toEqual({ ...before, batch: { window: windowA, phase: { phase: 'retrying', attempt: 0, failures: 0, at: 700, offerWait: true } }, dueAt: 650 });
    expect(effects).toEqual([{ kind: 'trace', trace: { kind: 'released', scope, stage: 'judge', id: ID_A, attempt: 1 } }]);
    const ticked = step(cfg, record, { kind: 'tick' }, 700);
    expect(ticked.record.batch).toEqual({ window: windowA, phase: { phase: 'judging', attempt: 1, failures: 0, leaseUntil: 30_700, offerWait: true } });
    expect(ticked.effects.map((e) => (e.kind === 'trace' ? e.trace.kind : e.kind))).toEqual(['deliver', 'delivering', 'judge', 'judging']);
    expect(ticked.effects[2]).toMatchObject({ kind: 'judge', attempt: 1, offered: ['interrupt', 'queue', 'note', 'ask', 'skip', 'wait'] });
  });

  test('a release names one attempt, so a replay or a release for another attempt is stale', () => {
    const released = step(cfg, judgingA(), { kind: 'judgeReleased', batchId: ID_A, attempt: 1 }, 700).record;
    const again = step(cfg, released, { kind: 'judgeReleased', batchId: ID_A, attempt: 1 }, 800);
    expect(again.record).toEqual(released);
    expect(again.effects).toEqual([{ kind: 'trace', trace: { kind: 'stale', scope, input: 'judgeReleased', why: `no live judge attempt 1 for batch ${ID_A}` } }]);
    const other = step(cfg, judgingA(), { kind: 'judgeReleased', batchId: ID_A, attempt: 2 }, 700);
    expect(other.record).toEqual(judgingA());
  });

  test('a wait resets the failure count but not the attempt count', () => {
    const before: ScopeRecord = { ...judgingA(), batch: { window: windowA, phase: { phase: 'judging', attempt: 3, failures: 2, leaseUntil: 30_600, offerWait: true } } };
    const waited = step(cfg, before, { kind: 'judged', batchId: ID_A, attempt: 3, judgment: { route: 'wait' } }, 700).record;
    expect(waited.batch?.phase).toEqual({ phase: 'waiting', attempt: 3, until: 10_700 });
    const rejudged = step(cfg, waited, { kind: 'tick' }, 10_700).record;
    expect(rejudged.batch?.phase).toEqual({ phase: 'judging', attempt: 4, failures: 0, leaseUntil: 40_700, offerWait: false });
    const failed = step(cfg, rejudged, { kind: 'judgeFailed', batchId: ID_A, attempt: 4, message: 'busy', permanent: false }, 10_800).record;
    expect(failed.batch?.phase).toEqual({ phase: 'retrying', attempt: 4, failures: 1, at: 11_800, offerWait: false });
  });

  test('the retry budget exhausted dead-letters through the outbox at once', () => {
    const before: ScopeRecord = { ...judgingA(), batch: { window: windowA, phase: { phase: 'judging', attempt: 4, failures: 3, leaseUntil: 30_600, offerWait: true } } };
    const { record, effects } = step(cfg, before, { kind: 'judgeFailed', batchId: ID_A, attempt: 4, message: 'down', permanent: false }, 700);
    const letter: DeadLetter = { id: ID_A, scope, stage: 'judge', window: windowA, decision: null, message: 'down', permanent: false, attempts: 4, at: 700 };
    const inFlight: Delivery = { item: letterItem(letter), phase: { phase: 'inFlight', attempt: 1, failures: 0, leaseUntil: 10_700 } };
    expect(record).toEqual({
      ...base(),
      seen: ['a'],
      outbox: { decisions: [], deadLetters: [inFlight] },
      dueAt: 10_700,
    });
    expect(effects).toEqual([
      { kind: 'trace', trace: { kind: 'deadLettered', scope, letter } },
      { kind: 'deliver', scope, delivery: inFlight },
      { kind: 'trace', trace: { kind: 'delivering', scope, deliveryId: ID_A, attempt: 1 } },
    ]);
  });

  test('a permanent failure dead-letters without retrying', () => {
    const { record } = step(cfg, judgingA(), { kind: 'judgeFailed', batchId: ID_A, attempt: 1, message: 'refused', permanent: true }, 700);
    expect(record.batch).toBeNull();
    expect(record.outbox.deadLetters[0]).toMatchObject({ item: { kind: 'deadLetter', letter: { permanent: true, attempts: 1 } } });
  });

  test('the judge effect carries recent verdicts', () => {
    const before: ScopeRecord = { ...base(), buffer: { entries: [A], dropped: 0, gap: false, forcedAt: null }, recent: [{ id: ID_B, route: 'note', at: 1 }] };
    const { effects } = step(cfg, before, { kind: 'tick' }, 600);
    expect(effects[1]).toMatchObject({ kind: 'judge', recent: [{ id: ID_B, route: 'note', at: 1 }] });
  });
});

describe('delivery', () => {
  const inFlight = (attempt: number, failures: number, leaseUntil: number): ScopeRecord => ({
    ...base(),
    outbox: { decisions: [{ item: decisionItem(decisionA), phase: { phase: 'inFlight', attempt, failures, leaseUntil } }], deadLetters: [] },
    dueAt: leaseUntil,
  });

  test('an ack removes the delivery', () => {
    const { record, effects } = step(cfg, inFlight(1, 0, 10_700), { kind: 'delivered', deliveryId: ID_A, attempt: 1 }, 800);
    expect(record).toEqual({ ...base(), dueAt: null });
    expect(effects).toEqual([{ kind: 'trace', trace: { kind: 'delivered', scope, deliveryId: ID_A } }]);
  });

  test('an ack for another attempt is stale', () => {
    const before = inFlight(2, 1, 10_700);
    const { record, effects } = step(cfg, before, { kind: 'delivered', deliveryId: ID_A, attempt: 1 }, 800);
    expect(record).toEqual(before);
    expect(effects).toEqual([{ kind: 'trace', trace: { kind: 'stale', scope, input: 'delivered', why: `no live delivery attempt 1 for ${ID_A}` } }]);
  });

  test('a failure backs off, retries with the next attempt and is acked', () => {
    const failed = step(cfg, inFlight(1, 0, 10_700), { kind: 'deliveryFailed', deliveryId: ID_A, attempt: 1, message: 'closed', permanent: false }, 800);
    expect(failed.record).toEqual({
      ...base(),
      outbox: { decisions: [{ item: decisionItem(decisionA), phase: { phase: 'retrying', attempt: 1, failures: 1, at: 1800 } }], deadLetters: [] },
      dueAt: 1800,
    });
    expect(failed.effects).toEqual([{ kind: 'trace', trace: { kind: 'retrying', scope, stage: 'sink', id: ID_A, attempt: 1, at: 1800, message: 'closed' } }]);
    const retried = step(cfg, failed.record, { kind: 'tick' }, 1800);
    const second: Delivery = { item: decisionItem(decisionA), phase: { phase: 'inFlight', attempt: 2, failures: 1, leaseUntil: 11_800 } };
    expect(retried.record.outbox.decisions).toEqual([second]);
    expect(retried.effects[0]).toEqual({ kind: 'deliver', scope, delivery: second });
    const acked = step(cfg, retried.record, { kind: 'delivered', deliveryId: ID_A, attempt: 2 }, 1900);
    expect(acked.record.outbox).toEqual({ decisions: [], deadLetters: [] });
  });

  test('an expired lease is one failed attempt, not two', () => {
    const { record } = step(cfg, inFlight(1, 0, 10_700), { kind: 'tick' }, 10_700);
    expect(record.outbox.decisions).toEqual([{ item: decisionItem(decisionA), phase: { phase: 'retrying', attempt: 1, failures: 1, at: 11_700 } }]);
    const still = step(cfg, record, { kind: 'tick' }, 11_000);
    expect(still.record.outbox).toEqual(record.outbox);
    expect(still.effects).toEqual([]);
  });

  test('a delivery the runner never started is released: it retries at once, costs no failure, and the retry takes its attempt number', () => {
    const { record, effects } = step(cfg, inFlight(1, 0, 10_700), { kind: 'deliveryReleased', deliveryId: ID_A, attempt: 1 }, 800);
    expect(record).toEqual({ ...base(), outbox: { decisions: [{ item: decisionItem(decisionA), phase: { phase: 'retrying', attempt: 0, failures: 0, at: 800 } }], deadLetters: [] }, dueAt: 800 });
    expect(effects).toEqual([{ kind: 'trace', trace: { kind: 'released', scope, stage: 'sink', id: ID_A, attempt: 1 } }]);
    const retried = step(cfg, record, { kind: 'tick' }, 800);
    const sent: Delivery = { item: decisionItem(decisionA), phase: { phase: 'inFlight', attempt: 1, failures: 0, leaseUntil: 10_800 } };
    expect(retried.record.outbox.decisions).toEqual([sent]);
    expect(retried.effects[0]).toEqual({ kind: 'deliver', scope, delivery: sent });
    const replayed = step(cfg, record, { kind: 'deliveryReleased', deliveryId: ID_A, attempt: 1 }, 900);
    expect(replayed.record).toEqual(record);
    expect(replayed.effects).toEqual([{ kind: 'trace', trace: { kind: 'stale', scope, input: 'deliveryReleased', why: `no live delivery attempt 1 for ${ID_A}` } }]);
  });

  test('a released dead letter keeps its lane and its failures', () => {
    const letter: DeadLetter = { id: ID_A, scope, stage: 'sink', window: windowA, decision: decisionA, message: 'closed', permanent: false, attempts: 4, at: 800 };
    const before: ScopeRecord = { ...base(), outbox: { decisions: [], deadLetters: [{ item: letterItem(letter), phase: { phase: 'inFlight', attempt: 7, failures: 2, leaseUntil: 10_800 } }] } };
    const { record, effects } = step(cfg, before, { kind: 'deliveryReleased', deliveryId: ID_A, attempt: 7 }, 900);
    expect(record.outbox).toEqual({ decisions: [], deadLetters: [{ item: letterItem(letter), phase: { phase: 'retrying', attempt: 6, failures: 2, at: 900 } }] });
    expect(effects).toEqual([{ kind: 'trace', trace: { kind: 'released', scope, stage: 'deadLetter', id: ID_A, attempt: 7 } }]);
  });

  test('a backoff that has passed does not wait for the next tick', () => {
    const { record } = step(cfg, inFlight(1, 0, 10_700), { kind: 'deliveryFailed', deliveryId: ID_A, attempt: 1, message: 'closed', permanent: false }, 800);
    const { record: later } = step(cfg, record, { kind: 'tick' }, 5000);
    expect(later.outbox.decisions[0]).toMatchObject({ phase: { phase: 'inFlight', attempt: 2, leaseUntil: 15_000 } });
  });

  test('an exhausted decision moves to the dead-letter lane, keeps counting attempts and starts its backoff over', () => {
    const { record, effects } = step(cfg, inFlight(4, 3, 10_700), { kind: 'deliveryFailed', deliveryId: ID_A, attempt: 4, message: 'closed', permanent: false }, 800);
    const letter: DeadLetter = { id: ID_A, scope, stage: 'sink', window: windowA, decision: decisionA, message: 'closed', permanent: false, attempts: 4, at: 800 };
    expect(record.outbox).toEqual({ decisions: [], deadLetters: [{ item: letterItem(letter), phase: { phase: 'inFlight', attempt: 5, failures: 0, leaseUntil: 10_800 } }] });
    expect(effects[0]).toEqual({ kind: 'trace', trace: { kind: 'deadLettered', scope, letter } });
    const failedAgain = step(cfg, record, { kind: 'deliveryFailed', deliveryId: ID_A, attempt: 5, message: 'handler down', permanent: false }, 900);
    expect(failedAgain.record.outbox.deadLetters).toEqual([{ item: letterItem(letter), phase: { phase: 'retrying', attempt: 5, failures: 1, at: 1900 } }]);
  });

  test('a permanent sink failure dead-letters at once', () => {
    const { record } = step(cfg, inFlight(1, 0, 10_700), { kind: 'deliveryFailed', deliveryId: ID_A, attempt: 1, message: 'bad', permanent: true }, 800);
    expect(record.outbox.decisions).toEqual([]);
    expect(record.outbox.deadLetters[0]).toMatchObject({ item: { kind: 'deadLetter', letter: { permanent: true, attempts: 1 } }, phase: { phase: 'inFlight', attempt: 2, failures: 0 } });
  });

  test('a dead letter whose handler fails retries with capped backoff and is never dropped', () => {
    const letter: DeadLetter = { id: ID_A, scope, stage: 'sink', window: windowA, decision: decisionA, message: 'closed', permanent: false, attempts: 4, at: 800 };
    const stuck = (failures: number): ScopeRecord => ({
      ...base(),
      outbox: { decisions: [], deadLetters: [{ item: letterItem(letter), phase: { phase: 'inFlight', attempt: failures + 5, failures, leaseUntil: 10_800 } }] },
    });
    const { record } = step(cfg, stuck(0), { kind: 'deliveryFailed', deliveryId: ID_A, attempt: 5, message: 'down', permanent: true }, 900);
    expect(record.outbox.deadLetters).toEqual([{ item: letterItem(letter), phase: { phase: 'retrying', attempt: 5, failures: 1, at: 1900 } }]);
    const fourth = step(cfg, stuck(3), { kind: 'tick' }, 10_800);
    expect(fourth.record.outbox.deadLetters[0]?.phase).toEqual({ phase: 'retrying', attempt: 8, failures: 4, at: 42_800 });
    const capped = step(cfg, stuck(40), { kind: 'tick' }, 10_800);
    expect(capped.record.outbox.deadLetters[0]?.phase).toEqual({ phase: 'retrying', attempt: 45, failures: 41, at: 70_800 });
  });

  test('a second dead letter waits behind the one in flight, so two are never in flight', () => {
    const first: DeadLetter = { id: ID_B, scope, stage: 'judge', window: { ...windowA, id: ID_B }, decision: null, message: 'down', permanent: true, attempts: 1, at: 0 };
    const stuck: Delivery = { item: letterItem(first), phase: { phase: 'inFlight', attempt: 3, failures: 2, leaseUntil: 10_000 } };
    const before: ScopeRecord = { ...inFlight(4, 3, 10_700), outbox: { decisions: inFlight(4, 3, 10_700).outbox.decisions, deadLetters: [stuck] } };
    const escalated = step(cfg, before, { kind: 'deliveryFailed', deliveryId: ID_A, attempt: 4, message: 'closed', permanent: false }, 800);
    const letter: DeadLetter = { id: ID_A, scope, stage: 'sink', window: windowA, decision: decisionA, message: 'closed', permanent: false, attempts: 4, at: 800 };
    expect(escalated.record.outbox).toEqual({ decisions: [], deadLetters: [stuck, { item: letterItem(letter), phase: { phase: 'retrying', attempt: 4, failures: 0, at: 800 } }] });
    expect(escalated.effects.filter((e) => e.kind === 'deliver')).toEqual([]);
    expect(escalated.record.dueAt).toBe(10_000);
    const ticked = step(cfg, escalated.record, { kind: 'tick' }, 900);
    expect(ticked.record.outbox.deadLetters.map((d) => d.phase.phase)).toEqual(['inFlight', 'retrying']);
    expect(ticked.effects).toEqual([]);
  });

  test('a dead letter in flight does not hold back a decision behind it', () => {
    const letter: DeadLetter = { id: ID_B, scope, stage: 'judge', window: { ...windowA, id: ID_B }, decision: null, message: 'down', permanent: true, attempts: 1, at: 0 };
    const stuck: Delivery = { item: letterItem(letter), phase: { phase: 'inFlight', attempt: 3, failures: 2, leaseUntil: 10_000 } };
    const before: ScopeRecord = {
      ...base(),
      outbox: { decisions: [{ item: decisionItem(decisionA), phase: { phase: 'retrying', attempt: 0, failures: 0, at: 0 } }], deadLetters: [stuck] },
    };
    const { record, effects } = step(cfg, before, { kind: 'tick' }, 100);
    const sent: Delivery = { item: decisionItem(decisionA), phase: { phase: 'inFlight', attempt: 1, failures: 0, leaseUntil: 10_100 } };
    expect(record.outbox).toEqual({ decisions: [sent], deadLetters: [stuck] });
    expect(effects).toEqual([
      { kind: 'deliver', scope, delivery: sent },
      { kind: 'trace', trace: { kind: 'delivering', scope, deliveryId: ID_A, attempt: 1 } },
    ]);
    expect(record.dueAt).toBe(10_000);
    const acked = step(cfg, record, { kind: 'delivered', deliveryId: ID_A, attempt: 1 }, 200);
    expect(acked.record.outbox).toEqual({ decisions: [], deadLetters: [stuck] });
  });

  test('deliveries leave one at a time, oldest first', () => {
    const second: Decision = { ...decisionA, id: ID_B };
    const before: ScopeRecord = {
      ...base(),
      outbox: {
        decisions: [
          { item: decisionItem(decisionA), phase: { phase: 'retrying', attempt: 0, failures: 0, at: 0 } },
          { item: decisionItem(second), phase: { phase: 'retrying', attempt: 0, failures: 0, at: 0 } },
        ],
        deadLetters: [],
      },
    };
    const { record, effects } = step(cfg, before, { kind: 'tick' }, 100);
    expect(record.outbox.decisions[0]).toMatchObject({ item: { id: ID_A }, phase: { phase: 'inFlight', attempt: 1 } });
    expect(record.outbox.decisions[1]).toEqual(before.outbox.decisions[1]);
    expect(effects.filter((e) => e.kind === 'deliver')).toHaveLength(1);
    expect(record.dueAt).toBe(10_100);
  });
});

describe('flush', () => {
  test('marks the buffer forced with no effect, and the next tick settles it as forced with wait withheld', () => {
    const X = consequence('x', 100);
    const before: ScopeRecord = { ...base(), buffer: { entries: [X], dropped: 0, gap: false, forcedAt: null } };
    const flushed = step(cfg, before, { kind: 'flush' }, 200);
    expect(flushed.record).toEqual({ ...before, buffer: { ...before.buffer, forcedAt: 200 }, dueAt: 200 });
    expect(flushed.effects).toEqual([]);
    const { record } = step(cfg, flushed.record, { kind: 'tick' }, 200);
    expect(record.buffer).toEqual({ entries: [], dropped: 0, gap: false, forcedAt: null });
    expect(record.batch).toEqual({
      window: { id: ID_X, events: [X], reason: 'forced', waits: 0, dropped: 0, gap: false, settledAt: 200 },
      phase: { phase: 'judging', attempt: 1, failures: 0, leaseUntil: 30_200, offerWait: false },
    });
  });

  test('with nothing open it is a no-op', () => {
    const { record, effects } = step(cfg, base(), { kind: 'flush' }, 200);
    expect(record).toEqual(base());
    expect(effects).toEqual([{ kind: 'trace', trace: { kind: 'stale', scope, input: 'flush', why: 'nothing to settle' } }]);
  });

  test('while judging it waits for the verdict, then settles', () => {
    const before: ScopeRecord = { ...judgingA(), buffer: { entries: [consequence('x', 100)], dropped: 0, gap: false, forcedAt: null } };
    const flushed = step(cfg, before, { kind: 'flush' }, 200);
    expect(flushed.record).toEqual({ ...before, buffer: { ...before.buffer, forcedAt: 200 }, dueAt: 30_600 });
    const judged = step(cfg, flushed.record, { kind: 'judged', batchId: ID_A, attempt: 1, judgment: { route: 'skip' } }, 300);
    expect(judged.record.batch).toMatchObject({ window: { reason: 'forced' }, phase: { phase: 'judging' } });
  });

  test('while judging with nothing buffered it is recorded, and a wait verdict is judged again at once as forced with wait withheld', () => {
    const flushed = step(cfg, judgingA(), { kind: 'flush' }, 200);
    expect(flushed.record).toEqual({ ...judgingA(), buffer: { entries: [], dropped: 0, gap: false, forcedAt: 200 } });
    expect(flushed.effects).toEqual([]);
    const forced: Window = { ...windowA, reason: 'forced', waits: 1 };
    const { record, effects } = step(cfg, flushed.record, { kind: 'judged', batchId: ID_A, attempt: 1, judgment: { route: 'wait' } }, 300);
    expect(record).toEqual({
      ...base(),
      seen: ['a'],
      batch: { window: forced, phase: { phase: 'judging', attempt: 2, failures: 0, leaseUntil: 30_300, offerWait: false } },
      dueAt: 30_300,
    });
    expect(effects).toEqual([
      { kind: 'trace', trace: { kind: 'waiting', scope, batchId: ID_A, waits: 1, until: 10_300 } },
      { kind: 'judge', scope, batchId: ID_A, attempt: 2, window: forced, offered: NO_WAIT, recent: [], leaseUntil: 30_300 },
      { kind: 'trace', trace: { kind: 'judging', scope, batchId: ID_A, attempt: 2, offerWait: false } },
    ]);
  });

  test('while judging with nothing buffered, a verdict that routes spends the force', () => {
    const flushed = step(cfg, judgingA(), { kind: 'flush' }, 200);
    const { record } = step(cfg, flushed.record, { kind: 'judged', batchId: ID_A, attempt: 1, judgment: { route: 'queue' } }, 300);
    expect(record.buffer).toEqual({ entries: [], dropped: 0, gap: false, forcedAt: null });
    expect(record.batch).toBeNull();
    expect(record.outbox.decisions.map((d) => d.item.id)).toEqual([ID_A]);
  });

  test('judges a retrying batch at the next tick, keeping its attempts and failures', () => {
    const before: ScopeRecord = { ...base(), batch: { window: windowA, phase: { phase: 'retrying', attempt: 2, failures: 2, at: 4700, offerWait: true } } };
    const flushed = step(cfg, before, { kind: 'flush' }, 800);
    expect(flushed.record.dueAt).toBe(800);
    const { record } = step(cfg, flushed.record, { kind: 'tick' }, 800);
    expect(record.batch).toEqual({ window: { ...windowA, reason: 'forced' }, phase: { phase: 'judging', attempt: 3, failures: 2, leaseUntil: 30_800, offerWait: false } });
    expect(record.buffer.forcedAt).toBeNull();
  });

  test('judges a waiting batch at the next tick as forced', () => {
    const before: ScopeRecord = { ...base(), batch: { window: { ...windowA, waits: 1 }, phase: { phase: 'waiting', attempt: 1, until: 10_700 } } };
    const flushed = step(cfg, before, { kind: 'flush' }, 800);
    const { record } = step(cfg, flushed.record, { kind: 'tick' }, 800);
    expect(record.batch).toEqual({ window: { ...windowA, reason: 'forced', waits: 1 }, phase: { phase: 'judging', attempt: 2, failures: 0, leaseUntil: 30_800, offerWait: false } });
    expect(record.buffer.forcedAt).toBeNull();
  });
});

describe('validate', () => {
  const all = offeredRoutes(true);

  test('rejects a route that was not offered', () => {
    expect(validate({ route: 'wait' }, offeredRoutes(false))).toBeNull();
  });

  test('renormalises probabilities over the offered routes', () => {
    expect(validate({ route: 'queue', probabilities: { queue: 0.4, note: 0.4, wait: 0.9 } }, offeredRoutes(false))).toEqual({
      route: 'queue',
      probabilities: { interrupt: 0, queue: 0.5, note: 0.5, ask: 0, skip: 0, wait: 0 },
      predicates: {},
      confidence: null,
    });
  });

  test('keeps a confidence in [0, 1] and drops any other', () => {
    expect(validate({ route: 'queue', confidence: 0.83 }, all)?.confidence).toBe(0.83);
    expect(validate({ route: 'queue', confidence: 1.5 }, all)?.confidence).toBeNull();
    expect(validate({ route: 'queue' }, all)?.confidence).toBeNull();
  });

  test('gives the chosen route everything when no probability is usable', () => {
    expect(validate({ route: 'note', probabilities: { note: -1, ask: Number.NaN } }, all)?.probabilities).toEqual({ interrupt: 0, queue: 0, note: 1, ask: 0, skip: 0, wait: 0 });
  });

  test('drops predicates outside [0, 1] without costing the route', () => {
    expect(validate({ route: 'ask', predicates: { fine: 0.4, broken: 7 } }, all)?.predicates).toEqual({ fine: 0.4 });
  });
});

describe('ids', () => {
  test('windowId is FNV-1a 64 over the length-prefixed scope and event ids', () => {
    expect(windowId('s', ['a'])).toBe(ID_A);
    expect(windowId('s', ['a', 'b'])).toBe(ID_AB);
    expect(windowId('s', [])).toBe('4553f218180a9bb3');
  });

  test('the same ids in two scopes, and a joined id, give different windows', () => {
    expect(windowId('t', ['a'])).toBe('7047dd1ca8a7c2c8');
    expect(windowId('s', ['a\nb'])).toBe('9c6fc85e9100adb5');
  });

  test('step leaves its input untouched', () => {
    const before = judgingA();
    const snapshot = structuredClone(before);
    step(cfg, before, { kind: 'judged', batchId: ID_A, attempt: 1, judgment: { route: 'queue' } }, 700);
    expect(before).toEqual(snapshot);
  });
});
