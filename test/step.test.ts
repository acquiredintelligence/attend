import { describe, expect, test } from 'vitest';
import { deadline, hashIds, initialRecord, offeredRoutes, step, validate, type StepConfig } from '../src/step.js';
import type { Decision, Entry, Event, Kinded, ScopeRecord, Window } from '../src/types.js';
import { DEFAULT_POLICY } from '../src/types.js';

const kind = (e: Event): Kinded => {
  switch (e.name) {
    case 'close':
      return { kind: 'closing' };
    case 'say':
      return { kind: 'flush' };
    case 'drag':
      return { kind: 'continuous', stream: 'hand' };
    case 'look':
      return { kind: 'passive' };
    case 'out':
      return { kind: 'consequence' };
    default:
      return { kind: 'context' };
  }
};

const cfg: StepConfig = { kind, policy: DEFAULT_POLICY };
const scope = 's';

const base = (): ScopeRecord => initialRecord(scope);
const ev = (id: string, name: string): Event => ({ id, at: 0, name, data: {} });
const closing = (id: string, seenAt: number): Entry => ({ id, at: 0, name: 'close', data: {}, kind: 'closing', seenAt });
const consequence = (id: string, seenAt: number): Entry => ({ id, at: 0, name: 'out', data: {}, kind: 'consequence', seenAt });
const continuous = (id: string, seenAt: number): Entry => ({ id, at: 0, name: 'drag', data: {}, kind: 'continuous', stream: 'hand', seenAt });
const passive = (id: string, seenAt: number): Entry => ({ id, at: 0, name: 'look', data: {}, kind: 'passive', seenAt });
const flush = (id: string, seenAt: number): Entry => ({ id, at: 0, name: 'say', data: {}, kind: 'flush', seenAt });

const A = closing('a', 100);
const ID_A = 'af63dc4c8601ec8c';
const ID_B = 'af63df4c8601f1a5';
const ID_AB = 'e5beb1190415e670';

const windowA: Window = { id: ID_A, events: [A], reason: 'closing', waits: 0, dropped: 0, gap: false, settledAt: 600 };

const judgingA = (): ScopeRecord => ({
  ...base(),
  seen: ['a'],
  batch: { ...windowA, phase: 'judging', attempt: 1, leaseUntil: 30_600, offerWait: true },
  dueAt: 30_600,
});

const decisionA: Decision = {
  id: ID_A,
  scope,
  route: 'queue',
  probabilities: { interrupt: 0, queue: 1, note: 0, ask: 0, skip: 0, wait: 0 },
  predicates: {},
  window: windowA,
  at: 700,
};

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

  test('moves the cursor and a truncated batch marks the window with a gap', () => {
    const { record, effects } = step(cfg, base(), { kind: 'events', events: [ev('a', 'close')], cursor: 'c9', truncated: true }, 100);
    expect(record).toEqual({ ...base(), cursor: 'c9', seen: ['a'], buffer: { entries: [A], dropped: 0, gap: true, forcedAt: null }, dueAt: 600 });
    expect(effects).toEqual([
      { kind: 'trace', trace: { kind: 'gap', scope, cursor: 'c9' } },
      { kind: 'trace', trace: { kind: 'ingested', scope, count: 1, duplicates: 0 } },
    ]);
  });

  test('a gap with no open window marks the next one', () => {
    const { record } = step(cfg, base(), { kind: 'events', events: [], cursor: null, truncated: true }, 100);
    expect(record).toEqual({ ...base(), buffer: { entries: [], dropped: 0, gap: true, forcedAt: null } });
    const settled = step(cfg, step(cfg, record, { kind: 'events', events: [ev('a', 'close')] }, 100).record, { kind: 'tick' }, 600);
    expect(settled.record.batch).toMatchObject({ id: ID_A, gap: true });
  });

  test('a flush is due at once but still emits no effect on ingest', () => {
    const { record, effects } = step(cfg, base(), { kind: 'events', events: [ev('a', 'say')] }, 100);
    expect(record.dueAt).toBe(100);
    expect(effects).toEqual([{ kind: 'trace', trace: { kind: 'ingested', scope, count: 1, duplicates: 0 } }]);
  });

  test('overflow drops the oldest non-act first and never an act', () => {
    const small: StepConfig = { kind, policy: { ...DEFAULT_POLICY, maxEvents: 2 } };
    const { record, effects } = step(small, base(), { kind: 'events', events: [ev('a', 'close'), ev('b', 'out'), ev('c', 'close')] }, 100);
    expect(record.buffer).toEqual({ entries: [A, closing('c', 100)], dropped: 1, gap: false, forcedAt: null });
    expect(effects[0]).toEqual({ kind: 'trace', trace: { kind: 'dropped', scope, entries: [consequence('b', 100)], why: 'overflow' } });
  });

  test('the seen ring is bounded by seenCount, oldest out', () => {
    const tiny: StepConfig = { kind, policy: { ...DEFAULT_POLICY, seenCount: 2 } };
    const { record } = step(tiny, base(), { kind: 'events', events: [ev('a', 'out'), ev('b', 'out'), ev('c', 'out')] }, 100);
    expect(record.seen).toEqual(['b', 'c']);
  });
});

describe('deadline', () => {
  const buf = (entries: Entry[]) => ({ entries, dropped: 0, gap: false, forcedAt: null });

  test('closing grace', () => {
    expect(deadline(DEFAULT_POLICY, buf([A, consequence('b', 400)]))).toEqual({ at: 600, reason: 'closing' });
  });

  test('stream gap after the last gesture', () => {
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

  test('a forced window is due at the flush call', () => {
    expect(deadline(DEFAULT_POLICY, { ...buf([consequence('a', 100)]), forcedAt: 150 })).toEqual({ at: 150, reason: 'forced' });
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
    expect(record.batch).toMatchObject({ reason: 'flush', phase: 'judging', offerWait: false });
    expect(effects[1]).toMatchObject({ kind: 'judge', offered: ['interrupt', 'queue', 'note', 'ask', 'skip'] });
  });

  test('an act-less window never fires', () => {
    const before: ScopeRecord = { ...base(), buffer: { entries: [consequence('a', 100)], dropped: 0, gap: false, forcedAt: null } };
    const { record, effects } = step(cfg, before, { kind: 'tick' }, 50_000);
    expect(record).toEqual({ ...before, dueAt: null });
    expect(effects).toEqual([]);
  });

  test('an all-act window past maxEvents settles as overflow', () => {
    const small: StepConfig = { kind, policy: { ...DEFAULT_POLICY, maxEvents: 2 } };
    const entries = [closing('a', 100), closing('b', 100), closing('c', 100)];
    const before: ScopeRecord = { ...base(), buffer: { entries, dropped: 0, gap: false, forcedAt: null } };
    const { record } = step(small, before, { kind: 'tick' }, 100);
    expect(record.batch).toMatchObject({ reason: 'overflow', events: entries, phase: 'judging' });
  });

  test('a window older than maxWindowMs settles as age', () => {
    const entries = [continuous('a', 100), continuous('b', 59_800)];
    const before: ScopeRecord = { ...base(), buffer: { entries, dropped: 0, gap: false, forcedAt: null } };
    const { record } = step(cfg, before, { kind: 'tick' }, 60_100);
    expect(record.batch).toMatchObject({ reason: 'age', events: entries });
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
    expect(record).toEqual({
      ...base(),
      seen: ['a'],
      batch: { ...windowB, phase: 'judging', attempt: 1, leaseUntil: 30_700, offerWait: true },
      outbox: [{ id: ID_A, kind: 'decision', decision: decisionA, phase: 'inFlight', attempt: 1, leaseUntil: 10_700 }],
      recent: [{ id: ID_A, route: 'queue', at: 700 }],
      dueAt: 10_700,
    });
    expect(effects.map((e) => (e.kind === 'trace' ? e.trace.kind : e.kind))).toEqual(['judged', 'deliver', 'delivering', 'settled', 'judge', 'judging']);
    expect(effects[1]).toEqual({ kind: 'deliver', scope, delivery: { id: ID_A, kind: 'decision', decision: decisionA, phase: 'inFlight', attempt: 1, leaseUntil: 10_700 } });
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

  test('a wait holds the batch until the horizon', () => {
    const { record, effects } = step(cfg, judgingA(), { kind: 'judged', batchId: ID_A, attempt: 1, judgment: { route: 'wait' } }, 700);
    expect(record).toEqual({ ...base(), seen: ['a'], batch: { ...windowA, waits: 1, phase: 'waiting', until: 10_700 }, dueAt: 10_700 });
    expect(effects).toEqual([{ kind: 'trace', trace: { kind: 'waiting', scope, batchId: ID_A, waits: 1, until: 10_700 } }]);
  });

  test('a wait that was not offered is a retryable failure', () => {
    const before: ScopeRecord = { ...judgingA(), batch: { ...windowA, phase: 'judging', attempt: 1, leaseUntil: 30_600, offerWait: false } };
    const { record } = step(cfg, before, { kind: 'judged', batchId: ID_A, attempt: 1, judgment: { route: 'wait' } }, 700);
    expect(record.batch).toEqual({ ...windowA, phase: 'retrying', attempt: 1, at: 1700, offerWait: false });
  });

  test('the horizon judges the batch alone with wait withheld', () => {
    const before: ScopeRecord = { ...base(), seen: ['a'], batch: { ...windowA, waits: 1, phase: 'waiting', until: 10_700 }, dueAt: 10_700 };
    const { record, effects } = step(cfg, before, { kind: 'tick' }, 10_700);
    expect(record.batch).toEqual({ ...windowA, waits: 1, phase: 'judging', attempt: 1, leaseUntil: 40_700, offerWait: false });
    expect(effects[0]).toMatchObject({ kind: 'judge', attempt: 1, offered: ['interrupt', 'queue', 'note', 'ask', 'skip'] });
  });

  test('a new settle while waiting merges and judges at once, keeping waits', () => {
    const B = closing('b', 1000);
    const before: ScopeRecord = {
      ...base(),
      seen: ['a', 'b'],
      buffer: { entries: [B], dropped: 0, gap: false, forcedAt: null },
      batch: { ...windowA, waits: 1, phase: 'waiting', until: 10_700 },
      dueAt: 1500,
    };
    const { record, effects } = step(cfg, before, { kind: 'tick' }, 1500);
    expect(record.batch).toEqual({
      id: ID_AB,
      events: [A, B],
      reason: 'closing',
      waits: 1,
      dropped: 0,
      gap: false,
      settledAt: 1500,
      phase: 'judging',
      attempt: 1,
      leaseUntil: 31_500,
      offerWait: true,
    });
    expect(effects[0]).toEqual({ kind: 'trace', trace: { kind: 'settled', scope, batchId: ID_AB, reason: 'closing', count: 2, merged: true } });
  });

  test('wait is withheld once waits reaches maxWaits', () => {
    const B = closing('b', 1000);
    const before: ScopeRecord = {
      ...base(),
      buffer: { entries: [B], dropped: 0, gap: false, forcedAt: null },
      batch: { ...windowA, waits: 2, phase: 'waiting', until: 10_700 },
    };
    const { record } = step(cfg, before, { kind: 'tick' }, 1500);
    expect(record.batch).toMatchObject({ waits: 2, offerWait: false });
  });

  test('a retryable failure backs off, then retries with the next attempt', () => {
    const failed = step(cfg, judgingA(), { kind: 'judgeFailed', batchId: ID_A, attempt: 1, message: 'busy', permanent: false }, 700);
    expect(failed.record).toEqual({ ...base(), seen: ['a'], batch: { ...windowA, phase: 'retrying', attempt: 1, at: 1700, offerWait: true }, dueAt: 1700 });
    expect(failed.effects).toEqual([{ kind: 'trace', trace: { kind: 'retrying', scope, stage: 'judge', id: ID_A, attempt: 1, at: 1700, message: 'busy' } }]);
    const retried = step(cfg, failed.record, { kind: 'tick' }, 1700);
    expect(retried.record.batch).toEqual({ ...windowA, phase: 'judging', attempt: 2, leaseUntil: 31_700, offerWait: true });
    expect(retried.effects[0]).toMatchObject({ kind: 'judge', attempt: 2, offered: ['interrupt', 'queue', 'note', 'ask', 'skip', 'wait'] });
  });

  test('a new settle while retrying merges and judges at once, keeping attempts', () => {
    const B = closing('b', 1000);
    const before: ScopeRecord = {
      ...base(),
      buffer: { entries: [B], dropped: 0, gap: false, forcedAt: null },
      batch: { ...windowA, phase: 'retrying', attempt: 2, at: 4700, offerWait: true },
    };
    const { record } = step(cfg, before, { kind: 'tick' }, 1500);
    expect(record.batch).toMatchObject({ id: ID_AB, phase: 'judging', attempt: 3 });
  });

  test('an expired judge lease counts as one retryable failure', () => {
    const { record } = step(cfg, judgingA(), { kind: 'tick' }, 30_600);
    expect(record.batch).toEqual({ ...windowA, phase: 'retrying', attempt: 1, at: 31_600, offerWait: true });
  });

  test('the retry budget exhausted dead-letters through the outbox at once', () => {
    const before: ScopeRecord = { ...judgingA(), batch: { ...windowA, phase: 'judging', attempt: 4, leaseUntil: 30_600, offerWait: true } };
    const { record, effects } = step(cfg, before, { kind: 'judgeFailed', batchId: ID_A, attempt: 4, message: 'down', permanent: false }, 700);
    const letter = { id: ID_A, scope, stage: 'judge', window: windowA, decision: null, message: 'down', permanent: false, attempts: 4, at: 700 };
    expect(record).toEqual({
      ...base(),
      seen: ['a'],
      outbox: [{ id: ID_A, kind: 'deadLetter', letter, phase: 'inFlight', attempt: 1, leaseUntil: 10_700 }],
      dueAt: 10_700,
    });
    expect(effects).toEqual([
      { kind: 'trace', trace: { kind: 'deadLettered', scope, letter } },
      { kind: 'deliver', scope, delivery: { id: ID_A, kind: 'deadLetter', letter, phase: 'inFlight', attempt: 1, leaseUntil: 10_700 } },
      { kind: 'trace', trace: { kind: 'delivering', scope, deliveryId: ID_A, attempt: 1 } },
    ]);
  });

  test('a permanent failure dead-letters without retrying', () => {
    const { record } = step(cfg, judgingA(), { kind: 'judgeFailed', batchId: ID_A, attempt: 1, message: 'refused', permanent: true }, 700);
    expect(record.batch).toBeNull();
    expect(record.outbox[0]).toMatchObject({ kind: 'deadLetter', letter: { permanent: true, attempts: 1 } });
  });

  test('the judge effect carries recent verdicts', () => {
    const before: ScopeRecord = { ...base(), buffer: { entries: [A], dropped: 0, gap: false, forcedAt: null }, recent: [{ id: ID_B, route: 'note', at: 1 }] };
    const { effects } = step(cfg, before, { kind: 'tick' }, 600);
    expect(effects[1]).toMatchObject({ kind: 'judge', recent: [{ id: ID_B, route: 'note', at: 1 }] });
  });
});

describe('delivery', () => {
  const inFlight = (attempt: number, leaseUntil: number): ScopeRecord => ({
    ...base(),
    outbox: [{ id: ID_A, kind: 'decision', decision: decisionA, phase: 'inFlight', attempt, leaseUntil }],
    dueAt: leaseUntil,
  });

  test('an ack removes the delivery', () => {
    const { record, effects } = step(cfg, inFlight(1, 10_700), { kind: 'delivered', deliveryId: ID_A, attempt: 1 }, 800);
    expect(record).toEqual({ ...base(), dueAt: null });
    expect(effects).toEqual([{ kind: 'trace', trace: { kind: 'delivered', scope, deliveryId: ID_A } }]);
  });

  test('an ack for another attempt is stale', () => {
    const before = inFlight(2, 10_700);
    const { record, effects } = step(cfg, before, { kind: 'delivered', deliveryId: ID_A, attempt: 1 }, 800);
    expect(record).toEqual(before);
    expect(effects).toEqual([{ kind: 'trace', trace: { kind: 'stale', scope, input: 'delivered', why: `no live delivery attempt 1 for ${ID_A}` } }]);
  });

  test('a failure backs off, retries with the next attempt and is acked', () => {
    const failed = step(cfg, inFlight(1, 10_700), { kind: 'deliveryFailed', deliveryId: ID_A, attempt: 1, message: 'closed', permanent: false }, 800);
    expect(failed.record).toEqual({ ...base(), outbox: [{ id: ID_A, kind: 'decision', decision: decisionA, phase: 'retrying', attempt: 1, at: 1800 }], dueAt: 1800 });
    expect(failed.effects).toEqual([{ kind: 'trace', trace: { kind: 'retrying', scope, stage: 'sink', id: ID_A, attempt: 1, at: 1800, message: 'closed' } }]);
    const retried = step(cfg, failed.record, { kind: 'tick' }, 1800);
    expect(retried.record.outbox).toEqual([{ id: ID_A, kind: 'decision', decision: decisionA, phase: 'inFlight', attempt: 2, leaseUntil: 11_800 }]);
    expect(retried.effects[0]).toEqual({ kind: 'deliver', scope, delivery: { id: ID_A, kind: 'decision', decision: decisionA, phase: 'inFlight', attempt: 2, leaseUntil: 11_800 } });
    const acked = step(cfg, retried.record, { kind: 'delivered', deliveryId: ID_A, attempt: 2 }, 1900);
    expect(acked.record.outbox).toEqual([]);
  });

  test('an expired lease is one failed attempt, not two', () => {
    const { record } = step(cfg, inFlight(1, 10_700), { kind: 'tick' }, 10_700);
    expect(record.outbox).toEqual([{ id: ID_A, kind: 'decision', decision: decisionA, phase: 'retrying', attempt: 1, at: 11_700 }]);
    const still = step(cfg, record, { kind: 'tick' }, 11_000);
    expect(still.record.outbox).toEqual(record.outbox);
    expect(still.effects).toEqual([]);
  });

  test('a backoff that has passed does not wait for the next tick', () => {
    const { record } = step(cfg, inFlight(1, 10_700), { kind: 'deliveryFailed', deliveryId: ID_A, attempt: 1, message: 'closed', permanent: false }, 800);
    const { record: later } = step(cfg, record, { kind: 'tick' }, 5000);
    expect(later.outbox[0]).toMatchObject({ phase: 'inFlight', attempt: 2, leaseUntil: 15_000 });
  });

  test('an exhausted decision becomes a dead letter that carries it and keeps counting attempts', () => {
    const { record, effects } = step(cfg, inFlight(4, 10_700), { kind: 'deliveryFailed', deliveryId: ID_A, attempt: 4, message: 'closed', permanent: false }, 800);
    const letter = { id: ID_A, scope, stage: 'sink', window: windowA, decision: decisionA, message: 'closed', permanent: false, attempts: 4, at: 800 };
    expect(record.outbox).toEqual([{ id: ID_A, kind: 'deadLetter', letter, phase: 'inFlight', attempt: 5, leaseUntil: 10_800 }]);
    expect(effects[0]).toEqual({ kind: 'trace', trace: { kind: 'deadLettered', scope, letter } });
  });

  test('a permanent sink failure dead-letters at once', () => {
    const { record } = step(cfg, inFlight(1, 10_700), { kind: 'deliveryFailed', deliveryId: ID_A, attempt: 1, message: 'bad', permanent: true }, 800);
    expect(record.outbox[0]).toMatchObject({ kind: 'deadLetter', letter: { permanent: true, attempts: 1 }, phase: 'inFlight', attempt: 2 });
  });

  test('a dead letter whose handler fails retries with capped backoff and is never dropped', () => {
    const letter = { id: ID_A, scope, stage: 'sink' as const, window: windowA, decision: decisionA, message: 'closed', permanent: false, attempts: 4, at: 800 };
    const before: ScopeRecord = { ...base(), outbox: [{ id: ID_A, kind: 'deadLetter', letter, phase: 'inFlight', attempt: 5, leaseUntil: 10_800 }] };
    const { record } = step(cfg, before, { kind: 'deliveryFailed', deliveryId: ID_A, attempt: 5, message: 'down', permanent: true }, 900);
    expect(record.outbox).toEqual([{ id: ID_A, kind: 'deadLetter', letter, phase: 'retrying', attempt: 5, at: 60_900 }]);
    const later = step(cfg, { ...before, outbox: [{ id: ID_A, kind: 'deadLetter', letter, phase: 'inFlight', attempt: 40, leaseUntil: 10_800 }] }, { kind: 'tick' }, 10_800);
    expect(later.record.outbox[0]).toMatchObject({ phase: 'retrying', attempt: 40, at: 70_800 });
  });

  test('deliveries leave one at a time, oldest first', () => {
    const second: Decision = { ...decisionA, id: ID_B };
    const before: ScopeRecord = {
      ...base(),
      outbox: [
        { id: ID_A, kind: 'decision', decision: decisionA, phase: 'retrying', attempt: 0, at: 0 },
        { id: ID_B, kind: 'decision', decision: second, phase: 'retrying', attempt: 0, at: 0 },
      ],
    };
    const { record, effects } = step(cfg, before, { kind: 'tick' }, 100);
    expect(record.outbox[0]).toMatchObject({ id: ID_A, phase: 'inFlight', attempt: 1 });
    expect(record.outbox[1]).toEqual(before.outbox[1]);
    expect(effects.filter((e) => e.kind === 'deliver')).toHaveLength(1);
    expect(record.dueAt).toBe(10_100);
  });
});

describe('flush', () => {
  test('settles an act-less window as forced with wait withheld', () => {
    const X = consequence('x', 100);
    const before: ScopeRecord = { ...base(), buffer: { entries: [X], dropped: 0, gap: false, forcedAt: null } };
    const { record } = step(cfg, before, { kind: 'flush' }, 200);
    expect(record.buffer).toEqual({ entries: [], dropped: 0, gap: false, forcedAt: null });
    expect(record.batch).toEqual({ id: 'af63f54c86021707', events: [X], reason: 'forced', waits: 0, dropped: 0, gap: false, settledAt: 200, phase: 'judging', attempt: 1, leaseUntil: 30_200, offerWait: false });
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
    expect(judged.record.batch).toMatchObject({ reason: 'forced', phase: 'judging' });
  });

  test('judges a waiting batch now', () => {
    const before: ScopeRecord = { ...base(), batch: { ...windowA, waits: 1, phase: 'waiting', until: 10_700 } };
    const { record } = step(cfg, before, { kind: 'flush' }, 800);
    expect(record.batch).toEqual({ ...windowA, waits: 1, phase: 'judging', attempt: 1, leaseUntil: 30_800, offerWait: false });
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
    });
  });

  test('gives the chosen route everything when no probability is usable', () => {
    expect(validate({ route: 'note', probabilities: { note: -1, ask: Number.NaN } }, all)?.probabilities).toEqual({ interrupt: 0, queue: 0, note: 1, ask: 0, skip: 0, wait: 0 });
  });

  test('drops predicates outside [0, 1] without costing the route', () => {
    expect(validate({ route: 'ask', predicates: { fine: 0.4, broken: 7 } }, all)?.predicates).toEqual({ fine: 0.4 });
  });
});

describe('ids', () => {
  test('hashIds is FNV-1a 64 over the joined ids', () => {
    expect(hashIds(['a'])).toBe(ID_A);
    expect(hashIds(['a', 'b'])).toBe(ID_AB);
    expect(hashIds([])).toBe('cbf29ce484222325');
  });

  test('step leaves its input untouched', () => {
    const before = judgingA();
    const snapshot = structuredClone(before);
    step(cfg, before, { kind: 'judged', batchId: ID_A, attempt: 1, judgment: { route: 'queue' } }, 700);
    expect(before).toEqual(snapshot);
  });
});
