import assert from 'node:assert/strict';
import fc from 'fast-check';
import { test } from 'vitest';
import { apply, newOutput, nextDue, REDUCER_VERSION } from '../src/core/reducer.js';
import { assertInvariants, fromRecord, initialState, quiescent, toRecord } from '../src/core/state.js';
import { DEFAULT_POLICY } from '../src/core/types.js';
import type { ApplyConfig, Decision, Input, Kind, ScopeRecord, ScopeState, SettlePolicy, Window } from '../src/core/types.js';
import { estimateRenderedBytes, windowDeadline, windowEntries, windowSize } from '../src/core/window.js';
import { drive, planArbitrary } from './support/arbitraries.js';
import type { Plan } from './support/arbitraries.js';
import { deepFreeze } from './support/deep-freeze.js';

const policy: SettlePolicy = { ...DEFAULT_POLICY, closingGraceMs: 10, streamGapMs: 10, passiveQuietMs: 10,
  maxWindowMs: 100, maxEvents: 20, seenCount: 30, waitHorizonMs: 20, maxWaits: 2,
  judgeTimeoutMs: 100, deliveryTimeoutMs: 100, retryBackoffMs: [5], recentCount: 2 };
const config: ApplyConfig = { policy, decisions: 'lane', deadLetters: 'lane', traces: true };
const id = '3ecbd4f2ef91c78f';

function event(name = 'a', kind: Kind = 'closing'): Input {
  return { kind: 'events', events: [{ id: name, at: 0, name: 'e', data: {}, kind }] };
}

function fresh(p = policy): ScopeState {
  const state = initialState('s', p);
  assertInvariants(state, p);
  return state;
}

function restore(record: ScopeRecord, p = policy): ScopeState {
  const state = fromRecord(deepFreeze(record), p);
  assertInvariants(state, p);
  return state;
}

function run(state: ScopeState, input: Input, now = 0, cfg = config) {
  const out = newOutput();
  apply(cfg, state, deepFreeze({ input, now }), out);
  assertInvariants(state, cfg.policy);
  deepFreeze(out.effects);
  deepFreeze(out.appends);
  deepFreeze(toRecord(state));
  return out;
}

function judging(cfg = config): ScopeState {
  const state = fresh(cfg.policy);
  run(state, event(), 0, cfg);
  run(state, { kind: 'tick' }, 10, cfg);
  assert.equal(state.batch?.window.id, id);
  return state;
}

function expectedWindow(): Window {
  return { id, events: [{ id: 'a', at: 0, name: 'e', data: {}, kind: 'closing', seenAt: 0 }],
    reason: 'closing', waits: 0, dropped: 0, gap: false, settledAt: 10 };
}

function expectedDecision(): Decision {
  return { id, scope: 's', route: 'note', probabilities: { interrupt: 0, queue: 0, note: 1, ask: 0, skip: 0, wait: 0 },
    predicates: {}, confidence: null, window: expectedWindow(), at: 10 };
}

function snapshots(plan: Plan, predicate: (state: ScopeState) => boolean): ScopeRecord[] {
  const state = initialState(plan.scope, plan.policy);
  assertInvariants(state, plan.policy);
  const result: ScopeRecord[] = [];
  for (const item of drive(plan)) {
    run(state, item.input, item.now, { ...config, policy: plan.policy });
    if (predicate(state)) result.push(toRecord(state));
  }
  return result;
}

test('end is stale on an empty open window, including with a held batch', () => {
  for (const state of [fresh(), judging()]) {
    const before = toRecord(state);
    assert.deepStrictEqual(run(state, { kind: 'end' }, 10), { effects: [{ kind: 'trace', trace: {
      kind: 'stale', scope: 's', input: 'end', why: 'nothing to settle' } }], appends: [], changed: false });
    assert.deepStrictEqual(toRecord(state), before);
  }
});

test('end records its deadline, yields traces only and survives a snapshot round trip', () => {
  const state = fresh();
  run(state, event());
  assert.equal(Object.hasOwn(toRecord(state).buffer, 'endedAt'), false);
  assert.deepStrictEqual(run(state, { kind: 'end' }, 4), { effects: [], appends: [], changed: true });
  const record = toRecord(state);
  assert.deepStrictEqual(record, { version: 0, scope: 's', cursors: {}, seen: ['a'], buffer: {
    entries: [{ id: 'a', at: 0, name: 'e', data: {}, kind: 'closing', seenAt: 0 }], dropped: 0, gap: false, forcedAt: null, endedAt: 4 },
  batch: null, outbox: { decisions: [], deadLetters: [] }, recent: [], dueAt: 4 });
  assert.deepStrictEqual(toRecord(restore(record)), record);
  assert.deepStrictEqual(run(state, { kind: 'end' }, 4), { effects: [], appends: [], changed: false });
});

test('end settles with no grace and offers wait while flush and forced reasons take precedence', () => {
  for (const mode of ['end', 'flush-entry', 'forced'] as const) {
    const state = fresh();
    run(state, event('a', mode === 'flush-entry' ? 'flush' : 'closing'));
    if (mode === 'forced') run(state, { kind: 'flush' }, 3);
    run(state, { kind: 'end' }, 4);
    const out = run(state, { kind: 'tick' }, 4);
    const reason = mode === 'end' ? 'closing' : mode === 'forced' ? 'forced' : 'flush';
    assert.equal(state.batch?.window.reason, reason);
    assert.equal(state.open.endedAt, null);
    assert.deepStrictEqual(out.effects.map((e) => e.kind), ['trace', 'judge', 'trace']);
    const judge = out.effects.find((e) => e.kind === 'judge');
    assert.ok(judge);
    assert.deepStrictEqual(judge.offered, mode === 'end'
      ? ['interrupt', 'queue', 'note', 'ask', 'skip', 'wait'] : ['interrupt', 'queue', 'note', 'ask', 'skip']);
    assert.equal(judge.window.settledAt, 4);
  }
});

test('events after end join the same window', () => {
  const state = fresh();
  run(state, event());
  run(state, { kind: 'end' }, 4);
  run(state, event('b'), 5);
  assert.equal(state.dueAt, 4);
  run(state, { kind: 'tick' }, 5);
  assert.equal(state.batch?.window.id, 'f9df8ba2df127ca6');
  assert.deepStrictEqual(state.batch?.window.events.map((e) => e.id), ['a', 'b']);
  assert.equal(state.batch?.window.reason, 'closing');
});

test('pruning or shedding the last ended entry clears endedAt without rejudging a held window', () => {
  const state = fresh();
  run(state, event('b', 'context'), 11);
  run(state, { kind: 'end' }, 11);
  assert.deepStrictEqual(run(state, { kind: 'tick' }, 111).effects.map((e) => e.kind), ['trace']);
  assert.equal(state.open.endedAt, null);
  assert.equal(state.dueAt, null);
  const held = judging();
  run(held, { kind: 'judgeDeferred', batchId: id, attempt: 1, until: 1000 }, 10);
  run(held, event('b', 'context'), 11);
  run(held, { kind: 'end' }, 11);
  assert.deepStrictEqual(run(held, { kind: 'tick' }, 111).effects.map((e) => e.kind), ['trace']);
  assert.equal(held.open.endedAt, null);
  assert.equal(held.dueAt, 1000);
  assert.deepStrictEqual(run(held, { kind: 'tick' }, 112).effects, []);
  const shed = fresh();
  run(shed, event('b', 'context'));
  run(shed, { kind: 'end' }, 1);
  run(shed, { kind: 'events', events: [] }, 1, { ...config, policy: { ...policy, maxEvents: 0 } });
  assert.equal(shed.open.endedAt, null);
  assert.equal(shed.dueAt, null);
});

test('judgeDeferred preserves failures and spends no attempt until its due tick', () => {
  const state = judging();
  run(state, { kind: 'judgeFailed', batchId: id, attempt: 1, message: 'retry', permanent: false }, 10);
  run(state, { kind: 'tick' }, 15);
  assert.deepStrictEqual(run(state, { kind: 'judgeDeferred', batchId: id, attempt: 2, until: 40 }, 16), {
    effects: [{ kind: 'trace', trace: { kind: 'deferred', scope: 's', batchId: id, attempt: 2, until: 40 } }], appends: [], changed: true });
  assert.deepStrictEqual(state.batch?.phase, { phase: 'retrying', attempt: 1, failures: 1, at: 40, offerWait: true });
  assert.equal(state.dueAt, 40);
  assert.deepStrictEqual(run(state, { kind: 'tick' }, 39), { effects: [], appends: [], changed: false });
  const out = run(state, { kind: 'tick' }, 40);
  assert.deepStrictEqual(state.batch?.phase, { phase: 'judging', attempt: 2, failures: 1, leaseUntil: 240, offerWait: true });
  assert.equal(out.effects.find((e) => e.kind === 'judge')?.attempt, 2);
});

test('judgeDeferred rejects wrong attempts and wrong batch ids with exact stale traces', () => {
  for (const [batchId, attempt] of [[id, 2], ['wrong', 1]] as const) {
    const state = judging();
    assert.deepStrictEqual(run(state, { kind: 'judgeDeferred', batchId, attempt, until: 40 }, 10), {
      effects: [{ kind: 'trace', trace: { kind: 'stale', scope: 's', input: 'judgeDeferred',
        why: `no live judge attempt ${attempt} for batch ${batchId}` } }], appends: [], changed: false });
  }
});

test('size triggers at the byte budget and offers wait; absent or larger budgets do not', () => {
  for (const budget of [43, 44, undefined]) {
    const cfg = { ...config, policy: { ...policy, ...(budget === undefined ? {} : { maxWindowBytes: budget }) } };
    const state = fresh(cfg.policy);
    run(state, event('a', 'context'), 5, cfg);
    assert.equal(state.open.bytes, 43);
    assert.equal(state.dueAt, budget === 43 ? 5 : null);
    const out = run(state, { kind: 'tick' }, 5, cfg);
    if (budget === 43) {
      assert.equal(state.batch?.window.reason, 'size');
      assert.equal(out.effects.find((e) => e.kind === 'judge')?.offered.includes('wait'), true);
    } else assert.deepStrictEqual(out, { effects: [], appends: [], changed: false });
  }
  const cfg = { ...config, policy: { ...policy, maxWindowBytes: 1 } };
  const state = fresh(cfg.policy);
  run(state, event('a', 'flush'), 5, cfg);
  run(state, { kind: 'tick' }, 5, cfg);
  assert.equal(state.batch?.window.reason, 'flush');
});

test('a waiting size window keeps its reason when the horizon merges a smaller open window', () => {
  const cfg = { ...config, policy: { ...policy, maxWindowBytes: 86 } };
  const state = fresh(cfg.policy);
  run(state, event('a', 'context'), 0, cfg);
  run(state, event('b', 'context'), 0, cfg);
  run(state, { kind: 'tick' }, 0, cfg);
  assert.equal(state.batch?.window.reason, 'size');
  run(state, { kind: 'judged', batchId: 'f9df8ba2df127ca6', attempt: 1, judgment: { route: 'wait' } }, 0, cfg);
  run(state, event('c', 'context'), 1, cfg);
  assert.equal(state.open.bytes, 43);
  const out = run(state, { kind: 'tick' }, 20, cfg);
  assert.equal(state.batch?.window.reason, 'size');
  assert.deepStrictEqual(state.batch?.window.events.map((entry) => entry.id), ['a', 'b', 'c']);
  const judge = out.effects.find((effect) => effect.kind === 'judge');
  assert.ok(judge);
  assert.equal(judge.offered.includes('wait'), false);
  const settled = out.effects.find((effect) => effect.kind === 'trace' && effect.trace.kind === 'settled');
  assert.ok(settled?.kind === 'trace' && settled.trace.kind === 'settled');
  assert.equal(settled.trace.merged, true);
});

test('decisions append identical rows in lane and log modes', () => {
  const body = expectedDecision();
  for (const decisions of ['lane', 'log'] as const) {
    const cfg = { ...config, decisions };
    const state = judging(cfg);
    const out = run(state, { kind: 'judged', batchId: id, attempt: 1, judgment: { route: 'note' } }, 10, cfg);
    assert.deepStrictEqual(out.appends, [{ kind: 'decision', id, scope: 's', at: 10, route: 'note', body }]);
    assert.equal(state.outbox.decisions.length, decisions === 'lane' ? 1 : 0);
    assert.deepStrictEqual(out.effects.map((e) => e.kind), decisions === 'lane' ? ['trace', 'deliver', 'trace'] : ['trace']);
    assert.deepStrictEqual(state.recent, [{ id, route: 'note', at: 10 }]);
    assert.equal(state.batch, null);
  }
});

test('skip and wait do not append decisions', () => {
  for (const route of ['skip', 'wait'] as const) {
    const state = judging();
    const out = run(state, { kind: 'judged', batchId: id, attempt: 1, judgment: { route } }, 10);
    assert.deepStrictEqual(out.appends, []);
    assert.deepStrictEqual(out.effects, [{ kind: 'trace', trace: route === 'skip'
      ? { kind: 'judged', scope: 's', batchId: id, route: 'skip' }
      : { kind: 'waiting', scope: 's', batchId: id, waits: 1, until: 30 } }]);
  }
});

test('judge dead letters append a null route and obey both dead-letter modes', () => {
  for (const deadLetters of ['lane', 'log'] as const) {
    const cfg = { ...config, deadLetters };
    const state = judging(cfg);
    const out = run(state, { kind: 'judgeFailed', batchId: id, attempt: 1, message: 'bad', permanent: true }, 10, cfg);
    const letter = { id, scope: 's', stage: 'judge', window: expectedWindow(), decision: null, message: 'bad', permanent: true, attempts: 1, at: 10 };
    assert.deepStrictEqual(out.appends, [{ kind: 'deadLetter', id, scope: 's', at: 10, route: null, body: letter }]);
    assert.equal(state.outbox.deadLetters.length, deadLetters === 'lane' ? 1 : 0);
    assert.equal(out.effects.some((e) => e.kind === 'deliver'), deadLetters === 'lane');
    assert.equal(state.batch, null);
    assert.equal(state.known.get('a'), 1);
  }
});

test('sink dead letters append the decision route and never append another dead letter on retry', () => {
  for (const deadLetters of ['lane', 'log'] as const) {
    const cfg = { ...config, deadLetters };
    const state = judging(cfg);
    run(state, { kind: 'judged', batchId: id, attempt: 1, judgment: { route: 'note' } }, 10, cfg);
    const out = run(state, { kind: 'deliveryFailed', deliveryId: id, attempt: 1, message: 'bad', permanent: true }, 10, cfg);
    const letter = { id, scope: 's', stage: 'sink', window: expectedWindow(), decision: expectedDecision(), message: 'bad', permanent: true, attempts: 1, at: 10 };
    assert.deepStrictEqual(out.appends, [{ kind: 'deadLetter', id, scope: 's', at: 10, route: 'note', body: letter }]);
    assert.equal(state.outbox.decisions.length, 0);
    assert.equal(state.outbox.deadLetters.length, deadLetters === 'lane' ? 1 : 0);
    if (deadLetters === 'lane') {
      const retry = run(state, { kind: 'deliveryFailed', deliveryId: id, attempt: 2, message: 'again', permanent: true }, 10, cfg);
      assert.deepStrictEqual(retry.appends, []);
      assert.deepStrictEqual(state.outbox.deadLetters[0]?.phase, { phase: 'retrying', attempt: 2, failures: 1, at: 15 });
    }
  }
});

test('traces false preserves state, appends and non-trace effects', () => {
  const loud = fresh();
  const quiet = fresh();
  for (const [input, now] of [[event(), 0], [{ kind: 'end' }, 4], [{ kind: 'tick' }, 4],
    [{ kind: 'judged', batchId: id, attempt: 1, judgment: { route: 'note' } }, 4]] satisfies [Input, number][]) {
    const a = run(loud, input, now);
    const b = run(quiet, input, now, { ...config, traces: false });
    assert.deepStrictEqual(toRecord(quiet), toRecord(loud));
    assert.deepStrictEqual(b.effects, a.effects.filter((e) => e.kind !== 'trace'));
    assert.deepStrictEqual(b.appends, a.appends);
    assert.equal(b.changed, a.changed);
  }
});

test('changed distinguishes duplicates, stale results, repeated cursors and repeated flags', () => {
  const state = fresh();
  assert.equal(run(state, { kind: 'tick' }).changed, false);
  assert.equal(run(state, { kind: 'judgeReleased', batchId: 'missing', attempt: 1 }).changed, false);
  assert.equal(run(state, event()).changed, true);
  assert.equal(run(state, event()).changed, false);
  const cursor: Input = { kind: 'events', events: [], source: 'source', cursor: null };
  assert.equal(run(state, cursor).changed, true);
  assert.equal(run(state, cursor).changed, false);
  assert.equal(run(state, { kind: 'events', events: [], truncated: true }).changed, true);
  assert.equal(run(state, { kind: 'events', events: [], truncated: true }).changed, false);
  assert.equal(run(state, { kind: 'flush' }, 1).changed, true);
  assert.equal(run(state, { kind: 'flush' }, 1).changed, false);
  const out = newOutput();
  apply(config, state, { input: { kind: 'flush' }, now: 2 }, out);
  assertInvariants(state, policy);
  apply(config, state, { input: { kind: 'events', events: [] }, now: 2 }, out);
  assertInvariants(state, policy);
  assert.equal(out.changed, true);
});

test('quiescent requires no open entries, batch, lane deliveries or deadline', () => {
  const empty = fresh();
  assert.equal(quiescent(empty), true);
  run(empty, event('a', 'context'));
  assert.equal(empty.dueAt, null);
  assert.equal(quiescent(empty), false);
  const held = judging();
  assert.equal(quiescent(held), false);
  run(held, { kind: 'judged', batchId: id, attempt: 1, judgment: { route: 'note' } }, 10);
  assert.equal(quiescent(held), false);
  run(held, { kind: 'delivered', deliveryId: id, attempt: 1 }, 10);
  assert.equal(quiescent(held), true);
  empty.open.entries.length = 0;
  empty.dueAt = 1;
  assert.equal(quiescent(empty), false);
});

test('snapshots, end and settle use only live entries after shedding', () => {
  const cfg = { ...config, policy: { ...policy, maxEvents: 3 } };
  const state = fresh(cfg.policy);
  for (const id of ['a', 'b', 'c', 'd']) run(state, event(id, 'consequence'), 0, cfg);
  assert.ok(state.open.start > 0);
  assert.equal(windowSize(state.open), 3);
  assert.deepStrictEqual(toRecord(state).buffer.entries.map((e) => e.id), ['b', 'c', 'd']);
  assert.equal(quiescent(state), false);
  assert.deepStrictEqual(run(state, { kind: 'end' }, 1, cfg), { effects: [], appends: [], changed: true });
  assert.equal(state.open.endedAt, 1);
  assert.equal(state.dueAt, 1);
  const entries = state.open.entries;
  run(state, { kind: 'flush' }, 1, cfg);
  const out = run(state, { kind: 'tick' }, 1, cfg);
  const judge = out.effects.find((e) => e.kind === 'judge');
  assert.ok(judge);
  assert.deepStrictEqual(judge.window.events.map((e) => e.id), ['b', 'c', 'd']);
  assert.equal(judge.window.events, entries);
  assert.equal(judge.window.dropped, 1);
  assert.equal(state.open.start, 0);
  assert.equal(windowSize(state.open), 0);
  assert.deepStrictEqual(windowEntries(state.open), []);
  assert.equal(quiescent(state), false);
  run(state, { kind: 'judged', batchId: judge.batchId, attempt: 1, judgment: { route: 'skip' } }, 1, cfg);
  assert.equal(quiescent(state), true);
  assert.deepStrictEqual(run(state, { kind: 'end' }, 1, cfg), { effects: [{ kind: 'trace', trace: {
    kind: 'stale', scope: 's', input: 'end', why: 'nothing to settle' } }], appends: [], changed: false });
});

test('assertInvariants identifies corrupt counts, rings, summaries, sizes, hashes, deadlines and lanes', () => {
  const corruptions: [string, (state: ScopeState) => void][] = [
    ['I1:', (s) => s.known.set('a', 99)],
    ['I1:', (s) => s.known.set('extra', 1)],
    ['I1:', (s) => { s.open.entries.push(s.open.entries[0]!); }],
    ['I2:', (s) => { s.seen.size = 31; }],
    ['I2:', (s) => { s.seen.head = -1; }],
    ['I3:', (s) => { s.open.summary.lastClosingAt = 99; }],
    ['I4:', (s) => { s.open.sizes[0] = 99; }],
    ['I5:', (s) => { s.open.hash.lo = 0; }],
    ['I6:', (s) => { s.dueAt = 99; }],
  ];
  for (const [prefix, corrupt] of corruptions) {
    const state = fresh();
    run(state, event());
    corrupt(state);
    assert.throws(() => assertInvariants(state, policy), (e: unknown) => e instanceof Error && e.message.startsWith(prefix));
  }
  const held = judging();
  assert.ok(held.batch?.hash);
  held.batch.hash.lo = 0;
  assert.throws(() => assertInvariants(held, policy), /^Error: I5: held hash/);
  const empty = fresh();
  empty.open.endedAt = 1;
  assert.throws(() => assertInvariants(empty, policy), /^Error: I3: endedAt on an empty window/);
  const lane = judging();
  run(lane, { kind: 'judged', batchId: id, attempt: 1, judgment: { route: 'note' } }, 10);
  lane.outbox.decisions.push(lane.outbox.decisions[0]!);
  lane.dueAt = nextDue(policy, lane);
  assert.throws(() => assertInvariants(lane, policy), /^Error: outbox: more than one in flight/);
  assert.equal(REDUCER_VERSION, 1);
});

test('fromRecord accepts frozen oracle snapshots without changing them', () => {
  fc.assert(fc.property(planArbitrary({ maxSteps: 60 }), (plan) => {
    for (const item of drive(plan)) {
      const record = deepFreeze(item.after.record);
      const before = JSON.stringify(record);
      const state = restore(record, plan.policy);
      assert.deepStrictEqual(toRecord(state), record);
      run(state, { kind: 'events', events: [], source: 'copy', cursor: 'changed' }, item.now, { ...config, policy: plan.policy });
      assert.equal(JSON.stringify(record), before);
    }
  }), { seed: 0xc004, numRuns: 40 });
});

test('end and flush settle the same reachable events with their own wait rules', () => {
  let compared = 0;
  let skipped = 0;
  fc.assert(fc.property(planArbitrary({ maxSteps: 80 }), fc.nat(), (plan, index) => {
    const candidates = snapshots(plan, (s) => windowSize(s.open) > 0 && s.batch?.phase.phase !== 'judging');
    if (candidates.length === 0) return;
    const record = candidates[index % candidates.length]!;
    const now = record.buffer.entries.at(-1)!.seenAt;
    const cfg = { ...config, policy: plan.policy };
    const ended = restore(record, plan.policy);
    const flushed = restore(record, plan.policy);
    run(ended, { kind: 'end' }, now, cfg);
    const deadline = windowDeadline(plan.policy, ended.open);
    assert.ok(deadline);
    run(flushed, { kind: 'flush' }, now, cfg);
    const endOut = run(ended, { kind: 'tick' }, now, cfg);
    const flushOut = run(flushed, { kind: 'tick' }, now, cfg);
    const a = endOut.effects.find((e) => e.kind === 'judge');
    const b = flushOut.effects.find((e) => e.kind === 'judge');
    if (!a || !b || !endOut.effects.some((e) => e.kind === 'trace' && e.trace.kind === 'settled')) { skipped += 1; return; }
    compared += 1;
    assert.deepStrictEqual(a.window.events, b.window.events);
    assert.equal(a.window.id, b.window.id);
    assert.equal(a.window.dropped, b.window.dropped);
    assert.equal(a.window.gap, b.window.gap);
    assert.ok(b.window.reason === 'forced' || b.window.reason === 'flush');
    assert.equal(b.offered.includes('wait'), false);
    const expectedReason = a.window.events.some((e) => e.kind === 'flush') ? 'flush'
      : record.buffer.forcedAt !== null || record.batch?.window.reason === 'forced' ? 'forced' : deadline.reason;
    assert.equal(a.window.reason, expectedReason);
    assert.equal(a.offered.includes('wait'), expectedReason !== 'flush' && expectedReason !== 'forced' && a.window.waits < plan.policy.maxWaits);
  }), { seed: 0xe0d, numRuns: 150 });
  assert.ok(compared > 40);
  assert.ok(compared > skipped);
  console.log(JSON.stringify({ endVsFlush: { compared, skipped } }));
});

test('judgeDeferred and judgeReleased converge on the same attempt at the later tick', () => {
  let compared = 0;
  fc.assert(fc.property(planArbitrary({ maxSteps: 80 }), fc.nat(), fc.integer({ min: 0, max: 20 }), (plan, index, delay) => {
    const candidates = snapshots(plan, (s) => s.batch?.phase.phase === 'judging');
    if (candidates.length === 0) return;
    const record = candidates[index % candidates.length]!;
    const b = record.batch!;
    const now = b.window.settledAt;
    const cfg = { ...config, policy: plan.policy };
    const deferred = restore(record, plan.policy);
    const released = restore(record, plan.policy);
    run(deferred, { kind: 'judgeDeferred', batchId: b.window.id, attempt: b.phase.attempt, until: now + delay }, now, cfg);
    run(released, { kind: 'judgeReleased', batchId: b.window.id, attempt: b.phase.attempt }, now, cfg);
    const a = toRecord(deferred);
    const c = toRecord(released);
    assert.ok(a.batch?.phase.phase === 'retrying');
    assert.ok(c.batch?.phase.phase === 'retrying');
    c.batch = { ...c.batch, phase: { ...c.batch.phase, at: now + delay } };
    c.dueAt = a.dueAt;
    assert.deepStrictEqual(a, c);
    const x = run(deferred, { kind: 'tick' }, now + delay, cfg);
    const y = run(released, { kind: 'tick' }, now + delay, cfg);
    assert.deepStrictEqual(toRecord(deferred), toRecord(released));
    assert.deepStrictEqual(x.effects.filter((e) => e.kind === 'judge'), y.effects.filter((e) => e.kind === 'judge'));
    assert.ok(x.effects.some((e) => e.kind === 'judge'));
    compared += 1;
  }), { seed: 0xdefe4, numRuns: 100 });
  assert.ok(compared > 40);
});

test('size settles at the byte deadline or inherits a held reason, and absent or infinite budgets match the oracle', () => {
  let sizes = 0;
  fc.assert(fc.property(planArbitrary({ maxSteps: 100 }), fc.integer({ min: 1, max: 500 }), (plan, budget) => {
    const sized = initialState(plan.scope, { ...plan.policy, maxWindowBytes: budget });
    const infinite = initialState(plan.scope, { ...plan.policy, maxWindowBytes: Infinity });
    const absent = initialState(plan.scope, plan.policy);
    const p = { ...plan.policy, maxWindowBytes: budget };
    assertInvariants(sized, p);
    assertInvariants(infinite, { ...plan.policy, maxWindowBytes: Infinity });
    assertInvariants(absent, plan.policy);
    for (const item of drive(plan)) {
      const held = sized.batch;
      const open = new Set(windowEntries(sized.open).map((e) => e.id));
      const out = run(sized, item.input, item.now, { ...config, policy: p });
      for (const effect of out.effects) if (effect.kind === 'judge' && effect.window.reason === 'size'
        && out.effects.some((e) => e.kind === 'trace' && e.trace.kind === 'settled' && e.trace.reason === 'size')) {
        const openBytes = effect.window.events.filter((e) => open.has(e.id)).reduce((n, e) => n + estimateRenderedBytes(e), 0);
        // A wait horizon inherits the held reason even when the open part is below budget.
        if (openBytes < budget) {
          assert.equal(held?.window.reason, 'size');
          assert.ok(held?.phase.phase === 'waiting' && held.phase.until <= item.now);
          assert.equal(effect.offered.includes('wait'), false);
        }
        sizes += 1;
      }
      if (sized.batch?.phase.phase !== 'judging' && windowSize(sized.open) > 0 && sized.open.bytes >= budget) {
        assert.ok(sized.dueAt !== null && sized.dueAt <= windowEntries(sized.open).at(-1)!.seenAt);
      }
      for (const [state, p2] of [[absent, plan.policy], [infinite, { ...plan.policy, maxWindowBytes: Infinity }]] as const) {
        const regular = run(state, item.input, item.now, { ...config, policy: p2 });
        assert.deepStrictEqual(toRecord(state), item.after.record);
        assert.deepStrictEqual(regular.effects, item.after.effects);
      }
    }
  }), { seed: 0x512e, numRuns: 100 });
  assert.ok(sizes > 20);
});

test('log modes preserve decisions and judge dead letters while keeping both lanes empty', () => {
  let decisions = 0;
  let letters = 0;
  fc.assert(fc.property(planArbitrary({ maxSteps: 100 }), (plan) => {
    const lane = initialState(plan.scope, plan.policy);
    const log = initialState(plan.scope, plan.policy);
    assertInvariants(lane, plan.policy);
    assertInvariants(log, plan.policy);
    for (const item of drive(plan)) {
      const a = run(lane, item.input, item.now, { ...config, policy: plan.policy });
      const b = run(log, item.input, item.now, { ...config, policy: plan.policy, decisions: 'log', deadLetters: 'log' });
      const rows = (out: typeof a) => out.appends.filter((row) => row.kind === 'decision' || row.body.stage === 'judge');
      assert.deepStrictEqual(rows(b), rows(a));
      decisions += b.appends.filter((row) => row.kind === 'decision').length;
      letters += b.appends.filter((row) => row.kind === 'deadLetter').length;
      assert.deepStrictEqual(log.outbox, { decisions: [], deadLetters: [] });
      assert.equal(b.effects.some((e) => e.kind === 'deliver'), false);
      const left = toRecord(lane);
      const right = toRecord(log);
      for (const key of ['cursors', 'seen', 'buffer', 'batch', 'recent'] as const) assert.deepStrictEqual(right[key], left[key]);
    }
  }), { seed: 0x109, numRuns: 100 });
  assert.ok(decisions > 10);
  assert.ok(letters > 10);
});
