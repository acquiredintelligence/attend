import assert from 'node:assert/strict';
import fc from 'fast-check';
import { test } from 'vitest';
import { apply, newOutput, REDUCER_VERSION } from '../src/core/reducer.js';
import { assertInvariants, fromRecord, initialState, toRecord } from '../src/core/state.js';
import type { Input, LoggedInput, ScopeRecord, ScopeState, SettlePolicy } from '../src/core/types.js';
import { drive, planArbitrary, policyArbitrary } from './support/arbitraries.js';
import type { Driven } from './support/arbitraries.js';
import { deepFreeze } from './support/deep-freeze.js';

type Cadence = 'always' | 'never' | 0.25 | 1 | 4;
interface Snapshot {
  version: number;
  reducerVersion: number;
  record: ScopeRecord;
  policy: SettlePolicy;
  segment: number;
}
interface Commit {
  logged: LoggedInput;
  segment: number;
}

const RUNS = 150;

function withBudget(policy: SettlePolicy, budget: number | undefined): SettlePolicy {
  return budget === undefined ? policy : { ...policy, maxWindowBytes: budget };
}

function json<T>(value: T): T {
  return JSON.parse(JSON.stringify(value));
}

function retarget(item: Driven, state: ScopeState): Input {
  const input = item.input;
  if (!item.edges.includes('result:live')) return input;
  if ('batchId' in input && state.batch?.phase.phase === 'judging') {
    return { ...input, batchId: state.batch.window.id, attempt: state.batch.phase.attempt };
  }
  if ('deliveryId' in input) {
    const head = [state.outbox.decisions[0], state.outbox.deadLetters[0]].find((d) => d?.phase.phase === 'inFlight');
    if (head?.phase.phase === 'inFlight') return { ...input, deliveryId: head.item.id, attempt: head.phase.attempt };
  }
  return input;
}

test('recovers every committed write under all snapshot cadences and policy changes', () => {
  const cadences: Cadence[] = ['always', 'never', 0.25, 1, 4];
  const counts = Object.fromEntries(cadences.map((cadence) => [cadence, { histories: 0, policyChanges: 0, writes: 0, snapshots: 0, replays: 0, replayedInputs: 0, sizeSettles: 0, ends: 0, deferrals: 0 }]));
  const budget = fc.option(fc.integer({ min: 40, max: 400 }), { nil: undefined });
  const change = fc.option(fc.record({ at: fc.nat(), policy: policyArbitrary, budget }), { nil: undefined });
  fc.assert(fc.property(planArbitrary({ maxSteps: 120 }), budget, change, fc.array(fc.nat(), { minLength: 1, maxLength: 80 }),
    (plan, firstBudget, next, choices) => {
      const history = drive(plan);
      for (const cadence of cadences) {
        const stats = counts[cadence]!;
        stats.histories += 1;
        let policy = withBudget(plan.policy, firstBudget);
        const state = initialState(plan.scope, policy);
        assertInvariants(state, policy);
        let segment = 0;
        let seenBound = policy.seenCount;
        let pendingPolicy = false;
        const commits: Commit[] = [];
        const snapshots: Snapshot[] = [];
        const ends = new Map<number, ScopeRecord>();
        let latest: Snapshot = { version: 0, reducerVersion: REDUCER_VERSION, record: json(toRecord(state)), policy, segment };
        let tailBytes = 0;
        let tailInputs = 0;
        let snapshotBytes = JSON.stringify(latest.record).length;
        const replay = (snapshot: Snapshot, through: number, expected: ScopeRecord): void => {
          assert.equal(snapshot.reducerVersion, 1);
          const restored = fromRecord(deepFreeze(json(snapshot.record)), snapshot.policy);
          let bound = Math.max(snapshot.policy.seenCount, restored.seen.size);
          assertInvariants(restored, { ...snapshot.policy, seenCount: bound });
          for (let i = snapshot.version; i < through; i += 1) {
            const commit = commits[i]!;
            assert.equal(commit.segment, snapshot.segment);
            const out = newOutput();
            apply({ policy: snapshot.policy, decisions: 'lane', deadLetters: 'lane', traces: true }, restored, deepFreeze(json(commit.logged)), out);
            if (commit.logged.input.kind === 'events') bound = snapshot.policy.seenCount;
            assertInvariants(restored, { ...snapshot.policy, seenCount: bound });
            assert.equal(out.changed, true);
            restored.version += 1;
            stats.replayedInputs += 1;
          }
          assert.deepStrictEqual(toRecord(restored), expected);
          stats.replays += 1;
        };
        const write = (input: Input, now: number): void => {
          const logged = deepFreeze(json({ input, now }));
          const out = newOutput();
          apply({ policy, decisions: 'lane', deadLetters: 'lane', traces: true }, state, logged, out);
          if (input.kind === 'events') seenBound = policy.seenCount;
          // A lower seenCount takes effect only on the next events input.
          assertInvariants(state, { ...policy, seenCount: Math.max(seenBound, policy.seenCount) });
          if (!out.changed) return;
          for (const effect of out.effects) {
            if (effect.kind !== 'trace') continue;
            if (effect.trace.kind === 'settled' && effect.trace.reason === 'size') stats.sizeSettles += 1;
            if (effect.trace.kind === 'deferred') stats.deferrals += 1;
          }
          if (input.kind === 'end') stats.ends += 1;
          state.version += 1;
          commits.push({ logged, segment });
          stats.writes += 1;
          const bytes = JSON.stringify(logged).length;
          const snapshot = pendingPolicy || cadence === 'always' || (cadence !== 'never'
            && (tailBytes + bytes >= cadence * Math.max(snapshotBytes, 4096) || tailInputs + 1 > 4096));
          if (snapshot) {
            latest = { version: state.version, reducerVersion: REDUCER_VERSION, record: json(toRecord(state)), policy: json(policy), segment };
            snapshots.push(latest);
            snapshotBytes = JSON.stringify(latest.record).length;
            tailBytes = 0;
            tailInputs = 0;
            stats.snapshots += 1;
            pendingPolicy = false;
          } else {
            tailBytes += bytes;
            tailInputs += 1;
          }
          const expected = toRecord(state);
          replay(latest, state.version, expected);
          const earlier = snapshots.filter((s) => s.segment === segment);
          if (segment === 0) earlier.unshift({ version: 0, reducerVersion: REDUCER_VERSION, record: json(toRecord(initialState(plan.scope, latest.policy))), policy: latest.policy, segment: 0 });
          const chosen = earlier[choices[state.version % choices.length]! % earlier.length];
          assert.ok(chosen);
          replay(chosen, state.version, expected);
          deepFreeze(out.effects);
          deepFreeze(out.appends);
          deepFreeze(expected);
        };
        const changeAt = next === undefined ? -1 : next.at % (history.length + 1);
        for (let i = 0; i <= history.length; i += 1) {
          if (next !== undefined && i === changeAt) {
            ends.set(segment, json(toRecord(state)));
            segment += 1;
            policy = withBudget(next.policy, next.budget);
            stats.policyChanges += 1;
            seenBound = Math.max(seenBound, state.seen.size);
            pendingPolicy = true;
          }
          const item = history[i];
          const now = item?.now ?? plan.start + 100_000;
          const choice = choices[i % choices.length]!;
          if (choice % 5 === 0) write({ kind: 'end' }, now);
          if (choice % 7 === 0) {
            const b = state.batch;
            write({ kind: 'judgeDeferred', batchId: b?.window.id ?? 'missing',
              attempt: b?.phase.attempt ?? -1, until: now + choice % 11 - 5 }, now);
          }
          if (choice % 11 === 0) write({ kind: 'judgeDeferred', batchId: 'stale', attempt: -1, until: now }, now);
          if (item) write(retarget(item, state), now);
          else write({ kind: 'events', events: [], source: 'recovery', cursor: 'final' }, now);
        }
        ends.set(segment, json(toRecord(state)));
        for (const snapshot of snapshots) {
          const expected = ends.get(snapshot.segment)!;
          replay(snapshot, expected.version, expected);
        }
        if (cadence === 'always') assert.equal(stats.snapshots, stats.writes);
      }
    }), { seed: 0x5ec04, numRuns: RUNS });
  for (const stats of Object.values(counts)) {
    assert.equal(stats.histories, RUNS);
    assert.ok(stats.writes > 0);
    assert.ok(stats.snapshots > 0);
    assert.ok(stats.replays >= stats.writes * 2);
    assert.ok(stats.policyChanges > RUNS / 4 && stats.policyChanges < RUNS);
    assert.ok(stats.sizeSettles > 0 && stats.ends > 0 && stats.deferrals > 0);
  }
  console.log(JSON.stringify({ recovery: counts }));
}, 300_000);
