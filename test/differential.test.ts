import assert from 'node:assert/strict';
import { isDeepStrictEqual } from 'node:util';
import fc from 'fast-check';
import { test } from 'vitest';
import { apply, newOutput } from '../src/core/reducer.js';
import { assertInvariants, fromRecord, initialState, toRecord } from '../src/core/state.js';
import { initialRecord, step } from '../src/core/step.js';
import { DEFAULT_POLICY } from '../src/core/types.js';
import type { ApplyConfig, DecisionAppend, ScopeState, Stepped } from '../src/core/types.js';
import { drive, planArbitrary } from './support/arbitraries.js';
import type { Driven } from './support/arbitraries.js';
import { deepFreeze } from './support/deep-freeze.js';

function oracleAppends(driven: Driven): DecisionAppend[] {
  const rows: DecisionAppend[] = [];
  for (const effect of driven.after.effects) {
    if (effect.kind !== 'trace') continue;
    const trace = effect.trace;
    if (trace.kind === 'judged' && trace.route !== 'skip') {
      const delivery = driven.after.record.outbox.decisions.find((item) => item.item.id === trace.batchId
        && !driven.before.outbox.decisions.some((old) => old.item === item.item));
      assert.ok(delivery?.item.kind === 'decision');
      const body = delivery.item.decision;
      rows.push({ kind: 'decision', id: body.id, scope: body.scope, at: body.at, route: body.route, body });
    } else if (trace.kind === 'deadLettered') {
      const body = trace.letter;
      rows.push({ kind: 'deadLetter', id: body.id, scope: body.scope, at: body.at, route: body.decision?.route ?? null, body });
    }
  }
  return rows;
}

test('matches the oracle through chained, restored, JSON, trace-free and frozen wrapper paths', () => {
  const override = process.env.ATTEND_DIFFERENTIAL_RUNS;
  const numRuns = override === undefined ? 900 : Number(override);
  const counts = { plans: 0, chainedInputs: 0, totalApplies: 0, offsetApplies: 0, inputs: {} as Record<string, number> };
  fc.assert(fc.property(planArbitrary(), fc.nat(), fc.nat(), (plan, restoreAt, jsonAt) => {
    const driven = drive(plan);
    const config: ApplyConfig = { policy: plan.policy, decisions: 'lane', deadLetters: 'lane', traces: true };
    const state = initialState(plan.scope, plan.policy);
    const quiet = initialState(plan.scope, plan.policy);
    assertInvariants(state, plan.policy);
    assertInvariants(quiet, plan.policy);
    const check = (current: ScopeState, item: Driven, traces: boolean): void => {
      const out = newOutput();
      const logged = deepFreeze({ input: item.input, now: item.now });
      apply({ ...config, traces }, current, logged, out);
      counts.totalApplies += 1;
      if (current.open.start > 0) counts.offsetApplies += 1;
      assertInvariants(current, plan.policy);
      const record = toRecord(current);
      assert.deepStrictEqual(record, item.after.record);
      assert.deepStrictEqual(out.effects, traces ? item.after.effects : item.after.effects.filter((e) => e.kind !== 'trace'));
      assert.equal(out.changed, !isDeepStrictEqual(item.before, item.after.record));
      assert.deepStrictEqual(out.appends, oracleAppends(item));
      deepFreeze(out.effects);
      deepFreeze(out.appends);
      deepFreeze(record);
    };
    counts.plans += 1;
    for (const item of driven) {
      check(state, item, true);
      check(quiet, item, false);
      counts.chainedInputs += 1;
      counts.inputs[item.input.kind] = (counts.inputs[item.input.kind] ?? 0) + 1;
      deepFreeze(item.before);
      assert.deepStrictEqual(step({ policy: plan.policy }, item.before, item.input, item.now), item.after);
      counts.totalApplies += 1;
    }
    for (const [index, json] of [[restoreAt, false], [jsonAt, true]] as const) {
      if (driven.length === 0) continue;
      const start = index % driven.length;
      const snapshot = driven[start]!.before;
      const current = fromRecord(deepFreeze(json ? JSON.parse(JSON.stringify(snapshot)) : snapshot), plan.policy);
      assertInvariants(current, plan.policy);
      for (const item of driven.slice(start)) check(current, item, true);
    }
  }), { seed: 0x4a774, numRuns });
  console.log(JSON.stringify({ differential: counts }));
  assert.ok(counts.offsetApplies > 0);
  if (override === undefined) assert.ok(counts.chainedInputs >= 100_000);
  else assert.equal(counts.plans, numRuns);
}, 300_000);

test('pins the single-entry and merged window ids through the pure wrapper', () => {
  const policy = { ...DEFAULT_POLICY, closingGraceMs: 1, waitHorizonMs: 1 };
  let result: Stepped = step({ policy }, initialRecord('s'), { kind: 'events', events: [{ id: 'a', at: 0, name: 'close', data: {}, kind: 'closing' }] }, 0);
  result = step({ policy }, result.record, { kind: 'tick' }, 1);
  assert.equal(result.record.batch?.window.id, '3ecbd4f2ef91c78f');
  result = step({ policy }, result.record, { kind: 'judged', batchId: '3ecbd4f2ef91c78f', attempt: 1, judgment: { route: 'wait' } }, 1);
  result = step({ policy }, result.record, { kind: 'events', events: [{ id: 'b', at: 1, name: 'close', data: {}, kind: 'closing' }] }, 1);
  result = step({ policy }, result.record, { kind: 'tick' }, 2);
  assert.equal(result.record.batch?.window.id, 'f9df8ba2df127ca6');
});
