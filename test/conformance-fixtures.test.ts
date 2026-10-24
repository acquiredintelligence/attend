/**
 * The TypeScript runner for the shared conformance fixtures under `conformance/fixtures`. The
 * Python port runs the same files with `python/tests/test_conformance.py`. With
 * `ATTEND_CONFORMANCE_UPDATE=1` (`pnpm conformance:update`) it rewrites the fixtures from `src`
 * instead of checking them.
 */
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, test } from 'vitest';
import { defaultNotice } from '../src/attend.js';
import { initialRecord, step, validate, windowId } from '../src/step.js';
import { utf8Length } from '../src/core/ids.js';
import { suggestThreshold, thresholdRoute } from '../src/core/threshold.js';
import type { ScopeRecord } from '../src/types.js';
import { digest, idFixtures, noticeFixtures, stepFixtures, thresholdFixtures, type StepFixturePlan } from './conformance/fixtures.js';
import { recordJudgeFixtures, runJudgeCase, type JudgeCase } from './conformance/judges.js';
import { runScenario, type Observed, type Scenario } from './conformance/scenario.js';
import { SCENARIOS } from './conformance/scenarios.js';

const UPDATE = process.env.ATTEND_CONFORMANCE_UPDATE === '1';
const directory = fileURLToPath(new URL('../conformance/fixtures/', import.meta.url));
const STEP_SEEDS = [101, 202, 303];

/** Reads a fixture, or with UPDATE builds and writes it. Large fixtures are written compact; `pretty` ones indented for review. */
function fixture<T>(name: string, build: () => T | Promise<T>, pretty = false): () => Promise<T> {
  return async () => {
    const path = `${directory}${name}.json`;
    if (UPDATE) {
      const value = await build();
      mkdirSync(directory, { recursive: true });
      writeFileSync(path, `${pretty ? JSON.stringify(value, null, 1) : JSON.stringify(value)}\n`);
      return value;
    }
    return JSON.parse(readFileSync(path, 'utf8')) as T;
  };
}

const json = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;

describe('conformance fixtures', () => {
  for (const seed of STEP_SEEDS) {
    test(`reducer traces, seed ${seed}`, async () => {
      const plans = await fixture(`step-${seed}`, () => ({ plans: stepFixtures(seed, 30, 80) }))();
      let steps = 0;
      for (const plan of (plans as { plans: StepFixturePlan[] }).plans) {
        let record: ScopeRecord = initialRecord(plan.scope);
        for (const [index, expected] of plan.steps.entries()) {
          const after = step(plan.config, record, expected.input, expected.now);
          record = after.record;
          const where = `${plan.name} step ${index} (${expected.input.kind})`;
          expect(json(after.effects), where).toEqual(expected.effects);
          if (expected.record) expect(json(record), where).toEqual(expected.record);
          expect(digest(record), where).toBe(expected.digest);
          steps += 1;
        }
      }
      expect(steps).toBeGreaterThan(1000);
    }, 120_000);
  }

  test('window ids and UTF-8 lengths', async () => {
    const { cases, utf8 } = await fixture('ids', () => idFixtures(7, 400))();
    for (const c of cases) expect(windowId(c.scope, c.ids), JSON.stringify(c)).toBe(c.id);
    for (const c of utf8) expect(utf8Length(c.text)).toBe(c.length);
  });

  test('the act threshold, validate and suggestThreshold', async () => {
    const f = await fixture('threshold', () => thresholdFixtures(11, 300))();
    for (const c of f.thresholdRoute) expect(json(thresholdRoute(c.judgment, c.offered, c.threshold)), JSON.stringify(c)).toEqual(c.result);
    for (const c of f.validate) expect(json(validate(c.judgment, c.offered)), JSON.stringify(c)).toEqual(c.result);
    for (const c of f.suggestThreshold) expect(json(suggestThreshold(c.judgments, c.options)), JSON.stringify(c)).toEqual(c.result);
  });

  test('defaultNotice', async () => {
    const { cases } = await fixture('notice', noticeFixtures)();
    for (const c of cases) expect(defaultNotice(c.decision, c.options)).toBe(c.text);
  });

  test('scenarios through createAttend on a fake clock', async () => {
    const recorded = await fixture('scenarios', async () => {
      const out: { scenario: Scenario; expected: Observed }[] = [];
      for (const scenario of SCENARIOS) out.push({ scenario, expected: await runScenario(scenario) });
      return { scenarios: out };
    }, true)();
    expect(recorded.scenarios.map((s) => s.scenario.name)).toEqual(SCENARIOS.map((s) => s.name));
    for (const { scenario, expected } of recorded.scenarios) {
      expect(await runScenario(scenario), scenario.name).toEqual(expected);
    }
  }, 60_000);

  test('hosted judge exchanges', async () => {
    const { cases } = await fixture('judges', recordJudgeFixtures)();
    for (const c of cases as JudgeCase[]) {
      const { expected, ...rest } = c;
      expect(await runJudgeCase(rest), c.name).toEqual(expected);
    }
  });
});
