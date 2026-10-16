import fc from 'fast-check';
import { describe, expect, test } from 'vitest';
import { thresholdRoute } from '../src/core/threshold.js';
import { ROUTES, type Judgment, type Route } from '../src/core/types.js';
import { suggestThreshold } from '../src/testing.js';

const usable = (value: number | undefined) => value !== undefined && Number.isFinite(value) && value >= 0 && value <= 1;
const probability = (judgment: Judgment, route: Route) => (usable(judgment.probabilities?.[route]) ? (judgment.probabilities?.[route] ?? 0) : 0);
const acts = (route: Route) => route === 'interrupt' || route === 'ask';
const probabilityArb = fc.option(fc.constantFrom(-0.1, 0, 0.1, 0.2, 0.25, 0.5, 0.7, 1, 1.1, NaN), { nil: undefined });
const probabilitiesArb = fc
  .tuple(...ROUTES.map(() => probabilityArb))
  .map((values) => Object.fromEntries(ROUTES.flatMap((route, index) => (values[index] === undefined ? [] : [[route, values[index]]]))));
const judgmentArb: fc.Arbitrary<Judgment> = fc
  .tuple(fc.constantFrom(...ROUTES), fc.option(probabilitiesArb, { nil: undefined }))
  .map(([route, probabilities]) => (probabilities === undefined ? { route } : { route, probabilities }));

describe('thresholdRoute', () => {
  test.each<{ name: string; judgment: Judgment; offered?: Route[]; threshold?: number; expected: { route: Route; act: number } | null }>([
    { name: 'no probabilities', judgment: { route: 'interrupt' }, expected: null },
    { name: 'empty probabilities', judgment: { route: 'ask', probabilities: {} }, expected: null },
    { name: 'no usable probabilities', judgment: { route: 'ask', probabilities: { interrupt: NaN, ask: 2 } }, expected: null },
    { name: 'interrupt wins', judgment: { route: 'skip', probabilities: { interrupt: 0.6, ask: 0.2 } }, expected: { route: 'interrupt', act: 0.8 } },
    { name: 'ask wins', judgment: { route: 'skip', probabilities: { interrupt: 0.2, ask: 0.6 } }, expected: { route: 'ask', act: 0.8 } },
    { name: 'acting ties prefer ask', judgment: { route: 'skip', probabilities: { interrupt: 0.4, ask: 0.4 } }, expected: { route: 'ask', act: 0.8 } },
    { name: 'exact threshold acts', judgment: { route: 'skip', probabilities: { interrupt: 0.5 } }, threshold: 0.5, expected: { route: 'interrupt', act: 0.5 } },
    { name: 'decimal sum meets threshold', judgment: { route: 'skip', probabilities: { interrupt: 0.7, ask: 0.1 } }, threshold: 0.8, expected: { route: 'interrupt', act: 0.8 } },
    { name: 'only ask offered', judgment: { route: 'skip', probabilities: { interrupt: 0.8, ask: 0 } }, offered: ['skip', 'ask'], expected: { route: 'ask', act: 0.8 } },
    { name: 'only interrupt offered', judgment: { route: 'skip', probabilities: { interrupt: 0, ask: 0.8 } }, offered: ['skip', 'interrupt'], expected: { route: 'interrupt', act: 0.8 } },
    { name: 'neither acting route offered', judgment: { route: 'skip', probabilities: { interrupt: 0.8 } }, offered: ['skip'], expected: null },
    { name: 'judge already chose acting winner', judgment: { route: 'interrupt', probabilities: { interrupt: 0.8 } }, expected: null },
    { name: 'ask replaces interrupt', judgment: { route: 'interrupt', probabilities: { ask: 0.8 } }, expected: { route: 'ask', act: 0.8 } },
    { name: 'interrupt falls to best quiet route', judgment: { route: 'interrupt', probabilities: { queue: 0.6, note: 0.3 } }, expected: { route: 'queue', act: 0 } },
    { name: 'ask falls to best quiet route', judgment: { route: 'ask', probabilities: { wait: 0.8 } }, expected: { route: 'wait', act: 0 } },
    { name: 'quiet ties prefer skip over note', judgment: { route: 'ask', probabilities: { skip: 0.4, note: 0.4 } }, expected: { route: 'skip', act: 0 } },
    { name: 'quiet ties prefer note over queue', judgment: { route: 'ask', probabilities: { note: 0.4, queue: 0.4 } }, expected: { route: 'note', act: 0 } },
    { name: 'quiet ties prefer queue over wait', judgment: { route: 'ask', probabilities: { queue: 0.4, wait: 0.4 } }, expected: { route: 'queue', act: 0 } },
    { name: 'wait must be offered', judgment: { route: 'ask', probabilities: { wait: 0.9, note: 0.1 } }, offered: ['ask', 'note', 'skip'], expected: { route: 'note', act: 0 } },
    { name: 'missing quiet probabilities choose skip', judgment: { route: 'ask', probabilities: { ask: 0.1 } }, expected: { route: 'skip', act: 0.1 } },
    { name: 'no quiet route offered', judgment: { route: 'ask', probabilities: { ask: 0.1 } }, offered: ['ask'], expected: null },
    { name: 'quiet judge route stands below threshold', judgment: { route: 'queue', probabilities: { note: 0.8 } }, expected: null },
    { name: 'wait stands below threshold', judgment: { route: 'wait', probabilities: { skip: 0.8 } }, expected: null },
    {
      name: 'out of range probabilities count as missing',
      judgment: { route: 'interrupt', probabilities: { interrupt: -0.1, ask: 1.1, note: 0.2, skip: Infinity, queue: NaN } },
      expected: { route: 'note', act: 0 },
    },
  ])('$name', ({ judgment, offered = [...ROUTES], threshold = 0.5, expected }) => {
    expect(thresholdRoute(judgment, offered, threshold)).toEqual(expected);
  });

  test('obeys offered routes, usable probabilities and tie order (2000 runs)', () => {
    fc.assert(
      fc.property(judgmentArb, fc.subarray([...ROUTES], { minLength: 1 }), fc.integer({ min: 1, max: 1000 }), (judgment, offered, thousandths) => {
        const threshold = thousandths / 1000;
        const result = thresholdRoute(judgment, offered, threshold);
        expect(result === null || (offered.includes(result.route) && result.route !== judgment.route)).toBe(true);
        const present = ROUTES.some((route) => usable(judgment.probabilities?.[route]));
        if (!present) {
          expect(result).toBeNull();
          return;
        }
        const act = Math.round((probability(judgment, 'interrupt') + probability(judgment, 'ask')) * 1e12) / 1e12;
        if (result !== null) expect(result.act).toBe(act);
        const final = result?.route ?? judgment.route;
        const choices = (act >= threshold ? ['ask', 'interrupt'] : acts(judgment.route) ? ['skip', 'note', 'queue', 'wait'] : []).filter((route): route is Route => offered.some((r) => r === route));
        if (choices.length > 0) {
          choices.sort((a, b) => probability(judgment, b) - probability(judgment, a));
          expect(final).toBe(choices[0]);
          expect(acts(final)).toBe(act >= threshold);
        } else expect(result).toBeNull();
        if (act < threshold && !acts(judgment.route)) expect(result).toBeNull();
      }),
      { numRuns: 2000 },
    );
  });
});

describe('suggestThreshold', () => {
  const fixture: Judgment[] = [
    { route: 'interrupt' },
    { route: 'skip' },
    { route: 'ask', probabilities: { skip: 1 } },
    { route: 'skip', probabilities: { interrupt: 0.8, ask: 0.7 } },
    { route: 'note', probabilities: { interrupt: 0.2 } },
    { route: 'skip', probabilities: { ask: 0.2 } },
    { route: 'skip', probabilities: { interrupt: 0.7, ask: 0.1 } },
    { route: 'skip', probabilities: { ask: 0.5 } },
  ];
  const candidates = [
    { threshold: 0.2, acts: 6, actsPerHour: 3 },
    { threshold: 0.5, acts: 4, actsPerHour: 2 },
    { threshold: 0.8, acts: 3, actsPerHour: 1.5 },
    { threshold: 1, acts: 2, actsPerHour: 1 },
  ];
  test('picks the smallest fitting candidate on a two-hour replay', () => {
    expect(suggestThreshold(fixture, { durationMs: 7_200_000, maxActsPerHour: 2 })).toEqual({ threshold: 0.5, candidates });
  });
  test('returns null when fixed acts prevent a zero budget', () => {
    expect(suggestThreshold(fixture, { durationMs: 7_200_000, maxActsPerHour: 0 })).toEqual({ threshold: null, candidates });
  });
  test('returns null when no candidate fits', () => {
    expect(suggestThreshold(fixture, { durationMs: 7_200_000, maxActsPerHour: 0.5 })).toEqual({ threshold: null, candidates });
  });
  test('a zero budget fits at one when every act probability is below one', () => {
    expect(suggestThreshold([{ route: 'interrupt', probabilities: { interrupt: 0.8 } }], { durationMs: 7_200_000, maxActsPerHour: 0 })).toEqual({
      threshold: 1,
      candidates: [
        { threshold: 0.8, acts: 1, actsPerHour: 0.5 },
        { threshold: 1, acts: 0, actsPerHour: 0 },
      ],
    });
  });
  test.each([0, -1, NaN, Infinity])('rejects invalid duration %s', (durationMs) => {
    expect(() => suggestThreshold([], { durationMs, maxActsPerHour: 1 })).toThrow(new RangeError(`attend: suggestThreshold needs a positive durationMs, got ${durationMs}`));
  });
  test.each([-1, NaN, Infinity])('rejects invalid budget %s', (maxActsPerHour) => {
    expect(() => suggestThreshold([], { durationMs: 1, maxActsPerHour })).toThrow(new RangeError(`attend: suggestThreshold needs a maxActsPerHour of 0 or more, got ${maxActsPerHour}`));
  });
  test('counts agree with thresholdRoute for every candidate (500 runs)', () => {
    fc.assert(
      fc.property(fc.array(judgmentArb, { maxLength: 80 }), fc.integer({ min: 1, max: 7_200_000 }), fc.integer({ min: 0, max: 100 }), (judgments, durationMs, maxActsPerHour) => {
        const result = suggestThreshold(judgments, { durationMs, maxActsPerHour });
        expect(result.candidates.at(-1)?.threshold).toBe(1);
        let previous = 0;
        for (const candidate of result.candidates) {
          expect(candidate.threshold).toBeGreaterThan(previous);
          previous = candidate.threshold;
          const count = judgments.filter((judgment) => acts(thresholdRoute(judgment, ROUTES, candidate.threshold)?.route ?? judgment.route)).length;
          expect(candidate.acts).toBe(count);
          expect(candidate.actsPerHour).toBe((count * 3_600_000) / durationMs);
        }
        expect(result.threshold).toBe(result.candidates.find((candidate) => candidate.actsPerHour <= maxActsPerHour)?.threshold ?? null);
      }),
      { numRuns: 500 },
    );
  });
});
