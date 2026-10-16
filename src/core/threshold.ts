import { ROUTES, type Judgment, type Route } from './types.js';

/**
 * The act-threshold rule. `act` is p(interrupt) + p(ask) from the judgment's own probabilities,
 * with missing entries counted as 0. Only finite probabilities in [0, 1] count, as `validate`
 * reads them. Returns the route to stage instead of the judge's, with `act`, or null when the
 * judge's route stands. A judgment with no usable probability is never changed.
 * At or above the threshold, choose the offered interrupt or ask with the higher probability;
 * ties go to ask. If neither is offered, the judge's route stands. Below the threshold, replace
 * an acting route with the highest-probability offered non-acting route; ties prefer skip,
 * note, queue, then wait. Otherwise the judge's route stands.
 */
export function thresholdRoute(judgment: Judgment, offered: readonly Route[], actThreshold: number): { route: Route; act: number } | null {
  const act = actProbability(judgment);
  if (act === null) return null;
  const choices: Route[] = act >= actThreshold ? ['ask', 'interrupt'] : acting(judgment.route) ? ['skip', 'note', 'queue', 'wait'] : [];
  let route: Route | undefined;
  let best = -1;
  for (const choice of choices) {
    const p = probability(judgment.probabilities?.[choice]) ?? 0;
    if (offered.includes(choice) && p > best) {
      route = choice;
      best = p;
    }
  }
  return route === undefined || route === judgment.route ? null : { route, act };
}

/** Replay duration and the maximum acceptable act rate. */
export interface SuggestThresholdOptions {
  /** How long the replay that recorded the judgments covered. */
  durationMs: number;
  maxActsPerHour: number;
}

/** The observed act count and hourly rate at one candidate threshold. */
export interface ThresholdCandidate {
  threshold: number;
  acts: number;
  actsPerHour: number;
}

/** Candidate rates and the smallest threshold that meets the budget. */
export interface ThresholdSuggestion {
  /** The smallest threshold whose act rate fits the budget, or null when none does. */
  threshold: number | null;
  candidates: ThresholdCandidate[];
}

/**
 * Picks an `actThreshold` offline from the judgments a replay recorded, with no extra judge calls.
 * The candidates are every `act` above 0 that the judgments reach, capped at 1, plus 1. A judgment
 * with no usable probability acts at every threshold when its own route is interrupt or ask.
 */
export function suggestThreshold(judgments: readonly Pick<Judgment, 'route' | 'probabilities'>[], options: SuggestThresholdOptions): ThresholdSuggestion {
  const { durationMs, maxActsPerHour } = options;
  if (!Number.isFinite(durationMs) || durationMs <= 0) throw new RangeError(`attend: suggestThreshold needs a positive durationMs, got ${durationMs}`);
  if (!Number.isFinite(maxActsPerHour) || maxActsPerHour < 0) throw new RangeError(`attend: suggestThreshold needs a maxActsPerHour of 0 or more, got ${maxActsPerHour}`);
  const acts: number[] = [];
  let fixed = 0;
  for (const judgment of judgments) {
    const act = actProbability(judgment);
    if (act === null) fixed += Number(acting(judgment.route));
    else if (act > 0) acts.push(act);
  }
  acts.sort((a, b) => a - b);
  const thresholds = [...new Set([...acts.map((act) => Math.min(act, 1)), 1])];
  let below = 0;
  const candidates = thresholds.map((threshold) => {
    while (below < acts.length && (acts[below] ?? 0) < threshold) below += 1;
    const count = fixed + acts.length - below;
    return { threshold, acts: count, actsPerHour: (count * 3_600_000) / durationMs };
  });
  return { threshold: candidates.find((candidate) => candidate.actsPerHour <= maxActsPerHour)?.threshold ?? null, candidates };
}

function probability(value: number | undefined): number | undefined {
  return value !== undefined && Number.isFinite(value) && value >= 0 && value <= 1 ? value : undefined;
}

function acting(route: Route): boolean {
  return route === 'interrupt' || route === 'ask';
}

function actProbability(judgment: Pick<Judgment, 'probabilities'>): number | null {
  const { probabilities } = judgment;
  if (probabilities === undefined || !ROUTES.some((route) => probability(probabilities[route]) !== undefined)) return null;
  // Decimal sums such as 0.7 + 0.1 must meet an exact 0.8 threshold.
  return Math.round(((probability(probabilities.interrupt) ?? 0) + (probability(probabilities.ask) ?? 0)) * 1e12) / 1e12;
}
