import type { Judgment, Route } from './types.js';

/**
 * The act-threshold rule. `act` is p(interrupt) + p(ask) from the judgment's own probabilities,
 * with missing entries counted as 0. Returns the route to stage instead of the judge's, with
 * `act`, or null when the judge's route stands. A judgment without probabilities is never changed.
 * At or above the threshold, choose the offered interrupt or ask with the higher probability;
 * ties go to ask. If neither is offered, the judge's route stands. Below the threshold, replace
 * an acting route with the highest-probability offered non-acting route; ties prefer skip,
 * note, queue, then wait. Otherwise the judge's route stands.
 */
export function thresholdRoute(judgment: Judgment, offered: readonly Route[], actThreshold: number): { route: Route; act: number } | null {
  throw new Error('attend: thresholdRoute is not implemented');
}

export interface SuggestThresholdOptions {
  /** How long the replay that recorded the judgments covered. */
  durationMs: number;
  maxActsPerHour: number;
}

export interface ThresholdCandidate {
  threshold: number;
  acts: number;
  actsPerHour: number;
}

export interface ThresholdSuggestion {
  /** The smallest threshold whose act rate fits the budget, or null when none does. */
  threshold: number | null;
  candidates: ThresholdCandidate[];
}

/** Picks an `actThreshold` offline from the judgments a replay recorded, with no extra judge calls. */
export function suggestThreshold(judgments: readonly Pick<Judgment, 'route' | 'probabilities'>[], options: SuggestThresholdOptions): ThresholdSuggestion {
  throw new Error('attend: suggestThreshold is not implemented');
}
