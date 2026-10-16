import type { ApplyConfig, ApplyOutput, Decision, Judgment, LoggedInput, Route, ScopeState, SettlePolicy } from './types.js';

/** Bumped whenever a release changes a transition. Stored with every snapshot. */
export const REDUCER_VERSION = 1;

export function newOutput(): ApplyOutput {
  throw new Error('attend: newOutput is not implemented');
}

/**
 * Applies one logged input to `state` in place: today's transitions, plus `end`, `judgeDeferred`
 * and the `size` trigger. Deterministic in (state, logged, config). Pushes effects and decision
 * rows onto `out`, and sets `out.changed` if anything changed. Never reads a clock and never calls
 * app code. `events`, `flush`, `end` and the releases still yield traces only.
 */
export function apply(config: ApplyConfig, state: ScopeState, logged: LoggedInput, out: ApplyOutput): void {
  throw new Error('attend: apply is not implemented');
}

/** The instant the state next wants a tick, computed inline with no allocation. */
export function nextDue(policy: SettlePolicy, state: ScopeState): number | null {
  throw new Error('attend: nextDue is not implemented');
}

/** Every route, with `wait` only when it is offered. */
export function offeredRoutes(offerWait: boolean): Route[] {
  throw new Error('attend: offeredRoutes is not implemented');
}

/** The judge's answer checked against the offered routes, with Jev's `confidence`. */
export function validate(judgment: Judgment, offered: Route[]): (Pick<Decision, 'probabilities' | 'predicates' | 'confidence'> & { route: Route }) | null {
  throw new Error('attend: validate is not implemented');
}
