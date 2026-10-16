import type { JudgeBudget } from '../core/types.js';

/** (contextTokens − reservedTokens) × windowShare × bytesPerToken. */
export function windowBytesFor(budget: JudgeBudget, windowShare: number): number {
  return Math.floor((budget.contextTokens - (budget.reservedTokens ?? 0)) * windowShare * (budget.bytesPerToken ?? 4));
}
