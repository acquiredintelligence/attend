import type { JudgeBudget } from '../core/types.js';

/** (contextTokens − reservedTokens) × windowShare × bytesPerToken. */
export function windowBytesFor(budget: JudgeBudget, windowShare: number): number {
  throw new Error('attend: windowBytesFor is not implemented');
}
