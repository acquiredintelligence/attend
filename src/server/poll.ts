import type { PollParams, PollResult, Principal, ServerContext } from './types.js';

export function pollEvents(context: ServerContext, params: PollParams, principal: Principal, signal: AbortSignal): Promise<PollResult> {
  throw new Error('attend: pollEvents is not implemented');
}
