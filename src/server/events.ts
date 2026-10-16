import type { JsonObject } from '../core/types.js';
import type { DecisionRow } from '../store/types.js';
import type { AttentionDeadLetterData, AttentionDecisionData, EventDescriptor, EventOccurrence, Principal, SubscriptionFilter } from './types.js';

export function decisionDescriptor(): EventDescriptor {
  throw new Error('attend: decisionDescriptor is not implemented');
}

export function deadLetterDescriptor(): EventDescriptor {
  throw new Error('attend: deadLetterDescriptor is not implemented');
}

/** Validates against the descriptor's inputSchema and the principal's grants. Throws an EventsErrorObject (InvalidParams or Forbidden). */
export function compileFilter(name: string, args: JsonObject | undefined, principal: Principal): SubscriptionFilter {
  throw new Error('attend: compileFilter is not implemented');
}

export function matches(filter: SubscriptionFilter, row: DecisionRow): boolean {
  throw new Error('attend: matches is not implemented');
}

/** The occurrence for a row, its data serialized once per shape and shared across subscribers. */
export function occurrenceFor(row: DecisionRow, include: 'notice' | 'events'): EventOccurrence<AttentionDecisionData | AttentionDeadLetterData> {
  throw new Error('attend: occurrenceFor is not implemented');
}
