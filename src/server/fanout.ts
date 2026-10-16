import type { DecisionLog, DecisionRow } from '../store/types.js';
import type { SubscriptionFilter } from './types.js';

export interface Fanout {
  subscribe(filter: SubscriptionFilter, onRow: (row: DecisionRow) => void): () => void;
  close(): void;
}

/** The process's tail cache of recent rows and the index that matches them to subscriptions. */
export function createFanout(log: DecisionLog): Fanout {
  throw new Error('attend: createFanout is not implemented');
}
