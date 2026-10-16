import type { DecisionLog } from '../store/types.js';
import type { EventsServer, EventsServerOptions } from './types.js';

/** Takes a DecisionLog, not an Attend, so a read-only process can serve subscribers from Postgres or Redis without judging. */
export function eventsServer(log: DecisionLog, options?: EventsServerOptions): EventsServer {
  throw new Error('attend: eventsServer is not implemented');
}
