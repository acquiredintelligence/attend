import type { Principal, ServerContext, StreamEventsResult, StreamNotification, StreamParams } from './types.js';

/** One events/stream subscription: throws an EventsErrorObject before any frame for an invalid one, sends active first, then catch-up from the log, then live rows and heartbeats, and resolves with the final frame when the server closes it. */
export function streamEvents(context: ServerContext, params: StreamParams, principal: Principal, subscriptionId: string | number, send: (notification: StreamNotification) => Promise<void>, signal: AbortSignal): Promise<StreamEventsResult> {
  throw new Error('attend: streamEvents is not implemented');
}
