import type { EventsServer, HttpHandlerOptions, HttpRoute, ServerContext } from './types.js';

export function httpHandler(context: ServerContext, install: EventsServer['install'], options: HttpHandlerOptions): HttpRoute {
  throw new Error('attend: httpHandler is not implemented');
}
