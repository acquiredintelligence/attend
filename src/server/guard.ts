import type { IncomingMessage } from 'node:http';
import type { Sessions } from './sessions.js';
import type { EventsErrorObject } from './types.js';

/** What the SDK would check for a request attend serves itself: the session, MCP-Protocol-Version, an Mcp-Method header matching the body, Accept and the principal. Null when the request may proceed. */
export function guardStream(req: IncomingMessage, method: string, sessions: Sessions): EventsErrorObject | null {
  throw new Error('attend: guardStream is not implemented');
}
