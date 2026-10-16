import type { IncomingMessage } from 'node:http';
import type { Sessions } from './sessions.js';
import type { EventsErrorObject, Principal } from './types.js';

type ProtocolConstants = Pick<typeof import('@modelcontextprotocol/server'), 'SUPPORTED_PROTOCOL_VERSIONS' | 'DEFAULT_NEGOTIATED_PROTOCOL_VERSION' | 'isJsonContentType'>;
export interface StreamRejection {
  status: number;
  body: { jsonrpc: '2.0'; id: string | number | null; error: EventsErrorObject };
}

/** Checks the transport requirements for the one request attend intercepts. */
export async function guardStream(req: Pick<IncomingMessage, 'headers'>, request: { method: string; id: string | number }, sessions: Sessions, sdk: ProtocolConstants, principal: () => Promise<Principal | null>): Promise<StreamRejection | null> {
  const reject = (status: number, code: number, message: string): StreamRejection => ({ status, body: { jsonrpc: '2.0', id: request.id, error: { code, message } } });
  const version = req.headers['mcp-protocol-version'] ?? sdk.DEFAULT_NEGOTIATED_PROTOCOL_VERSION;
  if (typeof version !== 'string' || (version !== '2026-07-28' && !sdk.SUPPORTED_PROTOCOL_VERSIONS.includes(version))) return reject(400, -32000, `Bad Request: Unsupported protocol version: ${version} (supported versions: ${sdk.SUPPORTED_PROTOCOL_VERSIONS.join(', ')})`);
  const method = req.headers['mcp-method'];
  if ((version === '2026-07-28' && method === undefined) || (method !== undefined && method !== request.method)) return reject(400, -32020, `Mcp-Method header must match ${request.method}`);
  if (version !== '2026-07-28') {
    const session = req.headers['mcp-session-id'];
    if (!session) return reject(400, -32000, 'Bad Request: Mcp-Session-Id header is required');
    if (typeof session !== 'string' || !sessions.touch(session)) return reject(404, -32001, 'Session not found');
  }
  const accept = req.headers.accept;
  if (!accept?.includes('application/json') || !accept.includes('text/event-stream')) return reject(406, -32000, 'Not Acceptable: Client must accept both application/json and text/event-stream');
  const contentType = req.headers['content-type'];
  if (typeof contentType !== 'string' || !sdk.isJsonContentType(contentType)) return reject(415, -32000, 'Unsupported Media Type: Content-Type must be application/json');
  if (!(await principal())) return reject(401, -32000, 'Unauthorized');
  return null;
}
