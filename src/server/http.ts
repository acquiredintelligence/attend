import type { IncomingMessage, ServerResponse } from 'node:http';
import { EventsError, eventsError, parseSubscription } from './events.js';
import { notificationJSON } from './fanout.js';
import { guardStream } from './guard.js';
import { createSessions } from './sessions.js';
import { streamEvents } from './stream.js';
import { SUBSCRIPTION_ID_META, type EventsServer, type HttpHandlerOptions, type HttpRoute, type Principal, type ServerContext, type StreamNotification, type StreamParams } from './types.js';

const AUTH_INTERVAL_MS = 60_000;
type Sdk = typeof import('@modelcontextprotocol/server');
type Session = { principal: Principal; transport: InstanceType<Sdk['WebStandardStreamableHTTPServerTransport']>; close: () => Promise<void>; abort: AbortController };

function json(res: ServerResponse, status: number, body: unknown) {
  if (res.destroyed || res.writableEnded) return;
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(body));
}

function writer(res: ServerResponse, signal: AbortSignal) {
  let pending = Promise.resolve();
  return (bytes: string | Uint8Array) => {
    const next = pending.then(async () => {
      signal.throwIfAborted();
      if (res.destroyed || res.writableEnded) throw new DOMException('Response closed', 'AbortError');
      if (res.write(bytes)) return;
      await new Promise<void>((resolve, reject) => {
        const done = () => { cleanup(); resolve(); };
        const abort = () => { cleanup(); reject(new DOMException('Response closed', 'AbortError')); };
        const cleanup = () => { res.off('drain', done); signal.removeEventListener('abort', abort); };
        res.once('drain', done);
        signal.addEventListener('abort', abort, { once: true });
        if (signal.aborted) abort();
      });
    });
    pending = next.catch(() => undefined);
    return next;
  };
}

/** Owns stream writes and session lifetimes while forwarding other MCP traffic to the SDK. */
export function httpHandler(context: ServerContext, install: EventsServer['install'], options: HttpHandlerOptions): HttpRoute {
  let sdkPromise: Promise<Sdk> | undefined;
  const entries = new Map<string, Session>();
  const streams = new Set<{ id: string | number; session: string | undefined; principal: string; abort: AbortController; done: Promise<void> }>();
  const sessions = createSessions({ max: options.sessions?.max ?? 10_000, idleMs: options.sessions?.idleMs ?? 1_800_000, onClose(id) {
    const entry = entries.get(id);
    entries.delete(id);
    if (entry) { entry.abort.abort(); void entry.close().catch(() => undefined); }
  } });
  context.signal.addEventListener('abort', () => {
    void Promise.allSettled([...streams].map((stream) => stream.done)).then(() => sessions.dispose());
  }, { once: true });
  const resolvePrincipal = async (req: IncomingMessage) => context.options.principal ? context.options.principal(req) : context.options.localPrincipal ?? { id: 'local', scopes: [''] };
  return async (req, res) => {
    const path = new URL(req.url ?? '/', 'http://localhost').pathname;
    const route = options.routes?.[`${req.method} ${path}`];
    if (route) { await route(req, res); return; }
    if (path !== (options.path ?? '/mcp')) { res.writeHead(404); res.end(); return; }
    const abort = new AbortController();
    const stop = () => abort.abort();
    res.once('close', stop); res.once('error', stop); req.once('aborted', stop);
    const write = writer(res, abort.signal);
    let id: string | number | null = null;
    try {
      const sdk = await (sdkPromise ??= import('@modelcontextprotocol/server'));
      let text = '';
      let body: unknown;
      if (req.method === 'POST') {
        const tooLarge = () => json(res, 413, { jsonrpc: '2.0', id: null, error: { code: -32000, message: `Request body too large: exceeds ${sdk.DEFAULT_MAX_REQUEST_BODY_SIZE} bytes` } });
        if (Number(req.headers['content-length']) > sdk.DEFAULT_MAX_REQUEST_BODY_SIZE) { tooLarge(); return; }
        const chunks: Buffer[] = [];
        let bytes = 0;
        for await (const chunk of req) {
          const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
          bytes += buffer.length;
          if (bytes > sdk.DEFAULT_MAX_REQUEST_BODY_SIZE) { tooLarge(); return; }
          chunks.push(buffer);
        }
        text = Buffer.concat(chunks).toString('utf8');
        try { body = JSON.parse(text); }
        catch { json(res, 400, { jsonrpc: '2.0', id: null, error: { code: -32700, message: 'Parse error: Invalid JSON' } }); return; }
        if (Array.isArray(body) && body.some((entry: unknown) => entry !== null && typeof entry === 'object' && 'method' in entry && entry.method === 'events/stream')) {
          json(res, 400, { jsonrpc: '2.0', id: null, error: { code: -32600, message: 'Invalid Request: events/stream cannot be sent in a batch' } }); return;
        }
      }
      const request = sdk.isJSONRPCRequest(body) ? body : undefined;
      id = request?.id ?? null;
      const sessionId = typeof req.headers['mcp-session-id'] === 'string' ? req.headers['mcp-session-id'] : undefined;
      let principal: Principal | null = null;
      if (request?.method === 'events/stream') {
        const rejection = await guardStream(req, request, sessions, sdk, async () => principal = await resolvePrincipal(req));
        if (rejection) { json(res, rejection.status, rejection.body); return; }
      } else principal = await resolvePrincipal(req);
      if (!principal) { json(res, 401, { jsonrpc: '2.0', id, error: { code: -32000, message: 'Unauthorized' } }); return; }
      if (context.signal.aborted) { json(res, 503, { jsonrpc: '2.0', id, error: { code: -32000, message: 'Server closed' } }); return; }
      const headers = new Headers();
      for (const [name, value] of Object.entries(req.headers)) {
        if (Array.isArray(value)) for (const item of value) headers.append(name, item);
        else if (value !== undefined) headers.set(name, value);
      }
      const web = new Request(`http://${req.headers.host ?? 'localhost'}${req.url ?? '/'}`, { method: req.method ?? 'GET', headers, ...(req.method === 'POST' ? { body: text } : {}), signal: abort.signal });
      const legacy = request?.method === 'events/stream' ? req.headers['mcp-protocol-version'] !== '2026-07-28' : await sdk.isLegacyRequest(web, body);
      if (legacy && !['POST', 'GET', 'DELETE'].includes(req.method ?? 'GET')) {
        const transport = new sdk.WebStandardStreamableHTTPServerTransport();
        try { await relay(await transport.handleRequest(web)); } finally { await transport.close(); }
        return;
      }
      let session: Session | undefined;
      if (legacy && sessionId) {
        if (!sessions.touch(sessionId)) { json(res, 404, { jsonrpc: '2.0', id, error: { code: -32001, message: 'Session not found' } }); return; }
        session = entries.get(sessionId);
        if (session && principal.id !== session.principal.id) { json(res, 403, { jsonrpc: '2.0', id, error: { code: -32024, message: 'Session belongs to another principal' } }); return; }
      }
      if (request?.method === 'events/stream') {
        const owner = principal;
        let opened = false;
        let terminating = false;
        let termination: Promise<void> | undefined;
        const params = request.params as unknown as StreamParams;
        const send = async (notification: StreamNotification) => {
          if (terminating) throw new DOMException('Stream terminated', 'AbortError');
          if (!opened) {
            opened = true;
            res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache, no-transform', Connection: 'keep-alive', ...(sessionId ? { 'Mcp-Session-Id': sessionId } : {}) });
            res.flushHeaders();
          }
          await write(`data: ${notificationJSON(notification)}\n\n`);
        };
        let checking = false;
        const timer = setInterval(() => {
          if (checking || terminating || abort.signal.aborted || !context.options.principal) return;
          checking = true;
          void (async () => {
            const current = await resolvePrincipal(req);
            try {
              if (!current || current.id !== owner.id) throw eventsError('Forbidden', 'Access revoked', { reason: 'principal revoked' });
              parseSubscription(params, current);
              if (!params.arguments?.scope && !params.arguments?.scopePrefix && !owner.scopes.every((grant) => current.scopes.some((scope) => grant.startsWith(scope)))) throw eventsError('Forbidden', 'Access revoked', { reason: 'grants revoked' });
            } catch (error) {
              if (!(error instanceof EventsError) || error.code !== -32024) throw error;
              terminating = true;
              termination = write(`data: ${notificationJSON({ method: 'notifications/events/terminated', params: { error: error.toJSON(), _meta: { [SUBSCRIPTION_ID_META]: request.id } } })}\n\n`).finally(() => { abort.abort(); res.end(); });
              await termination;
            }
          })().catch(stop).finally(() => { checking = false; });
        }, AUTH_INTERVAL_MS);
        timer.unref();
        const sessionStop = () => abort.abort();
        session?.abort.signal.addEventListener('abort', sessionStop, { once: true });
        let finish: () => void = () => undefined;
        const entry = { id: request.id, session: sessionId, principal: principal.id, abort, done: new Promise<void>((resolve) => { finish = resolve; }) };
        streams.add(entry);
        const release = legacy && sessionId ? sessions.hold(sessionId) : () => undefined;
        try {
          const result = await streamEvents(context, params, principal, request.id, send, abort.signal);
          if (!abort.signal.aborted && !terminating) { await write(`data: ${JSON.stringify({ jsonrpc: '2.0', id: request.id, result })}\n\n`); res.end(); }
        } catch (error) {
          if (!abort.signal.aborted && !terminating) {
            if (!opened && error instanceof EventsError) json(res, 200, { jsonrpc: '2.0', id, error: error.toJSON() });
            else if (error instanceof DOMException && error.name === 'AbortError') res.end();
            else throw error;
          }
          await termination;
          if (abort.signal.aborted && !res.destroyed) res.end();
        } finally { release(); clearInterval(timer); session?.abort.signal.removeEventListener('abort', sessionStop); streams.delete(entry); finish(); }
        return;
      }
      if (sdk.isJSONRPCNotification(body) && body.method === 'notifications/cancelled') {
        for (const stream of streams) if (stream.id === body.params?.requestId && stream.session === sessionId && stream.principal === principal.id) stream.abort.abort();
      }
      let response: Response;
      if (!legacy) {
        const owner = principal;
        const handler = sdk.createMcpHandler(() => {
          const server = options.newServer?.() ?? new sdk.Server({ name: context.options.name ?? 'attend', version: context.options.version ?? '0.2.0' }, { capabilities: {} });
          install(server, { principal: owner });
          // The public seam omits SDK methods that its factory also requires.
          return server as InstanceType<Sdk['Server']> | InstanceType<Sdk['McpServer']>;
        }, { legacy: 'reject' });
        response = await handler.fetch(web, { parsedBody: body });
        try { await relay(response); } finally { await handler.close(); }
        return;
      }
      if (!session) {
        if (request?.method !== 'initialize') { json(res, 400, { jsonrpc: '2.0', id, error: { code: -32000, message: 'Bad Request: Mcp-Session-Id header is required' } }); return; }
        const newId = sessions.open();
        if (!newId) { json(res, 503, { jsonrpc: '2.0', id, error: eventsError('ResourceExhausted', 'Session quota exceeded', { limit: 'sessions', max: options.sessions?.max ?? 10_000 }).toJSON() }); return; }
        try {
          const server = options.newServer?.() ?? new sdk.Server({ name: context.options.name ?? 'attend', version: context.options.version ?? '0.2.0' }, { capabilities: {} });
          const transport = new sdk.WebStandardStreamableHTTPServerTransport({ sessionIdGenerator: () => newId, onsessionclosed: () => sessions.close(newId) });
          install(server, { principal });
          session = { principal, transport, abort: new AbortController(), async close() { await transport.close(); if ('close' in server && typeof server.close === 'function') await server.close(); } };
          entries.set(newId, session);
          await server.connect(transport);
        } catch (error) { sessions.close(newId); throw error; }
      }
      response = await session.transport.handleRequest(web, { parsedBody: body });
      await relay(response);
      async function relay(response: Response) {
        res.writeHead(response.status, Object.fromEntries(response.headers));
        const reader = response.body?.getReader();
        const cancel = () => { void reader?.cancel().catch(() => undefined); };
        abort.signal.addEventListener('abort', cancel, { once: true });
        try {
          if (reader) while (!abort.signal.aborted) {
            const { value, done } = await reader.read();
            if (done) break;
            await write(value);
          }
          if (!abort.signal.aborted) res.end();
        } finally { abort.signal.removeEventListener('abort', cancel); if (abort.signal.aborted) cancel(); reader?.releaseLock(); }
      }
    } catch (error) {
      if (!abort.signal.aborted) {
        if (!res.headersSent) json(res, 500, { jsonrpc: '2.0', id, error: { code: -32603, message: 'Internal server error' } });
        else res.destroy(error instanceof Error ? error : new Error(String(error)));
      }
    } finally { res.off('close', stop); res.off('error', stop); req.off('aborted', stop); }
  };
}
