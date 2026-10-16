import { createServer, request as httpRequest, type IncomingMessage, type ServerResponse } from 'node:http';
import { once } from 'node:events';
import { afterEach, expect, test, vi } from 'vitest';
import { Client, StreamableHTTPClientTransport } from '@modelcontextprotocol/client';
import { Server, SUPPORTED_PROTOCOL_VERSIONS, DEFAULT_NEGOTIATED_PROTOCOL_VERSION, isJsonContentType } from '@modelcontextprotocol/server';
import { memoryStore } from '../../src/store/memory.js';
import type { DecisionAppend } from '../../src/core/types.js';
import { eventsServer } from '../../src/server/install.js';
import { decisionDescriptor } from '../../src/server/events.js';
import { createFanout } from '../../src/server/fanout.js';
import { httpHandler } from '../../src/server/http.js';
import { guardStream } from '../../src/server/guard.js';
import { createSessions } from '../../src/server/sessions.js';
import { streamUsage } from '../../src/server/stream.js';
import { SUBSCRIPTION_ID_META, type EventsServerOptions, type HttpHandlerOptions, type ServerContext, type Principal } from '../../src/server/types.js';

const name = 'attention.decision';
const all: Principal = { id: 'local', scopes: [''] };
const modern = { 'Content-Type': 'application/json', Accept: 'application/json, text/event-stream', 'MCP-Protocol-Version': '2026-07-28', 'Mcp-Method': 'events/stream' };
const legacy = { ...modern, 'MCP-Protocol-Version': '2025-11-25' };
const schema = { '~standard': { version: 1 as const, vendor: 'test', validate: (value: unknown) => ({ value: value as Record<string, unknown> }) } };
interface Frame { jsonrpc: string; id?: string | number; method?: string; params?: { cursor?: string; truncated?: boolean; eventId?: string; error?: { code: number }; _meta: Record<string, unknown> }; result?: Record<string, unknown>; error?: { code: number; message: string } }
const eventually = async (get: () => unknown, expected: unknown) => expect.poll(get, { timeout: 10_000, interval: 5 }).toEqual(expected);
const cleanup: (() => Promise<void>)[] = [];
afterEach(async () => { vi.useRealTimers(); await Promise.all(cleanup.splice(0).map((close) => close())); });
function append(id: string, bytes = 8): DecisionAppend {
  const at = Date.now();
  return { kind: 'decision', id, scope: 's', at, route: 'interrupt', body: { id, scope: 's', at, route: 'interrupt', probabilities: { interrupt: 1, queue: 0, note: 0, ask: 0, skip: 0, wait: 0 }, predicates: {}, confidence: null, window: { id, events: [{ id: `event-${id}`, name: 'change', at, data: { text: 'x'.repeat(bytes) }, kind: 'closing', seenAt: at }], reason: 'closing', settledAt: at, waits: 0, gap: false, dropped: 0 } } };
}
async function target(options: EventsServerOptions = {}, httpOptions: HttpHandlerOptions = {}, publicApi = false) {
  const store = memoryStore();
  const events = eventsServer(store.log, options);
  const close = new AbortController();
  const context: ServerContext = { log: store.log, epoch: () => store.log.epoch(), options, signal: close.signal, descriptors: [decisionDescriptor()], fanout: createFanout(store.log) };
  const route = publicApi ? events.http(httpOptions) : httpHandler(context, events.install, httpOptions);
  const responses: ServerResponse[] = [];
  const server = createServer((req, res) => { responses.push(res); void route(req, res); });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  const address = server.address(); if (!address || typeof address === 'string') throw new Error('Missing server address');
  const url = `http://127.0.0.1:${address.port}/mcp`;
  let version = 0;
  const shutdown = async () => { close.abort(); await events.close(); };
  cleanup.push(async () => { await shutdown(); context.fanout.close(); server.closeAllConnections(); await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve())); });
  return {
    url, context, responses, events, shutdown,
    async commit(rows: DecisionAppend[]) {
      expect(await store.commit([{ scope: 's', expectedVersion: version++, inputs: [{ input: { kind: 'flush' }, now: Date.now() }], snapshot: null, appends: rows }])).toEqual(['ok']);
    },
    async post(method: string, params: object = {}, headers: Record<string, string> = modern, id: string | number = 1) {
      return fetch(url, { method: 'POST', headers: { ...headers, 'Mcp-Method': method }, body: JSON.stringify({ jsonrpc: '2.0', id, method, params }) });
    },
    async initialize() {
      const response = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json, text/event-stream' }, body: JSON.stringify({ jsonrpc: '2.0', id: 0, method: 'initialize', params: { protocolVersion: '2025-11-25', capabilities: {}, clientInfo: { name: 'raw', version: '1' } } }) });
      await response.text();
      return { status: response.status, session: response.headers.get('mcp-session-id')! };
    },
  };
}
async function stream(url: string, id: string | number = 7, params: object = {}, headers: Record<string, string> = modern) {
  const frames: Frame[] = [];
  const chunks: string[] = [];
  let buffer = '';
  const req = httpRequest(url, { method: 'POST', headers });
  req.on('error', () => undefined);
  const response = new Promise<IncomingMessage>((resolve, reject) => { req.once('response', resolve); req.once('error', reject); });
  req.end(JSON.stringify({ jsonrpc: '2.0', id, method: 'events/stream', params: { name, ...params } }));
  const res = await response;
  res.setEncoding('utf8');
  res.on('error', () => undefined);
  res.on('data', (chunk: string) => {
    chunks.push(chunk); buffer += chunk;
    let split: number;
    while ((split = buffer.indexOf('\n\n')) >= 0) {
      const frame = buffer.slice(0, split); buffer = buffer.slice(split + 2);
      if (frame.startsWith('data: ')) frames.push(JSON.parse(frame.slice(6)));
    }
  });
  let ended = false;
  res.once('end', () => { ended = true; });
  res.once('close', () => { ended = true; });
  cleanup.push(async () => { req.destroy(); res.destroy(); });
  return { req, res, frames, chunks, ended: () => ended, ids: () => frames.flatMap((f) => f.params?.eventId ? [f.params.eventId] : []) };
}

test('guard table preserves status, precedence and codes without opening subscriptions', async () => {
  const sessions = createSessions({ max: 2, idleMs: 1000 });
  const session = sessions.open()!;
  const constants = { SUPPORTED_PROTOCOL_VERSIONS, DEFAULT_NEGOTIATED_PROTOCOL_VERSION, isJsonContentType } as const;
  const base = { 'content-type': 'application/json', 'mcp-protocol-version': '2025-11-25', 'mcp-method': 'events/stream', 'mcp-session-id': session, accept: 'application/json, text/event-stream' };
  try {
    for (const [headers, principal, status, code] of [
      [{ ...base, 'mcp-protocol-version': 'invalid', 'mcp-method': 'wrong' }, all, 400, -32000],
      [{ ...base, 'mcp-method': 'wrong' }, all, 400, -32020],
      [{ ...base, 'mcp-session-id': 'unknown' }, all, 404, -32001],
      [{ ...base, 'mcp-session-id': undefined }, all, 400, -32000],
      [{ ...base, accept: 'application/json' }, all, 406, -32000],
      [base, null, 401, -32000],
      [{ ...base, 'content-type': 'text/plain' }, all, 415, -32000],
      [{ ...base, 'mcp-protocol-version': '2026-07-28', 'mcp-method': undefined }, all, 400, -32020],
    ] satisfies [Record<string, string | undefined>, Principal | null, number, number][]) {
      expect(await guardStream({ headers }, { method: 'events/stream', id: 'test' }, sessions, constants, async () => principal)).toMatchObject({ status, body: { jsonrpc: '2.0', id: 'test', error: { code } } });
    }
    expect(await guardStream({ headers: base }, { method: 'events/stream', id: 1 }, sessions, constants, async () => all)).toBeNull();
    expect(await guardStream({ headers: { ...base, 'mcp-protocol-version': undefined } }, { method: 'events/stream', id: 1 }, sessions, constants, async () => all)).toBeNull();
  } finally { sessions.dispose(); }
});

test('HTTP guard rejects invalid transport headers and null principals without opening a stream', async () => {
  const t = await target({ principal: (req) => req.headers.authorization === 'denied' ? null : all });
  for (const [headers, status, code] of [
    [{ ...modern, 'Mcp-Method': 'wrong' }, 400, -32020],
    [{ ...modern, 'MCP-Protocol-Version': 'invalid' }, 400, -32000],
    [{ ...legacy, 'Mcp-Session-Id': 'unknown' }, 404, -32001],
    [legacy, 400, -32000],
    [{ ...modern, Accept: 'application/json' }, 406, -32000],
    [{ ...modern, Authorization: 'denied' }, 401, -32000],
    [{ ...modern, 'Content-Type': 'text/plain' }, 415, -32000],
  ] satisfies [Record<string, string>, number, number][]) {
    const response = await fetch(t.url, { method: 'POST', headers, body: JSON.stringify({ jsonrpc: '2.0', id: 7, method: 'events/stream', params: { name } }) });
    expect(response.status).toBe(status);
    expect(await response.json()).toMatchObject({ jsonrpc: '2.0', id: 7, error: { code } });
    expect(t.context.fanout.size).toBe(0);
  }
  const accepted = await stream(t.url); await eventually(() => accepted.frames.length, 1);
  expect(accepted.res.statusCode).toBe(200);
});

for (const id of [23, 'string-id']) test(`SSE active, event and heartbeat frames preserve ${id}`, async () => {
  const t = await target({ heartbeatMs: 50 });
  const s = await stream(t.url, id);
  await eventually(() => s.frames.length >= 2, true);
  expect(s.frames[0]).toEqual({ jsonrpc: '2.0', method: 'notifications/events/active', params: { cursor: `v1.${await t.context.epoch()}.`, truncated: false, _meta: { [SUBSCRIPTION_ID_META]: id } } });
  await t.commit([append('row')]); await eventually(s.ids, ['row']);
  for (const frame of s.frames) expect(frame.params?._meta[SUBSCRIPTION_ID_META]).toBe(id);
  expect(s.frames.some((f) => f.method === 'notifications/events/heartbeat')).toBe(true);
  expect(s.chunks.join('')).not.toMatch(/(?:^|\n):/);
  expect(s.res.headers['cache-control']).toBe('no-cache, no-transform');
});

test('slow localhost reader bounds owned bytes and resumes every row exactly once', async () => {
  const budget = 16_384;
  const t = await target({ streamBufferBytes: budget });
  const s = await stream(t.url, 7, { arguments: { include: 'events' } });
  await eventually(() => s.frames.length, 1);
  s.res.pause(); s.res.socket?.pause();
  const writes = vi.spyOn(t.responses[0]!, 'write');
  const rows = Array.from({ length: 3000 }, (_, i) => append(`row-${i}`, 8192));
  global.gc?.();
  const before = process.memoryUsage().heapUsed;
  await t.commit(rows);
  await eventually(() => t.context.fanout.checked(), '3000');
  // Notice includes the text too; a frame holds at most two payload copies plus 2 KiB of fields.
  const maxFrameBytes = 2 * 8192 + 2048;
  const fixedSlack = 2 * 1024 * 1024;
  await eventually(() => streamUsage(t.context).pageRows > 0 && streamUsage(t.context).writeBytes > 0, true);
  const usage = streamUsage(t.context);
  if (global.gc) {
    global.gc();
    expect(process.memoryUsage().heapUsed - before).toBeLessThanOrEqual(budget + 256 * maxFrameBytes + fixedSlack);
  }
  expect(usage.count).toBe(1);
  expect(usage.queuedBytes).toBeLessThanOrEqual(budget);
  expect(usage.pageRows).toBeLessThanOrEqual(256);
  expect(usage.writeBytes).toBeLessThanOrEqual(maxFrameBytes);
  expect(usage.queuedBytes + Math.max(usage.pageRows, 1) * maxFrameBytes).toBeLessThanOrEqual(budget + 256 * maxFrameBytes + fixedSlack);
  expect(writes.mock.results.some((result) => result.value === false)).toBe(true);
  expect(s.ids()).toEqual([]);
  s.res.socket?.resume(); s.res.resume();
  await eventually(s.ids, Array.from({ length: 3000 }, (_, i) => `row-${i}`));
  expect(s.frames.filter((f) => f.method === 'notifications/events/active').map((f) => f.params?.truncated)).toEqual([false]);
}, 30_000);

test('destroying a socket frees the stream quota and fanout without cancellation', async () => {
  const t = await target({ maxStreamsPerPrincipal: 1 });
  const s = await stream(t.url);
  await eventually(() => s.frames.length, 1);
  s.res.destroy(); s.req.destroy();
  await t.commit([append('one-more-write')]);
  await eventually(() => streamUsage(t.context).count, 0);
  expect(t.context.fanout.size).toBe(0);
  expect(t.responses[0]?.destroyed).toBe(true);
  const next = await stream(t.url, 'next'); await eventually(() => next.frames.length, 1);
  expect(next.res.statusCode).toBe(200);
  expect(next.frames[0]?.method).toBe('notifications/events/active');
});

test('session table caps, refreshes, expires and disposes entries', () => {
  vi.useFakeTimers();
  const closed: string[] = [];
  const sessions = createSessions({ max: 1, idleMs: 100, onClose: (id) => closed.push(id) });
  const first = sessions.open()!;
  expect(typeof first).toBe('string'); expect(first.length).toBeGreaterThan(0);
  expect(sessions.open()).toBeNull();
  vi.advanceTimersByTime(60); expect(sessions.touch(first)).toBe(true);
  vi.advanceTimersByTime(60); expect(sessions.touch(first)).toBe(true);
  vi.advanceTimersByTime(101); expect(sessions.touch(first)).toBe(false);
  expect(closed).toEqual([first]);
  const second = sessions.open()!; sessions.close(second);
  expect(sessions.touch(second)).toBe(false);
  expect(closed).toEqual([first, second]); sessions.dispose();
});

test('HTTP initialize uses capped sessions, binds principals and DELETE closes the server', async () => {
  const closed: number[] = [];
  let index = 0;
  const t = await target({ principal: (req) => ({ id: String(req.headers.authorization ?? 'local'), scopes: [''] }) }, { sessions: { max: 1 }, newServer: () => {
    const server = new Server({ name: 'owned', version: '1' }, { capabilities: {} });
    const id = index++; const close = server.close.bind(server); server.close = async () => { closed.push(id); await close(); }; return server;
  } });
  const first = await t.initialize(); expect(first.status).toBe(200); expect(first.session).toBeTruthy();
  const capped = await t.initialize(); expect(capped.status).toBe(503);
  const wrong = await t.post('events/list', {}, { ...legacy, 'Mcp-Session-Id': first.session, Authorization: 'other' });
  expect(wrong.status).toBe(403); await wrong.text();
  const deleted = await fetch(t.url, { method: 'DELETE', headers: { 'Mcp-Session-Id': first.session, 'MCP-Protocol-Version': '2025-11-25' } });
  expect(deleted.status).toBe(200); await deleted.text();
  await eventually(() => closed.includes(0), true);
  const stale = await t.post('events/list', {}, { ...legacy, 'Mcp-Session-Id': first.session });
  expect(stale.status).toBe(404); await stale.text();
  expect((await t.initialize()).status).toBe(200);
});

test('HTTP idle expiry closes its SDK server and rejects the next request', async () => {
  let closed = false;
  const t = await target({}, { sessions: { idleMs: 30 }, newServer: () => {
    const server = new Server({ name: 'idle', version: '1' }, { capabilities: {} }); const close = server.close.bind(server);
    server.close = async () => { closed = true; await close(); }; return server;
  } });
  const first = await t.initialize(); expect(first.status).toBe(200);
  await eventually(() => closed, true);
  const response = await t.post('events/list', {}, { ...legacy, 'Mcp-Session-Id': first.session });
  expect(response.status).toBe(404); expect(await response.json()).toMatchObject({ error: { code: -32001 } });
});

for (const era of ['legacy', 'modern'] as const) test(`real SDK Client uses SDK methods and the owned stream in ${era}`, async () => {
  const t = await target({ heartbeatMs: 50 });
  const client = new Client({ name: 'http-test', version: '1' }, era === 'modern' ? { versionNegotiation: { mode: { pin: '2026-07-28' } } } : {});
  const wire: Frame[] = [];
  const transport = new StreamableHTTPClientTransport(new URL(t.url), { fetch: async (url, init) => {
    const response = await fetch(url, init);
    if (typeof init?.body === 'string' && ['events/list', 'events/poll'].includes(JSON.parse(init.body).method)) {
      for (const line of (await response.clone().text()).split('\n')) {
        if (line.startsWith('data: ')) wire.push(JSON.parse(line.slice(6)));
        else if (line.startsWith('{')) wire.push(JSON.parse(line));
      }
    }
    return response;
  } });
  const notifications: string[] = [];
  client.fallbackNotificationHandler = async (frame) => { notifications.push(frame.method); };
  cleanup.push(() => client.close());
  await client.connect(transport);
  expect(Boolean(transport.sessionId)).toBe(era === 'legacy');
  for (const method of ['events/list', 'events/poll']) {
    const result = await client.request({ method, params: method === 'events/poll' ? { name } : {} }, schema);
    expect(result).toBeDefined();
    expect(wire.at(-1)?.result?.resultType).toBe('complete');
  }
  const result = client.request({ method: 'events/stream', params: { name } }, schema);
  await eventually(() => notifications.includes('notifications/events/active'), true);
  expect(t.context.fanout.size).toBe(1);
  await t.shutdown();
  expect(await result).toMatchObject({ _meta: { [SUBSCRIPTION_ID_META]: expect.any(Number) } });
});

test('extra routes, unknown paths, parse errors, body limit and wrong methods', async () => {
  const t = await target({}, { routes: { 'POST /v1/ingest': (_req, res) => { res.writeHead(201); res.end('extra'); } } });
  const extra = await fetch(t.url.replace('/mcp', '/v1/ingest'), { method: 'POST' }); expect(extra.status).toBe(201); expect(await extra.text()).toBe('extra');
  const missing = await fetch(t.url.replace('/mcp', '/missing')); expect(missing.status).toBe(404); await missing.text();
  const invalid = await fetch(t.url, { method: 'POST', headers: modern, body: '{' }); expect(invalid.status).toBe(400); expect(await invalid.json()).toMatchObject({ error: { code: -32700 } });
  const oversized = await fetch(t.url, { method: 'POST', headers: modern, body: 'x'.repeat(4 * 1024 * 1024 + 1) }); expect(oversized.status).toBe(413); await oversized.text();
  const session = await t.initialize();
  for (const headers of [{}, { 'Mcp-Session-Id': session.session }]) {
    const wrong = await fetch(t.url, { method: 'PUT', headers }); expect(wrong.status).toBe(405); await wrong.text();
  }
});

test('subscription errors remain JSON-RPC errors with HTTP 200 before SSE opens', async () => {
  const t = await target();
  const response = await t.post('events/stream', { name: 'unknown' });
  expect(response.status).toBe(200); expect(response.headers.get('content-type')).toBe('application/json');
  expect(await response.json()).toEqual({ jsonrpc: '2.0', id: 1, error: { code: -32023, message: 'Unknown event', data: { kind: 'event' } } });
  expect(t.context.fanout.size).toBe(0);
});

test('authorization recheck terminates revoked grants without a final response', async () => {
  let principal: Principal | null = all;
  const t = await target({ principal: () => principal });
  vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] });
  const s = await stream(t.url, 'revoked', { arguments: { scope: 's' } });
  await eventually(() => s.frames.length, 1);
  principal = { id: 'local', scopes: ['other/'] };
  await vi.advanceTimersByTimeAsync(60_000);
  await eventually(s.ended, true);
  expect(s.frames.map((f) => f.method)).toEqual(['notifications/events/active', 'notifications/events/terminated']);
  expect(s.frames[1]?.params?.error?.code).toBe(-32024);
  expect(s.frames.filter((f) => f.result)).toEqual([]);
  expect(t.context.fanout.size).toBe(0);
});

test('notifications/cancelled closes the intercepted stream without a final result', async () => {
  const t = await target(); const session = await t.initialize();
  const headers = { ...legacy, 'Mcp-Session-Id': session.session };
  const s = await stream(t.url, 'cancel-me', {}, headers); await eventually(() => s.frames.length, 1);
  const response = await fetch(t.url, { method: 'POST', headers, body: JSON.stringify({ jsonrpc: '2.0', method: 'notifications/cancelled', params: { requestId: 'cancel-me' } }) });
  expect(response.status).toBe(202); await response.text();
  await eventually(s.ended, true);
  expect(s.frames.map((f) => f.method)).toEqual(['notifications/events/active']);
  expect(t.context.fanout.size).toBe(0);
});

test('EventsServer.close writes the final complete JSON-RPC response', async () => {
  const t = await target({}, {}, true);
  const s = await stream(t.url, 'shutdown'); await eventually(() => s.frames.length, 1);
  await t.events.close(); await eventually(s.ended, true);
  expect(s.frames.at(-1)).toEqual({ jsonrpc: '2.0', id: 'shutdown', result: { resultType: 'complete', _meta: { [SUBSCRIPTION_ID_META]: 'shutdown' } } });
  expect(s.frames.map((f) => f.method).filter(Boolean)).toEqual(['notifications/events/active']);
});
