import { expect, test } from 'vitest';
import { Server, McpServer, InMemoryTransport, createMcpHandler } from '@modelcontextprotocol/server';
import { Client, StreamableHTTPClientTransport } from '@modelcontextprotocol/client';
import { z } from 'zod/v4';
import { memoryStore } from '../../src/store/memory.js';
import { eventsServer } from '../../src/server/install.js';
import { listTools } from '../../src/server/tools.js';
import type { DecisionAppend } from '../../src/core/types.js';

async function setup() {
  const store = memoryStore();
  const events = eventsServer(store.log);
  const server = new Server({ name: 'test', version: '1' }, { capabilities: {} });
  events.install(server, { principal: { id: 'a', scopes: ['a/'] } });
  const client = new Client({ name: 'test', version: '1' });
  const [a, b] = InMemoryTransport.createLinkedPair();
  await server.connect(a);
  await client.connect(b);
  async function commit(ids: string[], scope = 'a/s') {
    const at = Date.now();
    const appends: DecisionAppend[] = ids.map((id) => ({ kind: 'decision', id, scope, at, route: 'interrupt', body: { id, scope, at, route: 'interrupt', probabilities: { interrupt: 1, queue: 0, note: 0, ask: 0, skip: 0, wait: 0 }, predicates: {}, confidence: null, window: { id, events: [{ id: `e:${id}`, at, name: 'changed', data: {}, kind: 'closing', seenAt: at }], reason: 'closing', settledAt: at, waits: 0, gap: false, dropped: 0 } } }));
    expect(await store.commit([{ scope, expectedVersion: (await store.load(scope))?.version ?? 0, inputs: [{ input: { kind: 'flush' }, now: at }], appends, snapshot: null }])).toEqual(['ok']);
    return appends;
  }
  return { client, commit, async close() { await client.close(); await server.close(); await events.close(); } };
}

function content(result: Awaited<ReturnType<Client['callTool']>>) { return result.structuredContent as { events: { eventId: string }[]; cursor: string; truncated: boolean; error?: { code: number }; }; }

test('the two tools advertise strict object schemas', async () => {
  const t = await setup();
  try {
    const result = await t.client.listTools();
    expect(result.tools.map((tool) => tool.name)).toEqual(['attend_wait', 'attend_window']);
    for (const tool of result.tools) expect(tool.inputSchema).toMatchObject({ type: 'object', additionalProperties: false });
    expect(result.tools[1]!.inputSchema.required).toEqual(['decisionId']);
    expect(result.tools[0]!.inputSchema.properties?.arguments).toEqual({ type: 'object' });
  } finally { await t.close(); }
});

test('attend_wait returns new rows and waits for a commit', async () => {
  const t = await setup();
  try {
    const initial = content(await t.client.callTool({ name: 'attend_wait', arguments: { waitMs: 0 } }));
    expect(initial.events).toEqual([]);
    await t.commit(['a']);
    const result = content(await t.client.callTool({ name: 'attend_wait', arguments: { cursor: initial.cursor, waitMs: 0 } }));
    expect(Object.keys(result).sort()).toEqual(['cursor', 'events', 'truncated']);
    expect(result.events.map((e) => e.eventId)).toEqual(['a']);
    const timer = setTimeout(() => { void t.commit(['b']); }, 100);
    try {
      const waited = content(await t.client.callTool({ name: 'attend_wait', arguments: { cursor: result.cursor, waitMs: 2000 } }));
      expect(waited.events.map((e) => e.eventId)).toEqual(['b']);
    } finally { clearTimeout(timer); }
  } finally { await t.close(); }
});

test('attend_wait clamps maxEvents to 20', async () => {
  const t = await setup();
  try {
    const initial = content(await t.client.callTool({ name: 'attend_wait', arguments: { waitMs: 0 } }));
    await t.commit(Array.from({ length: 25 }, (_, i) => String(i)));
    expect(content(await t.client.callTool({ name: 'attend_wait', arguments: { cursor: initial.cursor, waitMs: 0, maxEvents: 50 } })).events.map((e) => e.eventId)).toEqual(Array.from({ length: 20 }, (_, i) => String(i)));
  } finally { await t.close(); }
});

test('bad tool arguments are tool errors and an unknown tool is a JSON-RPC error', async () => {
  const t = await setup();
  try {
    const result = await t.client.callTool({ name: 'attend_wait', arguments: { arguments: 'invalid' } });
    expect(result.isError).toBe(true);
    expect(content(result).error?.code).toBe(-32602);
    await expect(t.client.callTool({ name: 'missing' })).rejects.toMatchObject({ code: -32602 });
  } finally { await t.close(); }
});

test('attend_window returns entries and conceals unknown and out-of-grant ids alike', async () => {
  const t = await setup();
  try {
    const [row] = await t.commit(['allowed']);
    await t.commit(['hidden'], 'b/s');
    const result = await t.client.callTool({ name: 'attend_window', arguments: { decisionId: 'allowed' } });
    expect((result.structuredContent as { events: unknown }).events).toEqual(row!.body.window.events);
    const missing = await t.client.callTool({ name: 'attend_window', arguments: { decisionId: 'missing' } });
    expect(missing.isError).toBe(true);
    expect(content(missing).error?.code).toBe(-32023);
    expect(await t.client.callTool({ name: 'attend_window', arguments: { decisionId: 'hidden' } })).toEqual(missing);
  } finally { await t.close(); }
});

for (const args of [{ maxEvents: 20.5 }, { maxEvents: 0 }, { waitMs: -1 }, { waitMs: 30000.5 }, { bogus: true }]) test(`tool arguments reject ${JSON.stringify(args)}`, async () => {
  const t = await setup();
  try {
    const result = await t.client.callTool({ name: 'attend_wait', arguments: args });
    expect(result.isError).toBe(true);
    expect(content(result).error?.code).toBe(-32602);
  } finally { await t.close(); }
});

const passthrough = z.looseObject({});

async function coexistence(kind: 'mcp' | 'low' | 'inner', when: 'before' | 'after', options: { modern?: boolean; tools?: boolean; paging?: boolean } = {}) {
  const store = memoryStore();
  const events = eventsServer(store.log, { tools: options.tools ?? true });
  const principal = { id: 'a', scopes: ['a/'] };
  function create() {
    const server = kind === 'low'
      ? new Server({ name: 'app', version: '1' }, { capabilities: when === 'before' || options.tools === false ? { tools: {} } : {} })
      : new McpServer({ name: 'app', version: '1' });
    const low = server instanceof McpServer ? server.server : server;
    const register = () => {
      if (server instanceof McpServer) {
        server.registerTool('app_tool', { description: 'App tool', inputSchema: z.object({ q: z.string() }) }, async ({ q }) => ({ content: [{ type: 'text', text: `app ${q}` }] }));
      } else {
        server.setRequestHandler('tools/list', async (request) => options.paging && request.params?.cursor === '2'
          ? { tools: [{ name: 'page_two', inputSchema: { type: 'object' } }] }
          : { tools: [{ name: 'app_tool', inputSchema: { type: 'object' } }], ...(options.paging ? { nextCursor: '2' } : {}) });
        server.setRequestHandler('tools/call', async (request) => ({ content: [{ type: 'text', text: `app ${request.params.name}` }] }));
      }
    };
    if (when === 'before') register();
    const set = low.setRequestHandler;
    events.install(kind === 'inner' ? low : server, { principal });
    if (options.tools === false) expect(low.setRequestHandler).toBe(set);
    if (when === 'after') register();
    return server;
  }
  const client = new Client({ name: 'test', version: '1' }, options.modern ? { versionNegotiation: { mode: { pin: '2026-07-28' } } } : {});
  const handler = options.modern ? createMcpHandler(create) : undefined;
  const server = handler ? undefined : create();
  if (handler) {
    await client.connect(new StreamableHTTPClientTransport(new URL('http://attend.test/mcp'), { fetch: (url, init) => handler.fetch(new Request(url, init)) }));
  } else {
    const [a, b] = InMemoryTransport.createLinkedPair();
    await server!.connect(a);
    await client.connect(b);
  }
  async function commit() {
    const row: DecisionAppend = { kind: 'decision', id: 'a', scope: 'a/s', at: 1000, route: 'interrupt', body: {
      id: 'a', scope: 'a/s', at: 1000, route: 'interrupt', probabilities: { interrupt: 1, queue: 0, note: 0, ask: 0, skip: 0, wait: 0 }, predicates: {}, confidence: null,
      window: { id: 'a', events: [{ id: 'e:a', at: 998, name: 'changed', data: {}, kind: 'closing', seenAt: 999 }], reason: 'closing', settledAt: 999, waits: 0, gap: false, dropped: 0 },
    } };
    expect(await store.commit([{ scope: 'a/s', expectedVersion: 0, inputs: [{ input: { kind: 'flush' }, now: 1000 }], appends: [row], snapshot: null }])).toEqual(['ok']);
  }
  return { client, server, commit, async close() { await client.close(); await handler?.close(); await server?.close(); await events.close(); await store.close?.(); } };
}

async function checkCoexistence(t: Awaited<ReturnType<typeof coexistence>>, appText: string) {
  const listed = await t.client.request({ method: 'tools/list', params: {} });
  expect(listed.tools.map((tool) => tool.name)).toEqual(['app_tool', 'attend_wait', 'attend_window']);
  expect(listed.tools.slice(1).map((tool) => tool.inputSchema)).toEqual(listTools().map((tool) => tool.inputSchema));
  expect((await t.client.callTool({ name: 'app_tool', arguments: { q: 'hello' } })).content).toEqual([{ type: 'text', text: appText }]);
  const initial = content(await t.client.callTool({ name: 'attend_wait', arguments: { cursor: null, waitMs: 0 } }));
  expect(initial.events).toEqual([]);
  await t.commit();
  const result = content(await t.client.callTool({ name: 'attend_wait', arguments: { cursor: initial.cursor, waitMs: 0 } }));
  expect(result.events.map((event) => event.eventId)).toEqual(['a']);
  const window = await t.client.callTool({ name: 'attend_window', arguments: { decisionId: 'a' } });
  expect((window.structuredContent as { events: unknown }).events).toEqual([{ id: 'e:a', at: 998, name: 'changed', data: {}, kind: 'closing', seenAt: 999 }]);
  const polled = await t.client.request({ method: 'events/poll', params: { name: 'attention.decision', cursor: initial.cursor } }, passthrough);
  expect((polled.events as { eventId: string }[]).map((event) => event.eventId)).toEqual(['a']);
}

test('McpServer tools coexist with app tools registered before and after install', async () => {
  const t = await coexistence('mcp', 'before');
  try {
    await checkCoexistence(t, 'app hello');
    if (!(t.server instanceof McpServer)) throw new Error('Expected McpServer');
    t.server.registerTool('late_tool', { description: 'Late tool', inputSchema: z.object({ q: z.string() }) }, async ({ q }) => ({ content: [{ type: 'text', text: `late ${q}` }] }));
    expect((await t.client.listTools()).tools.map((tool) => tool.name)).toEqual(['app_tool', 'attend_wait', 'attend_window', 'late_tool']);
    expect((await t.client.callTool({ name: 'late_tool', arguments: { q: 'hello' } })).content).toEqual([{ type: 'text', text: 'late hello' }]);
  } finally { await t.close(); }
});

test('McpServer tools coexist over the 2026-07-28 HTTP entry', async () => {
  const t = await coexistence('mcp', 'before', { modern: true });
  try { await checkCoexistence(t, 'app hello'); }
  finally { await t.close(); }
});

for (const when of ['before', 'after'] as const) test(`low-level Server tools coexist with app handlers set ${when} install`, async () => {
  const t = await coexistence('low', when);
  try { await checkCoexistence(t, 'app app_tool'); }
  finally { await t.close(); }
});

for (const when of ['before', 'after'] as const) test(`install(mcp.server) composes app tools registered ${when} install`, async () => {
  const t = await coexistence('inner', when);
  try { await checkCoexistence(t, 'app hello'); }
  finally { await t.close(); }
});

test('a paging app lists attend tools on its first page only', async () => {
  const t = await coexistence('low', 'before', { paging: true });
  try {
    await checkCoexistence(t, 'app app_tool');
    const first = await t.client.request({ method: 'tools/list', params: {} });
    expect(first.nextCursor).toBe('2');
    expect((await t.client.listTools({ cursor: '2' })).tools.map((tool) => tool.name)).toEqual(['page_two']);
  } finally { await t.close(); }
});

for (const kind of ['mcp', 'low'] as const) test(`tools false leaves the ${kind} app registry and handlers alone`, async () => {
  const t = await coexistence(kind, 'before', { tools: false });
  try {
    expect((await t.client.listTools()).tools.map((tool) => tool.name)).toEqual(['app_tool']);
    expect((await t.client.callTool({ name: 'app_tool', arguments: { q: 'hello' } })).content).toEqual([{ type: 'text', text: kind === 'mcp' ? 'app hello' : 'app app_tool' }]);
  } finally { await t.close(); }
});
