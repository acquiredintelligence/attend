import { expect, test } from 'vitest';
import { Server, InMemoryTransport } from '@modelcontextprotocol/server';
import { Client } from '@modelcontextprotocol/client';
import { memoryStore } from '../../src/store/memory.js';
import { eventsServer } from '../../src/server/install.js';
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
