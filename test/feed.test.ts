import { expect, test } from 'vitest';
import { Server, InMemoryTransport } from '@modelcontextprotocol/server';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { decisionFeed, type DecisionFeedItem, type DecisionFeedOptions } from '../src/mcp.js';
import { PermanentError } from '../src/errors.js';
import { memoryStore } from '../src/store/memory.js';
import { eventsServer } from '../src/server/install.js';
import type { DecisionAppend } from '../src/core/types.js';
import type { McpServerLike } from '../src/server/types.js';

function decision(id: string, scope = 's'): DecisionAppend {
  const at = Date.now();
  return { kind: 'decision', id, scope, at, route: 'interrupt', body: { id, scope, at, route: 'interrupt', confidence: null, predicates: {}, probabilities: { interrupt: 1, queue: 0, note: 0, ask: 0, skip: 0, wait: 0 }, window: { id, events: [{ id: `e:${id}`, at, name: 'changed', data: {}, kind: 'closing', seenAt: at }], reason: 'closing', waits: 0, gap: false, dropped: 0, settledAt: at } } };
}

async function setup(script?: NonNullable<McpServerLike['fallbackRequestHandler']>) {
  const store = memoryStore();
  const server = new Server({ name: 'test', version: '1' }, { capabilities: {} });
  const events = eventsServer(store.log, { poll: { nextPollMs: 20 } });
  events.install(server);
  let polls = 0;
  const original = server.fallbackRequestHandler!.bind(server);
  server.fallbackRequestHandler = async (request, ctx) => {
    if (request.method === 'events/poll') polls += 1;
    return script ? script({ ...request, params: request.params ?? {} }, ctx) : original(request, ctx);
  };
  const client = new Client({ name: 'test', version: '1' });
  const [a, b] = InMemoryTransport.createLinkedPair();
  await server.connect(a);
  await client.connect(b);
  async function commit(rows: DecisionAppend[]) {
    const scope = rows[0]!.scope;
    expect(await store.commit([{ scope, expectedVersion: (await store.load(scope))?.version ?? 0, inputs: [{ input: { kind: 'flush' }, now: Date.now() }], appends: rows, snapshot: null }])).toEqual(['ok']);
  }
  return { store, client, commit, polls: () => polls, async close() { await client.close(); await server.close(); await events.close(); } };
}

function iterator(client: Client, options: Partial<DecisionFeedOptions> = {}) {
  const abort = new AbortController();
  const feed = decisionFeed(client, { cursor: null, mode: 'poll', signal: abort.signal, ...options })[Symbol.asyncIterator]();
  return { feed, abort };
}
async function take(feed: AsyncIterator<DecisionFeedItem>) {
  const result = await feed.next();
  expect(result.done).toBe(false);
  return result.value as DecisionFeedItem;
}
const delay = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

async function until(check: () => boolean) {
  const deadline = Date.now() + 2000;
  while (!check()) {
    if (Date.now() > deadline) throw new Error('Condition did not become true');
    await delay(5);
  }
}

test('poll feed delivers decisions and resumes from the persisted cursor without duplicates', async () => {
  const t = await setup();
  const first = iterator(t.client);
  try {
    const start = await take(first.feed);
    expect(start).toMatchObject({ kind: 'cursor', truncated: false });
    await t.commit([decision('a')]);
    const item = await take(first.feed);
    expect(item).toMatchObject({ kind: 'decision', eventId: 'a', data: { route: 'interrupt' } });
    if (item.kind !== 'decision') throw new Error('Expected decision');
    expect(item.data.notice).toContain('Attend to this now');
    expect(item.cursor).not.toBe(start.cursor);
    await first.feed.return?.();
    await t.commit([decision('b')]);
    const second = iterator(t.client, { cursor: item.cursor });
    try { expect(await take(second.feed)).toMatchObject({ kind: 'decision', eventId: 'b' }); }
    finally { await second.feed.return?.(); }
  } finally { first.abort.abort(); await first.feed.return?.(); await t.close(); }
});

test('quiet filtered polls yield cursor movement', async () => {
  const t = await setup();
  const { feed, abort } = iterator(t.client, { arguments: { scope: 'mine' } });
  try {
    const start = await take(feed);
    await t.commit([decision('other', 'other')]);
    const item = await take(feed);
    expect(item).toMatchObject({ kind: 'cursor', truncated: false });
    expect(item.cursor).not.toBe(start.cursor);
  } finally { abort.abort(); await feed.return?.(); await t.close(); }
});

test('an unknown epoch yields a truncated cursor before replayed decisions', async () => {
  const t = await setup();
  await t.commit([decision('a')]);
  const { feed, abort } = iterator(t.client, { cursor: 'v1.old.7' });
  try {
    expect(await take(feed)).toEqual({ kind: 'cursor', cursor: 'v1.old.7', truncated: true });
    expect(await take(feed)).toMatchObject({ kind: 'decision', eventId: 'a' });
  } finally { abort.abort(); await feed.return?.(); await t.close(); }
});

test('dead letters arrive as deadLetter items', async () => {
  const t = await setup();
  const row = decision('letter');
  await t.commit([{ kind: 'deadLetter', id: row.id, scope: row.scope, at: row.at, route: null, body: { id: row.id, scope: row.scope, at: row.at, window: row.body.window, decision: null, stage: 'judge', message: 'failed', permanent: true, attempts: 3 } }]);
  const { feed, abort } = iterator(t.client, { cursor: `v1.${await t.store.log.epoch()}.`, name: 'attention.deadLetter' });
  try { expect(await take(feed)).toMatchObject({ kind: 'deadLetter', eventId: 'letter', data: { letterId: 'letter', stage: 'judge', message: 'failed' } }); }
  finally { abort.abort(); await feed.return?.(); await t.close(); }
});

test('a batch advances its cursor only on the last item and backpressures polling', async () => {
  const t = await setup();
  const cursor = `v1.${await t.store.log.epoch()}.`;
  await t.commit([decision('a'), decision('b'), decision('c')]);
  const { feed, abort } = iterator(t.client, { cursor });
  try {
    expect(await take(feed)).toMatchObject({ eventId: 'a', cursor });
    const polls = t.polls();
    await delay(80);
    expect(t.polls()).toBe(polls);
    expect(await take(feed)).toMatchObject({ eventId: 'b', cursor });
    const last = await take(feed);
    expect(last).toMatchObject({ eventId: 'c' });
    expect(last.cursor).not.toBe(cursor);
    await delay(80);
    expect(t.polls()).toBe(polls);
  } finally { abort.abort(); await feed.return?.(); await t.close(); }
});

test('aborting the signal ends iteration normally while a poll is quiet', async () => {
  const t = await setup();
  const { feed, abort } = iterator(t.client);
  try {
    await take(feed);
    const next = feed.next();
    await delay(30);
    abort.abort();
    expect(await next).toEqual({ done: true, value: undefined });
  } finally { abort.abort(); await feed.return?.(); await t.close(); }
});

test('breaking for await stops all subsequent polls', async () => {
  const t = await setup();
  const abort = new AbortController();
  try {
    for await (const item of decisionFeed(t.client, { mode: 'poll', cursor: null, signal: abort.signal })) {
      expect(item.kind).toBe('cursor');
      break;
    }
    const polls = t.polls();
    await delay(100);
    expect(t.polls()).toBe(polls);
  } finally { abort.abort(); await t.close(); }
});

test('an unknown event name rejects with PermanentError', async () => {
  const t = await setup();
  const { feed, abort } = iterator(t.client, { name: 'unknown' as 'attention.decision' });
  try { await expect(feed.next()).rejects.toBeInstanceOf(PermanentError); }
  finally { abort.abort(); await feed.return?.(); await t.close(); }
});

for (const mode of ['stream', 'auto'] as const) test(`${mode} feed yields active and heartbeat cursors around event batches`, async () => {
  let ended = false;
  const t = await setup(async (request, ctx) => {
    if (request.method === 'events/list') return { resultType: 'complete', events: [{ name: 'attention.decision', delivery: ['push'] }] };
    if (request.method !== 'events/stream') throw new Error('Unexpected method');
    const meta = { 'io.modelcontextprotocol/subscriptionId': ctx.mcpReq.id };
    await ctx.mcpReq.notify({ method: 'notifications/events/active', params: { cursor: 'active', truncated: false, _meta: meta } });
    await ctx.mcpReq.notify({ method: 'notifications/events/event', params: { eventId: 'a', name: 'attention.decision', timestamp: new Date().toISOString(), data: { notice: 'a' }, cursor: 'event-a', _meta: meta } });
    await ctx.mcpReq.notify({ method: 'notifications/events/heartbeat', params: { cursor: 'event-a', _meta: meta } });
    await ctx.mcpReq.notify({ method: 'notifications/events/event', params: { eventId: 'b', name: 'attention.decision', timestamp: new Date().toISOString(), data: { notice: 'b' }, cursor: 'event-b', _meta: meta } });
    await ctx.mcpReq.notify({ method: 'notifications/events/heartbeat', params: { cursor: 'heartbeat', _meta: meta } });
    await new Promise<void>((resolve) => {
      if (ctx.mcpReq.signal.aborted) resolve();
      else ctx.mcpReq.signal.addEventListener('abort', () => resolve(), { once: true });
    });
    ended = true;
    return { resultType: 'complete' };
  });
  const { feed, abort } = iterator(t.client, { mode });
  try {
    expect(await take(feed)).toEqual({ kind: 'cursor', cursor: 'active', truncated: false });
    expect(await take(feed)).toMatchObject({ kind: 'decision', eventId: 'a', cursor: 'event-a' });
    expect(await take(feed)).toMatchObject({ kind: 'decision', eventId: 'b', cursor: 'event-b' });
    expect(await take(feed)).toEqual({ kind: 'cursor', cursor: 'heartbeat', truncated: false });
    await feed.return?.();
    await until(() => ended);
  } finally { abort.abort(); await feed.return?.(); await t.close(); }
});

test('retryable errors resume from the last handed-out cursor', async () => {
  let calls = 0;
  const cursors: unknown[] = [];
  const t = await setup(async (request) => {
    calls += 1;
    cursors.push(request.params?.cursor);
    if (calls === 2) throw Object.assign(new Error('Try again'), { code: -32025 });
    return { resultType: 'complete', events: [], cursor: calls === 1 ? 'saved' : 'resumed', truncated: false, hasMore: false, nextPollMs: 1 };
  });
  const { feed, abort } = iterator(t.client);
  try {
    expect(await take(feed)).toEqual({ kind: 'cursor', cursor: 'saved', truncated: false });
    expect(await take(feed)).toEqual({ kind: 'cursor', cursor: 'resumed', truncated: false });
    expect(cursors).toEqual([null, 'saved', 'saved']);
  } finally { abort.abort(); await feed.return?.(); await t.close(); }
});

for (const known of [true, false]) test(`unknown occurrences preserve cursor progress with known events ${known}`, async () => {
  const t = await setup(async () => ({ resultType: 'complete', events: [
    ...(known ? [{ eventId: 'a', name: 'attention.decision', timestamp: new Date().toISOString(), data: { notice: 'a' } }] : []),
    { eventId: 'unknown', name: 'another.event', timestamp: new Date().toISOString(), data: {} },
  ], cursor: 'after', hasMore: false, truncated: false }));
  const { feed, abort } = iterator(t.client, { cursor: 'before' });
  try {
    expect(await take(feed)).toMatchObject(known ? { kind: 'decision', eventId: 'a', cursor: 'after' } : { kind: 'cursor', cursor: 'after', truncated: false });
  } finally { abort.abort(); await feed.return?.(); await t.close(); }
});
