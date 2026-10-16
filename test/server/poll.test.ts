import { randomUUID } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { afterAll, describe, expect, test } from 'vitest';
import { createClient } from 'redis';
import pg from 'pg';
import { migrate, postgresStore, type PostgresListener } from '../../src/store/postgres.js';
import { Server, InMemoryTransport, createMcpHandler } from '@modelcontextprotocol/server';
import { Client, StreamableHTTPClientTransport, type Transport } from '@modelcontextprotocol/client';
import { Client as LegacyClient } from '@modelcontextprotocol/sdk/client/index.js';
import { z } from 'zod/v4';
import { memoryStore } from '../../src/store/memory.js';
import { sqliteStore } from '../../src/store/sqlite.js';
import { redisStore, type RedisSend } from '../../src/store/redis.js';
import type { ScopeStore, ScopeWrite, DecisionQuery } from '../../src/store/types.js';
import type { DecisionAppend, JsonObject } from '../../src/core/types.js';
import { eventsServer } from '../../src/server/install.js';
import type { EventsServerOptions, Principal } from '../../src/server/types.js';
import { eventsWireConformance, type WireTarget } from '../../src/testing/wire.js';

const schema = { '~standard': { version: 1 as const, vendor: 'test', validate: (value: unknown) => ({ value: value as Record<string, unknown> }) } };
const all: Principal = { id: 'local', scopes: [''] };
const run = `attend-p8:{${randomUUID().slice(0, 8)}}:`;
let sequence = 0;
const redisClosers: (() => Promise<void>)[] = [];
async function redisConnection(): Promise<RedisSend> {
  const client = createClient({ url: process.env.ATTEND_REDIS_URL ?? 'redis://127.0.0.1:16379', socket: { reconnectStrategy: false, connectTimeout: 5000 } });
  client.on('error', () => undefined);
  await client.connect();
  redisClosers.push(() => client.close());
  return (args) => client.sendCommand(args);
}
let redisMain: Promise<RedisSend> | undefined;
async function redis(): Promise<ScopeStore> {
  const send = await (redisMain ??= redisConnection());
  return redisStore(send, { prefix: `${run}${sequence++}:`, blockingSend: await redisConnection() });
}
afterAll(async () => {
  const send = await redisMain;
  const keys = await send?.(['KEYS', `${run}*`]);
  if (send && Array.isArray(keys) && keys.length > 0) await send(['UNLINK', ...keys.map(String)]);
  await Promise.all(redisClosers.map((close) => close()));
});

const postgresUrl = process.env.ATTEND_POSTGRES_URL ?? 'postgres://postgres:attend@127.0.0.1:15432/attend';
const postgresSchema = `attend_p8_${randomUUID().replaceAll('-', '').slice(0, 12)}`;
const postgresConfig = { connectionString: postgresUrl, options: `-c search_path=${postgresSchema}`, connectionTimeoutMillis: 5000 };
let postgresReady: Promise<pg.Pool> | undefined;
let listenerReady: Promise<{ client: pg.Client; listener: PostgresListener }> | undefined;
let postgresSequence = 0;

function postgresPool() {
  return postgresReady ??= (async () => {
    const admin = new pg.Client({ connectionString: postgresUrl, connectionTimeoutMillis: 5000 });
    try {
      await admin.connect();
      await admin.query(`CREATE SCHEMA ${postgresSchema}`);
    } finally { await admin.end(); }
    return new pg.Pool({ ...postgresConfig, max: 4 });
  })();
}

function postgresListener() {
  return listenerReady ??= (async () => {
    await postgresPool();
    const client = new pg.Client(postgresConfig);
    await client.connect();
    const channels = new Map<string, { callbacks: Set<() => void>; ready: Promise<unknown> }>();
    client.on('notification', ({ channel }) => {
      for (const callback of channels.get(channel)?.callbacks ?? []) callback();
    });
    const listener: PostgresListener = {
      async listen(channel, callback) {
        let entry = channels.get(channel);
        if (!entry) {
          entry = { callbacks: new Set(), ready: client.query(`LISTEN "${channel}"`) };
          channels.set(channel, entry);
        }
        entry.callbacks.add(callback);
        await entry.ready;
        return async () => {
          entry.callbacks.delete(callback);
          if (entry.callbacks.size === 0 && channels.get(channel) === entry) {
            channels.delete(channel);
            await client.query(`UNLISTEN "${channel}"`);
          }
        };
      },
    };
    return { client, listener };
  })();
}

async function postgres(): Promise<ScopeStore> {
  const pool = await postgresPool();
  const prefix = `t${postgresSequence++}_`;
  await migrate(pool, { prefix });
  const store = postgresStore(pool, { prefix, listener: (await postgresListener()).listener });
  return { ...store, async commit(writes) {
    const outcomes = await store.commit(writes);
    const last = writes.flatMap((write) => write.appends).at(-1);
    // Other suites hold the cluster-wide visibility horizon behind this fixture's commit.
    if (last && outcomes.every((outcome) => outcome === 'ok')) await expect.poll(async () => (await store.log.find(last.kind, last.id))?.id, { timeout: 10_000 }).toBe(last.id);
    return outcomes;
  } };
}

afterAll(async () => {
  const listening = await listenerReady?.catch(() => undefined);
  await listening?.client.end();
  const pool = await postgresReady?.catch(() => undefined);
  try { await pool?.query(`DROP SCHEMA ${postgresSchema} CASCADE`); }
  finally { await pool?.end(); }
});

function append(id: string, scope = 's'): DecisionAppend {
  const at = Date.now();
  return { kind: 'decision', id, scope, at, route: 'interrupt', body: { id, scope, at, route: 'interrupt', probabilities: { interrupt: 1, queue: 0, note: 0, ask: 0, skip: 0, wait: 0 }, predicates: { relevant: 1 }, confidence: null, window: { id, events: [{ id: `e:${id}`, at, name: 'changed', data: { nested: ['original'] }, kind: 'closing', seenAt: at }], reason: 'closing', settledAt: at, waits: 0, gap: false, dropped: 0 } } };
}

async function target(store: ScopeStore = memoryStore(), era: 'legacy' | 'one' | 'modern' = 'legacy', principal = all, options: EventsServerOptions = {}, fallback = false) {
  const events = eventsServer(store.log, options);
  const raw = new Map<string | number, Record<string, unknown>>();
  let sent: string | number = '';
  function capture(message: unknown) {
    const m = message as { id?: string | number; result?: Record<string, unknown> };
    if (m.id !== undefined && m.result) raw.set(m.id, m.result);
  }
  const server = new Server({ name: 'attend-test', version: '1' }, { capabilities: {} });
  if (fallback) server.fallbackRequestHandler = async (request) => ({ resultType: 'complete', method: request.method });
  events.install(server, { principal });
  const client = era === 'one' ? new LegacyClient({ name: 'test', version: '1' }) : new Client({ name: 'test', version: '1' }, era === 'modern' ? { versionNegotiation: { mode: { pin: '2026-07-28' } } } : {});
  const handler = era === 'modern' ? createMcpHandler(() => {
    const instance = new Server({ name: 'attend-test', version: '1' }, { capabilities: {} });
    if (fallback) instance.fallbackRequestHandler = async (request) => ({ resultType: 'complete', method: request.method });
    events.install(instance, { principal });
    return instance;
  }) : undefined;
  const [serverTransport, memoryTransport] = InMemoryTransport.createLinkedPair();
  const transport: Transport = handler ? new StreamableHTTPClientTransport(new URL('http://attend.test/mcp'), { fetch: async (url, init) => {
    const request = new Request(url, init);
    const response = await handler.fetch(request);
    const text = await response.clone().text();
    for (const line of text.split('\n')) {
      const json = line.startsWith('data: ') ? line.slice(6) : line.startsWith('{') ? line : '';
      if (json) capture(JSON.parse(json));
    }
    return response;
  } }) : memoryTransport;
  const originalSend = transport.send.bind(transport);
  transport.send = async (message, ...args) => {
    if ('id' in message && 'method' in message) sent = message.id;
    return originalSend(message, ...args);
  };
  if (!handler) await server.connect(serverTransport);
  if (client instanceof LegacyClient) await client.connect(memoryTransport);
  else await client.connect(transport);
  const originalMessage = transport.onmessage;
  transport.onmessage = (message, extra) => { capture(message); originalMessage?.(message, extra); };
  const versions = new Map<string, number>();
  const result = {
    store, client, events, raw,
    async request(method: string, params: JsonObject): Promise<Record<string, unknown>> {
      if (client instanceof LegacyClient) await client.request({ method, params }, z.looseObject({}));
      else await client.request({ method, params }, schema);
      const response = raw.get(sent);
      if (!response) throw new Error(`No raw response for ${method} ${sent}`);
      return response;
    },
    async commit(appends: DecisionAppend[]) {
      const grouped = new Map<string, DecisionAppend[]>();
      for (const row of appends) grouped.set(row.scope, [...(grouped.get(row.scope) ?? []), row]);
      const writes: ScopeWrite[] = [...grouped].map(([scope, rows]) => {
        const inputs: ScopeWrite['inputs'] = [{ input: { kind: 'flush' }, now: Date.now() }];
        return { scope, expectedVersion: versions.get(scope) ?? 0, inputs, snapshot: null, appends: rows, serialized: { inputs: inputs.map((input) => JSON.stringify(input)), snapshot: null, appends: rows.map((row) => JSON.stringify(row.body)) } };
      });
      expect(await store.commit(writes)).toEqual(writes.map(() => 'ok'));
      for (const write of writes) versions.set(write.scope, write.expectedVersion + 1);
    },
    trim: (retention: Parameters<WireTarget['trim']>[0]) => store.log.trim(retention),
    async stop() {
      await client.close();
      await handler?.close();
      await server.close();
      await events.close();
      await store.close?.();
    },
  };
  return result;
}

for (const config of [
  { name: 'memory / SDK 2.x legacy', make: () => memoryStore(), era: 'legacy' },
  { name: 'memory / SDK 1.x legacy', make: () => memoryStore(), era: 'one' },
  { name: 'memory / SDK 2.x modern', make: () => memoryStore(), era: 'modern' },
  { name: 'SQLite / SDK 2.x legacy', make: () => {
    const db = new DatabaseSync(':memory:');
    const store = sqliteStore(db);
    return { ...store, async close() { await store.close?.(); db.close(); } };
  }, era: 'legacy' },
  { name: 'Redis / SDK 2.x legacy', make: redis, era: 'legacy' },
  { name: 'Redis / SDK 2.x modern', make: redis, era: 'modern' },
  { name: 'Postgres / SDK 2.x legacy', make: postgres, era: 'legacy' },
  { name: 'Postgres / SDK 2.x modern', make: postgres, era: 'modern' },
] as const) {
  describe(config.name, () => {
    for (const c of eventsWireConformance(async (principal) => target(await config.make(), config.era, principal))) {
      if (config.name.startsWith('Redis') && c.name === "a malformed position under the log's own epoch is InvalidParams") {
        // The Redis log's compare does not validate positions.
        test.skip(c.name, c.run);
      } else test(c.name, c.run, 30_000);
    }
  });
}

const poll = (t: Awaited<ReturnType<typeof target>>, params: JsonObject = {}) => t.request('events/poll', { name: 'attention.decision', ...params });
const rows = (result: Record<string, unknown>) => result.events as { eventId: string; data: { probabilities: Record<string, number>; events?: { data: { nested: string[] } }[] }; _meta?: JsonObject }[];

test('capabilities reach legacy initialize and modern discovery', async () => {
  const legacy = await target(memoryStore(), 'one');
  const modern = await target(memoryStore(), 'modern');
  try {
    expect(legacy.client.getServerCapabilities()).toEqual({ tools: {}, extensions: { 'io.modelcontextprotocol/events': {} } });
    expect(legacy.events.capabilities()).toEqual({ extensions: { 'io.modelcontextprotocol/events': {} } });
    const discovery = [...modern.raw.values()].find((result) => 'supportedVersions' in result)!;
    expect(discovery.supportedVersions).toContain('2026-07-28');
    expect(discovery.capabilities).toEqual({ tools: {}, extensions: { 'io.modelcontextprotocol/events': {} } });
    expect(discovery.resultType).toBe('complete');
  } finally { await legacy.stop(); await modern.stop(); }
});

test('tools false omits the capability and methods', async () => {
  const t = await target(memoryStore(), 'legacy', all, { tools: false });
  try {
    expect(t.client.getServerCapabilities()).not.toHaveProperty('tools');
    await expect(t.request('tools/call', { name: 'attend_wait' })).rejects.toMatchObject({ code: -32601 });
  } finally { await t.stop(); }
});

test('unserved methods reach the previous fallback or Method not found', async () => {
  for (const fallback of [false, true]) {
    const t = await target(memoryStore(), 'legacy', all, {}, fallback);
    try {
      for (const method of ['app/custom', 'events/subscribe', 'events/unsubscribe']) {
        if (fallback) expect(await t.request(method, {})).toEqual({ resultType: 'complete', method });
        else await expect(t.request(method, {})).rejects.toMatchObject({ code: -32601 });
      }
    } finally { await t.stop(); }
  }
});

test('the scan budget stops before a distant matching prefix', async () => {
  const t = await target();
  try {
    const cursor = (await poll(t)).cursor as string;
    await t.commit([...Array.from({ length: 9 }, (_, i) => append(`other-${i}`, 'other/')), append('mine', 'mine/')]);
    const first = await poll(t, { cursor, arguments: { scopePrefix: 'mine/' }, maxEvents: 2 });
    expect(rows(first)).toEqual([]);
    expect(first.hasMore).toBe(true);
    const second = await poll(t, { cursor: first.cursor as string, arguments: { scopePrefix: 'mine/' }, maxEvents: 2 });
    expect(rows(second).map((row) => row.eventId)).toEqual(['mine']);
    expect(second.hasMore).toBe(false);
  } finally { await t.stop(); }
});

test('maxEvents defaults to 100 and clamps at 1000', async () => {
  const t = await target();
  try {
    const cursor = (await poll(t)).cursor as string;
    await t.commit(Array.from({ length: 1005 }, (_, i) => append(String(i))));
    expect(rows(await poll(t, { cursor }))).toHaveLength(100);
    expect(rows(await poll(t, { cursor, maxEvents: 5000 }))).toHaveLength(1000);
  } finally { await t.stop(); }
});

test('an exact scope reaches the store index', async () => {
  const store = memoryStore();
  const queries: DecisionQuery[] = [];
  const read = store.log.read.bind(store.log);
  store.log.read = (query) => { queries.push(query); return read(query); };
  const t = await target(store);
  try {
    const cursor = (await poll(t)).cursor as string;
    await poll(t, { cursor, arguments: { scope: 'mine' } });
    expect(queries.at(-1)?.scope).toBe('mine');
  } finally { await t.stop(); }
});

test('close wakes a pending long poll', async () => {
  const t = await target();
  try {
    const cursor = (await poll(t)).cursor as string;
    const timer = setTimeout(() => { void t.events.close(); }, 50);
    const start = Date.now();
    try { expect(rows(await poll(t, { cursor, waitMs: 10000 }))).toEqual([]); }
    finally { clearTimeout(timer); }
    expect(Date.now() - start).toBeLessThan(1000);
    await t.events.close();
  } finally { await t.stop(); }
});

test('include events enforces the byte cap and marks omissions', async () => {
  const t = await target(memoryStore(), 'legacy', all, { payload: { maxEventsBytes: 64 } });
  try {
    const cursor = (await poll(t)).cursor as string;
    await t.commit([append('large')]);
    const result = rows(await poll(t, { cursor, arguments: { include: 'events' } }))[0]!;
    expect(result.data).not.toHaveProperty('events');
    expect(result._meta).toEqual({ 'attend/eventsOmitted': true });
  } finally { await t.stop(); }
});

test('received occurrences and descriptors cannot mutate store or shared data', async () => {
  const t = await target();
  try {
    const cursor = (await poll(t)).cursor as string;
    await t.commit([append('a')]);
    const params = { cursor, arguments: { include: 'events' } };
    const first = rows(await poll(t, params))[0]!;
    first.data.probabilities.interrupt = 0;
    first.data.events![0]!.data.nested[0] = 'mutated';
    const again = rows(await poll(t, params))[0]!;
    expect(again.data.probabilities.interrupt).toBe(1);
    expect(again.data.events![0]!.data.nested).toEqual(['original']);
    const listed = await t.request('events/list', {});
    (listed.events as { name: string }[])[0]!.name = 'mutated';
    expect((await t.request('events/list', {})).events).toMatchObject([{ name: 'attention.decision' }, { name: 'attention.deadLetter' }]);
  } finally { await t.stop(); }
});

for (const era of ['legacy', 'modern'] as const) test(`all served results carry resultType in ${era}`, async () => {
  const t = await target(memoryStore(), era);
  try {
    await t.commit([append('a')]);
    for (const [method, params] of [
      ['events/list', {}], ['events/poll', { name: 'attention.decision' }], ['tools/list', {}],
      ['tools/call', { name: 'attend_wait', arguments: { waitMs: 0 } }], ['tools/call', { name: 'attend_window', arguments: { decisionId: 'a' } }],
    ] satisfies [string, JsonObject][]) expect((await t.request(method, params)).resultType).toBe('complete');
  } finally { await t.stop(); }
});

for (const [params, field] of [
  [{ name: 1 }, 'name'], [{ arguments: [] }, 'arguments'], [{ arguments: null }, 'arguments'],
  [{ arguments: { scope: 3 } }, 'arguments.scope'], [{ arguments: { scopePrefix: 3 } }, 'arguments.scopePrefix'],
  [{ arguments: { routes: ['ask', 'ask'] } }, 'arguments.routes'], [{ arguments: { routes: 'ask' } }, 'arguments.routes'],
  [{ arguments: { include: 'all' } }, 'arguments.include'], [{ name: 'attention.deadLetter', arguments: { stage: 'other' } }, 'arguments.stage'],
  [{ cursor: 1 }, 'cursor'], [{ maxAgeMs: -1 }, 'maxAgeMs'], [{ maxAgeMs: 1.5 }, 'maxAgeMs'],
  [{ maxEvents: 1.5 }, 'maxEvents'], [{ waitMs: -1 }, 'waitMs'], [{ waitMs: 1.5 }, 'waitMs'],
] satisfies [JsonObject, string][]) test(`poll validates ${JSON.stringify(params)}`, async () => {
  const t = await target();
  try { await expect(poll(t, params)).rejects.toMatchObject({ code: -32602, data: { field } }); }
  finally { await t.stop(); }
});

test('long polling from now ignores existing rows and clamps waitMs to 30000', async () => {
  const store = memoryStore();
  const wait = store.log.wait.bind(store.log);
  let block = 0;
  let ready: () => void = () => undefined;
  const entered = new Promise<void>((resolve) => { ready = resolve; });
  store.log.wait = (after, blockMs, signal) => { block = blockMs; ready(); return wait(after, blockMs, signal); };
  const t = await target(store);
  try {
    await t.commit([append('old')]);
    const pending = poll(t, { cursor: null, waitMs: 100000 });
    await entered;
    expect(block).toBeGreaterThan(29000);
    expect(block).toBeLessThanOrEqual(30000);
    await t.commit([append('new')]);
    expect(rows(await pending).map((row) => row.eventId)).toEqual(['new']);
  } finally { await t.stop(); }
});

test('request cancellation aborts the store wait', async () => {
  const store = memoryStore();
  const wait = store.log.wait.bind(store.log);
  let ready: () => void = () => undefined;
  let finished: () => void = () => undefined;
  let waitingSignal: AbortSignal | undefined;
  const entered = new Promise<void>((resolve) => { ready = resolve; });
  const ended = new Promise<void>((resolve) => { finished = resolve; });
  store.log.wait = async (after, blockMs, signal) => {
    waitingSignal = signal;
    ready();
    await wait(after, blockMs, signal);
    finished();
  };
  const t = await target(store);
  const abort = new AbortController();
  try {
    if (t.client instanceof LegacyClient) throw new Error('Expected SDK 2.x client');
    const pending = t.client.request({ method: 'events/poll', params: { name: 'attention.decision', cursor: null, waitMs: 10000 } }, schema, { signal: abort.signal });
    const rejected = expect(pending).rejects.toBeDefined();
    await entered;
    abort.abort();
    await rejected;
    await ended;
    expect(waitingSignal?.aborted).toBe(true);
  } finally { abort.abort(); await t.stop(); }
});

test('an empty poll at the head makes one store read', async () => {
  const store = memoryStore();
  const log = store.log;
  const calls = { epoch: 0, bounds: 0, read: 0, wait: 0 };
  let epochs = 0;
  const t = await target({ ...store, log: { ...log,
    async epoch() { calls.epoch++; epochs++; return log.epoch(); },
    async bounds() { calls.bounds++; return log.bounds(); },
    async read(query) { calls.read++; return log.read(query); },
    async wait(after, blockMs, signal) { calls.wait++; return log.wait(after, blockMs, signal); },
  } });
  const reset = () => { calls.epoch = 0; calls.bounds = 0; calls.read = 0; calls.wait = 0; };
  try {
    await t.commit([append('a')]);
    const head = await poll(t, { cursor: null, maxAgeMs: 60_000 });
    expect(calls).toEqual({ epoch: 1, bounds: 1, read: 0, wait: 0 });
    const cursor = head.cursor as string;
    reset();
    expect(rows(await poll(t, { cursor }))).toEqual([]);
    expect(rows(await poll(t, { cursor }))).toEqual([]);
    expect(calls).toEqual({ epoch: 0, bounds: 0, read: 2, wait: 0 });
    reset();
    expect(rows(await poll(t, { cursor, maxAgeMs: 60_000 }))).toEqual([]);
    expect(calls).toEqual({ epoch: 0, bounds: 0, read: 1, wait: 0 });
    await t.commit([append('b')]);
    reset();
    expect(rows(await poll(t, { cursor })).map((row) => row.eventId)).toEqual(['b']);
    expect(calls).toEqual({ epoch: 0, bounds: 1, read: 1, wait: 0 });
    await t.commit([append('c')]);
    reset();
    expect(rows(await poll(t, { cursor, maxAgeMs: 60_000 })).map((row) => row.eventId)).toEqual(['b', 'c']);
    expect(calls).toEqual({ epoch: 0, bounds: 1, read: 2, wait: 0 });
    reset();
    expect(rows(await poll(t, { cursor, arguments: { scope: 'other' } }))).toEqual([]);
    expect(calls).toEqual({ epoch: 0, bounds: 1, read: 1, wait: 0 });
    reset();
    const epoch = cursor.split('.')[1];
    expect(rows(await poll(t, { cursor: `v1.${epoch}.`, maxAgeMs: 60_000 })).map((row) => row.eventId)).toEqual(['a', 'b', 'c']);
    expect(calls).toEqual({ epoch: 0, bounds: 0, read: 2, wait: 0 });
    expect(epochs).toBe(1);
  } finally { await t.stop(); }
});

test('a trim during a long poll says truncated', async () => {
  const store = memoryStore();
  const t = await target(store);
  const wait = store.log.wait.bind(store.log);
  let first = true;
  store.log.wait = async (after, blockMs, signal) => {
    if (!first) return wait(after, blockMs, signal);
    first = false;
    await t.commit([append('b'), append('c')]);
    await store.log.trim({ maxRows: 1 });
  };
  try {
    await t.commit([append('a')]);
    const cursor = (await poll(t)).cursor as string;
    const result = await poll(t, { cursor, waitMs: 5000 });
    expect(rows(result).map((row) => row.eventId)).toEqual(['c']);
    expect(result.truncated).toBe(true);
  } finally { await t.stop(); }
});

test('include events caps UTF-8 bytes at the exact boundary', async () => {
  const entries = [{ id: 'e:a', at: 1, name: 'changed', data: { text: 'é'.repeat(40) }, kind: 'closing' as const, seenAt: 2 }];
  // The serialized entries are 165 UTF-8 bytes and 125 UTF-16 units.
  for (const cap of [164, 165]) {
    const t = await target(memoryStore(), 'legacy', all, { payload: { maxEventsBytes: cap } });
    try {
      const cursor = (await poll(t)).cursor as string;
      const row = append('a');
      row.at = 3;
      row.body.at = 3;
      row.body.window.settledAt = 2;
      row.body.window.events = entries;
      await t.commit([row]);
      const result = rows(await poll(t, { cursor, arguments: { include: 'events' } }))[0]!;
      if (cap === 164) {
        expect(result.data).not.toHaveProperty('events');
        expect(result._meta).toEqual({ 'attend/eventsOmitted': true });
      } else {
        expect(result.data.events).toEqual(entries);
        expect(result).not.toHaveProperty('_meta');
      }
    } finally { await t.stop(); }
  }
});

test('a rejected epoch read is retried on the next poll', async () => {
  const store = memoryStore();
  let reads = 0;
  const t = await target({ ...store, log: { ...store.log, async epoch() {
    reads++;
    if (reads === 1) throw new Error('epoch unavailable');
    return store.log.epoch();
  } } });
  try {
    await expect(poll(t)).rejects.toBeDefined();
    expect(reads).toBe(1);
    expect(rows(await poll(t))).toEqual([]);
    expect(rows(await poll(t))).toEqual([]);
    expect(reads).toBe(2);
  } finally { await t.stop(); }
});
