import { randomUUID } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { afterAll, describe, expect, test } from 'vitest';
import pg from 'pg';
import { Server, InMemoryTransport } from '@modelcontextprotocol/server';
import { Client } from '@modelcontextprotocol/client';
import { memoryStore } from '../../src/store/memory.js';
import { sqliteStore } from '../../src/store/sqlite.js';
import { migrate, postgresStore } from '../../src/store/postgres.js';
import type { ScopeStore, ScopeWrite } from '../../src/store/types.js';
import type { DecisionAppend } from '../../src/core/types.js';
import { createFanout, eventNotification, notificationJSON } from '../../src/server/fanout.js';
import { decisionDescriptor, compileFilter } from '../../src/server/events.js';
import { streamEvents, streamUsage } from '../../src/server/stream.js';
import { eventsServer } from '../../src/server/install.js';
import { SUBSCRIPTION_ID_META, type ServerContext, type StreamNotification, type StreamParams, type Principal, type EventsServerOptions } from '../../src/server/types.js';

const all: Principal = { id: 'local', scopes: [''] };
const name = 'attention.decision';
const schema = { '~standard': { version: 1 as const, vendor: 'test', validate: (value: unknown) => ({ value }) } };
function gate() {
  let resolve: () => void = () => undefined;
  const promise = new Promise<void>((done) => { resolve = done; });
  return { promise, resolve };
}
function append(id: string, scope = 's', at = Date.now()): DecisionAppend {
  return { kind: 'decision', id, scope, at, route: 'interrupt', body: { id, scope, at, route: 'interrupt', probabilities: { interrupt: 1, queue: 0, note: 0, ask: 0, skip: 0, wait: 0 }, predicates: {}, confidence: null, window: { id, events: [{ id: `e:${id}`, at, name: 'changed', data: { text: 'x'.repeat(64) }, kind: 'closing', seenAt: at }], reason: 'closing', settledAt: at, waits: 0, gap: false, dropped: 0 } } };
}
function fixture(store: ScopeStore = memoryStore(), options: EventsServerOptions = {}) {
  const close = new AbortController();
  const fanout = createFanout(store.log);
  const context: ServerContext = { log: store.log, epoch: () => store.log.epoch(), options, descriptors: [decisionDescriptor()], signal: close.signal, fanout };
  const versions = new Map<string, number>();
  const pending: Promise<unknown>[] = [];
  return {
    context, store,
    async commit(rows: DecisionAppend[]) {
      const groups = new Map<string, DecisionAppend[]>();
      for (const row of rows) { const group = groups.get(row.scope) ?? []; group.push(row); groups.set(row.scope, group); }
      const writes: ScopeWrite[] = [...groups].map(([scope, appends]) => ({ scope, expectedVersion: versions.get(scope) ?? 0, inputs: [{ input: { kind: 'flush' }, now: Date.now() }], snapshot: null, appends }));
      expect(await store.commit(writes)).toEqual(writes.map(() => 'ok'));
      for (const write of writes) versions.set(write.scope, write.expectedVersion + 1);
    },
    open(params: Partial<StreamParams> = {}, id: string | number = 7, principal = all, send?: (n: StreamNotification) => Promise<void>) {
      const abort = new AbortController();
      const frames: StreamNotification[] = [];
      const result = streamEvents(context, { name, ...params }, principal, id, async (frame) => { frames.push(frame); await send?.(frame); }, abort.signal);
      pending.push(result.catch(() => undefined));
      return { frames, result, abort, ids: () => frames.flatMap((f) => f.method === 'notifications/events/event' ? [f.params.eventId] : []) };
    },
    async stop() { close.abort(); fanout.close(); await Promise.all(pending); await store.close?.(); },
  };
}
const eventually = async (get: () => unknown, expected: unknown) => expect.poll(get, { timeout: 10_000, interval: 5 }).toEqual(expected);

for (const id of [42, 'forty-two']) test(`active first and every notification preserves subscription id ${id}`, async () => {
  const t = fixture(undefined, { heartbeatMs: 50 });
  try {
    await t.commit([append('old')]);
    const stream = t.open({}, id);
    const epoch = await t.context.epoch();
    await eventually(() => stream.frames.length > 1, true);
    expect(stream.frames[0]).toEqual({ method: 'notifications/events/active', params: { cursor: `v1.${epoch}.1`, truncated: false, _meta: { [SUBSCRIPTION_ID_META]: id } } });
    expect(stream.ids()).toEqual([]);
    await t.commit([append('live')]);
    await eventually(stream.ids, ['live']);
    for (const frame of stream.frames) expect(frame.params._meta[SUBSCRIPTION_ID_META]).toBe(id);
  } finally { await t.stop(); }
});

test('subscribe before catch-up closes the commit race and preserves log order', async () => {
  const t = fixture();
  try {
    await t.commit([append('a'), append('b')]);
    const read = t.context.log.read.bind(t.context.log);
    let raced = false;
    t.context.log.read = async (query) => {
      const page = await read(query);
      if (query.kinds && !raced) { raced = true; await t.commit([append('between')]); }
      return page;
    };
    const stream = t.open({ cursor: `v1.${await t.context.epoch()}.` });
    await eventually(stream.ids, ['a', 'b', 'between']);
    await t.commit([append('live-1'), append('live-2')]);
    await eventually(stream.ids, ['a', 'b', 'between', 'live-1', 'live-2']);
    expect(raced).toBe(true);
  } finally { await t.stop(); }
});

const pgSchema = `attend_p9_${randomUUID().replaceAll('-', '')}`;
let pool: pg.Pool | undefined;
async function postgres() {
  const url = process.env.ATTEND_POSTGRES_URL ?? 'postgres://postgres:attend@127.0.0.1:15432/attend';
  const admin = new pg.Client({ connectionString: url });
  await admin.connect();
  try { await admin.query(`CREATE SCHEMA ${pgSchema}`); } finally { await admin.end(); }
  pool = new pg.Pool({ connectionString: url, options: `-c search_path=${pgSchema}`, max: 2 });
  await migrate(pool);
  return postgresStore(pool);
}
afterAll(async () => { if (pool) { try { await pool.query(`DROP SCHEMA ${pgSchema} CASCADE`); } finally { await pool.end(); } } });

for (const [label, make] of [
  ['memory', async () => memoryStore()],
  ['SQLite', async () => { const db = new DatabaseSync(':memory:'); const store = sqliteStore(db); return { ...store, async close() { await store.close?.(); db.close(); } }; }],
  ['Postgres', postgres],
] satisfies [string, () => Promise<ScopeStore>][]) describe(label, () => {
  test('catch-up crosses empty more:true pages, then delivers live rows and a head heartbeat', async () => {
    const t = fixture(await make(), { heartbeatMs: 50 });
    try {
      const epoch = await t.context.epoch();
      const rows = [...Array.from({ length: 1030 }, (_, i) => append(`other-${i}`, 'other/')), ...Array.from({ length: 260 }, (_, i) => append(`match-${i}`, 'mine/'))];
      await t.commit(rows);
      await eventually(async () => (await t.context.log.bounds()).last !== null, true);
      const queries: { more: boolean; rows: number }[] = [];
      const read = t.context.log.read.bind(t.context.log);
      t.context.log.read = async (query) => { const page = await read(query); if (query.kinds) queries.push({ more: page.more, rows: page.rows.length }); return page; };
      const stream = t.open({ cursor: `v1.${epoch}.`, arguments: { scopePrefix: 'mine/' } });
      await eventually(stream.ids, Array.from({ length: 260 }, (_, i) => `match-${i}`));
      expect(queries).toContainEqual({ more: true, rows: 0 });
      await t.commit([append('live', 'mine/')]);
      await eventually(() => stream.ids().at(-1), 'live');
      const head = (await t.context.log.bounds()).last;
      await eventually(() => stream.frames.filter((f) => f.method === 'notifications/events/heartbeat').at(-1)?.params.cursor, `v1.${epoch}.${head}`);
      expect(stream.ids()).toEqual([...Array.from({ length: 260 }, (_, i) => `match-${i}`), 'live']);
    } finally { await t.stop(); }
  }, 30_000);
});

test('quiet filtered heartbeats advance over nonmatches without redundant heartbeats during traffic', async () => {
  const t = fixture(undefined, { heartbeatMs: 50 });
  try {
    const stream = t.open({ arguments: { scope: 'mine' } });
    await eventually(() => stream.frames.some((f) => f.method === 'notifications/events/heartbeat'), true);
    await t.commit([append('other', 'other')]);
    const head = (await t.context.log.bounds()).last;
    await eventually(() => { const frame = stream.frames.at(-1); return frame && 'cursor' in frame.params ? frame.params.cursor : undefined; }, `v1.${await t.context.epoch()}.${head}`);
    const before = stream.frames.filter((f) => f.method === 'notifications/events/heartbeat').length;
    await t.commit(Array.from({ length: 50 }, (_, i) => append(`flow-${i}`, 'mine')));
    await eventually(() => stream.ids().length, 50);
    expect(stream.frames.filter((f) => f.method === 'notifications/events/heartbeat').length - before).toBeLessThanOrEqual(1);
    expect(stream.frames.every((f) => 'cursor' in f.params && /^v1\.[^.]+\./.test(f.params.cursor ?? ''))).toBe(true);
  } finally { await t.stop(); }
});

test('slow sends bound the outbox and replay every row without truncation', async () => {
  const budget = 1500;
  const t = fixture(undefined, { streamBufferBytes: budget });
  const blocked = gate();
  try {
    const stream = t.open({}, 7, all, async (frame) => { if (frame.method === 'notifications/events/event' && frame.params.eventId === 'first') await blocked.promise; });
    await eventually(() => stream.frames.length, 1);
    await t.commit([append('first')]);
    await eventually(stream.ids, ['first']);
    await t.commit(Array.from({ length: 1500 }, (_, i) => append(`row-${i}`)));
    await eventually(() => t.context.fanout.checked(), '1501');
    expect(streamUsage(t.context).queuedBytes).toBeLessThanOrEqual(budget);
    expect(stream.ids()).toEqual(['first']);
    blocked.resolve();
    await eventually(stream.ids, ['first', ...Array.from({ length: 1500 }, (_, i) => `row-${i}`)]);
    expect(stream.frames.filter((f) => f.method === 'notifications/events/active').map((f) => f.params.truncated)).toEqual([false]);
  } finally { blocked.resolve(); await t.stop(); }
});

test('abort during a pending write immediately frees quota and the fanout subscriber', async () => {
  const t = fixture(undefined, { maxStreamsPerPrincipal: 1 });
  const blocked = gate();
  try {
    const first = t.open({}, 1, all, () => blocked.promise);
    await eventually(() => first.frames.length, 1);
    const rejection = expect(first.result).rejects.toMatchObject({ name: 'AbortError' });
    first.abort.abort();
    await rejection;
    expect(streamUsage(t.context)).toEqual({ count: 0, queuedBytes: 0, writeBytes: 0, pageRows: 0 });
    expect(t.context.fanout.size).toBe(0);
    const next = t.open({}, 2);
    await eventually(() => next.frames.length, 1);
    await t.commit([append('new')]);
    await eventually(next.ids, ['new']);
    expect(first.frames).toHaveLength(1);
  } finally { blocked.resolve(); await t.stop(); }
});

test('quota rejects before frames and is isolated by principal id', async () => {
  const t = fixture(undefined, { maxStreamsPerPrincipal: 1 });
  try {
    const first = t.open(); await eventually(() => first.frames.length, 1);
    const over = t.open();
    await expect(over.result).rejects.toMatchObject({ code: -32025, data: { limit: 'streams', max: 1 } });
    expect(over.frames).toEqual([]);
    const other = t.open({}, 3, { id: 'other', scopes: [''] });
    await eventually(() => other.frames.length, 1);
    expect(t.context.fanout.size).toBe(2);
  } finally { await t.stop(); }
});

for (const [params, principal, code, data] of [
  [{ name: 'missing' }, all, -32023, { kind: 'event' }],
  [{ arguments: { scope: 'secret' } }, { id: 'limited', scopes: ['allowed/'] }, -32024, { reason: 'scope is outside granted prefixes' }],
  [{ arguments: { include: 'bad' } }, all, -32602, { field: 'arguments.include', reason: 'must be notice or events' }],
] satisfies [Partial<StreamParams>, Principal, number, object][]) test(`invalid subscriptions fail before active: ${code}`, async () => {
  const t = fixture();
  try { const stream = t.open(params, 7, principal); await expect(stream.result).rejects.toMatchObject({ code, data }); expect(stream.frames).toEqual([]); expect(t.context.fanout.size).toBe(0); }
  finally { await t.stop(); }
});

test('graceful close returns exactly the complete result while client abort rejects', async () => {
  const t = fixture();
  const stream = t.open({}, 'request');
  await eventually(() => stream.frames.length, 1);
  await t.stop();
  expect(await stream.result).toEqual({ resultType: 'complete', _meta: { [SUBSCRIPTION_ID_META]: 'request' } });
  expect(stream.frames.map((f) => f.method)).toEqual(['notifications/events/active']);
});

test('retention passing a blocked reader repeats active with truncation and continues at the floor', async () => {
  const t = fixture(memoryStore({ decisions: { maxRows: 3 } }), { streamBufferBytes: 100 });
  const blocked = gate();
  try {
    const stream = t.open({}, 7, all, async (frame) => { if (frame.method === 'notifications/events/event' && frame.params.eventId === 'first') await blocked.promise; });
    await eventually(() => stream.frames.length, 1);
    await t.commit([append('first')]); await eventually(stream.ids, ['first']);
    await t.commit(Array.from({ length: 10 }, (_, i) => append(`retained-${i}`)));
    await eventually(() => t.context.fanout.checked(), '11');
    blocked.resolve();
    await eventually(stream.ids, ['first', 'retained-7', 'retained-8', 'retained-9']);
    expect(stream.frames.filter((f) => f.method === 'notifications/events/active').map((f) => f.params.truncated)).toEqual([false, true]);
  } finally { blocked.resolve(); await t.stop(); }
});

test('unknown epochs and maxAge reset replay with truncated active', async () => {
  const t = fixture();
  try {
    await t.commit([append('old', 's', Date.now() - 10_000), append('new')]);
    const stream = t.open({ cursor: 'v1.unknown.1', maxAgeMs: 1000 });
    await eventually(stream.ids, ['new']);
    expect(stream.frames[0]).toMatchObject({ method: 'notifications/events/active', params: { truncated: true } });
    const aged = t.open({ cursor: `v1.${await t.context.epoch()}.`, maxAgeMs: 1000 });
    await eventually(aged.ids, ['new']);
    expect(aged.frames[0]).toMatchObject({ method: 'notifications/events/active', params: { truncated: true } });
  } finally { await t.stop(); }
});

test('a transient catch-up read failure emits an error then retries without closing', async () => {
  const t = fixture();
  try {
    await t.commit([append('recovered')]);
    const read = t.context.log.read.bind(t.context.log); let fail = true;
    t.context.log.read = (query) => { if (query.kinds && fail) { fail = false; return Promise.reject(new Error('temporary')); } return read(query); };
    const stream = t.open({ cursor: `v1.${await t.context.epoch()}.` });
    await eventually(stream.ids, ['recovered']);
    expect(stream.frames.slice(0, 2)).toMatchObject([{ method: 'notifications/events/active' }, { method: 'notifications/events/error', params: { error: { code: -32603, data: { reason: 'temporary' } } } }]);
  } finally { await t.stop(); }
});

test('fanout indexes scopes and grants, shares encoding, unsubscribes and stops reads', async () => {
  const t = fixture();
  const lists = { exact: [] as string[], prefix: [] as string[], all: [] as string[], grants: [] as string[] };
  const remove: (() => void)[] = [];
  let reads = 0;
  const read = t.context.log.read.bind(t.context.log);
  t.context.log.read = (query) => { reads++; return read(query); };
  try {
    for (const [key, args, principal] of [
      ['exact', { scope: 'a/1' }, all], ['prefix', { scopePrefix: 'a/' }, all], ['all', {}, all], ['grants', {}, { id: 'g', scopes: ['a/1', 'b/'] }],
    ] satisfies [keyof typeof lists, object, Principal][]) remove.push(t.context.fanout.subscribe(compileFilter(name, args, principal), (row) => lists[key].push(row.id)));
    await t.commit([append('a1', 'a/1'), append('a2', 'a/2'), append('b1', 'b/1'), append('c1', 'c/1')]);
    await eventually(() => lists.all, ['a1', 'a2', 'b1', 'c1']);
    expect(lists).toEqual({ exact: ['a1'], prefix: ['a1', 'a2'], all: ['a1', 'a2', 'b1', 'c1'], grants: ['a1', 'b1'] });
    const row = (await t.context.log.find('decision', 'a1'))!;
    for (const include of ['notice', 'events']) {
      const filter = compileFilter(name, { include }, all);
      const a = eventNotification(row, filter, 10, 'epoch', 1);
      const b = eventNotification(row, filter, 10, 'epoch', 2);
      expect(notificationJSON(a).replace('subscriptionId":1', 'subscriptionId":2')).toBe(notificationJSON(b));
      expect(a.params.eventId).toBe('a1');
      if (include === 'events') expect(a.params._meta).toEqual({ 'attend/eventsOmitted': true, [SUBSCRIPTION_ID_META]: 1 });
    }
    remove.forEach((off) => off());
    expect(t.context.fanout.size).toBe(0);
    t.context.fanout.close(); const before = reads;
    await t.commit([append('after-close')]);
    await new Promise<void>((resolve) => setImmediate(resolve));
    expect(reads).toBe(before);
    expect(lists.all).toEqual(['a1', 'a2', 'b1', 'c1']);
  } finally { await t.stop(); }
});

test('the shared reader starts at the head and never replays the log it already holds', async () => {
  const t = fixture();
  const reads: (string | null)[] = []; const read = t.context.log.read.bind(t.context.log);
  t.context.log.read = (query) => { reads.push(query.after); return read(query); };
  try {
    await t.commit(Array.from({ length: 600 }, (_, i) => append(`old-${i}`)));
    const stream = t.open({});
    await eventually(() => stream.frames.length, 1);
    await t.commit([append('fresh')]);
    await eventually(stream.ids, ['fresh']);
    expect(reads).toEqual(['600', '600']);
  } finally { await t.stop(); }
});

test('idle streams share a single reader and use the ring for replay', async () => {
  const t = fixture(undefined, { heartbeatMs: 50 });
  let reads = 0; const read = t.context.log.read.bind(t.context.log);
  t.context.log.read = (query) => { reads++; return read(query); };
  try {
    const streams = Array.from({ length: 10 }, (_, i) => t.open({}, i));
    await eventually(() => streams.every((stream) => stream.frames.length >= 2), true);
    expect(reads).toBe(1);
    await t.commit([append('a'), append('b')]);
    await eventually(() => streams.every((stream) => stream.ids().length === 2), true);
    const before = reads;
    const replay = t.open({ cursor: `v1.${await t.context.epoch()}.1` });
    await eventually(replay.ids, ['b']);
    expect(reads).toBe(before);
  } finally { await t.stop(); }
});

test('SDK InMemory transport streams notifications, completes on close, and suppresses cancelled results', async () => {
  const store = memoryStore();
  const events = eventsServer(store.log, { heartbeatMs: 50 });
  const server = new Server({ name: 'stream-test', version: '1' }, { capabilities: {} });
  events.install(server);
  const client = new Client({ name: 'test', version: '1' });
  const [serverTransport, clientTransport] = InMemoryTransport.createLinkedPair();
  const notifications: { method: string; params?: Record<string, unknown> | undefined }[] = [];
  const messages: unknown[] = [];
  client.fallbackNotificationHandler = async (notification) => { notifications.push(notification); };
  await server.connect(serverTransport);
  await client.connect(clientTransport);
  const onmessage = clientTransport.onmessage;
  clientTransport.onmessage = (message, extra) => { messages.push(message); onmessage?.(message, extra); };
  try {
    const abort = new AbortController();
    const cancelled = client.request({ method: 'events/stream', params: { name } }, schema, { signal: abort.signal });
    const rejection = expect(cancelled).rejects.toBeDefined();
    await eventually(() => notifications.length, 1);
    abort.abort(); await rejection;
    const framesBefore = messages.length;
    const live = client.request({ method: 'events/stream', params: { name } }, schema);
    await eventually(() => notifications.length, 2);
    await events.close();
    const result = await live;
    expect(result).toMatchObject({ _meta: { [SUBSCRIPTION_ID_META]: expect.any(Number) } });
    expect(messages.at(-1)).toMatchObject({ result: { resultType: 'complete', _meta: { [SUBSCRIPTION_ID_META]: expect.any(Number) } } });
    expect(messages.slice(framesBefore).filter((m) => typeof m === 'object' && m !== null && 'result' in m)).toHaveLength(1);
    expect(notifications.every((n) => n.method === 'notifications/events/active')).toBe(true);
  } finally { await events.close(); await client.close(); await server.close(); }
});

test('send rejection releases the subscriber and quota without further frames', async () => {
  const t = fixture(undefined, { maxStreamsPerPrincipal: 1 });
  try {
    const failed = t.open({}, 1, all, async () => { throw new Error('broken writer'); });
    await expect(failed.result).rejects.toThrow('broken writer');
    expect(t.context.fanout.size).toBe(0);
    expect(failed.frames.map((frame) => frame.method)).toEqual(['notifications/events/active']);
    const next = t.open({}, 2); await eventually(() => next.frames.length, 1);
    await t.commit([append('after-failure')]); await eventually(next.ids, ['after-failure']);
    expect(failed.frames).toHaveLength(1);
  } finally { await t.stop(); }
});

test('removing an event descriptor terminates the subscription', async () => {
  const t = fixture(undefined, { heartbeatMs: 10 });
  try {
    const stream = t.open({}, 'removed'); await eventually(() => stream.frames.length, 1);
    const ended = expect(stream.result).rejects.toMatchObject({ name: 'AbortError' });
    const descriptors = t.context.descriptors;
    if (!Array.isArray(descriptors)) throw new Error('Expected mutable fixture descriptors');
    descriptors.length = 0;
    await ended;
    expect(stream.frames.at(-1)).toEqual({ method: 'notifications/events/terminated', params: { error: { code: -32023, message: 'Event type removed', data: { kind: 'event' } }, _meta: { [SUBSCRIPTION_ID_META]: 'removed' } } });
    expect(t.context.fanout.size).toBe(0);
  } finally { await t.stop(); }
});

test('fanout bounds its ring, filters routes and probability, and isolates throwing subscribers', async () => {
  const t = fixture();
  try {
    const delivered: string[] = [];
    const strict = compileFilter(name, { routes: ['interrupt'], minProbability: 0.9 }, all);
    const off = t.context.fanout.subscribe(strict, (row) => delivered.push(row.id));
    t.context.fanout.subscribe(compileFilter(name, {}, all), () => { throw new Error('subscriber error'); });
    const low = append('low'); if (low.kind === 'decision') low.body.probabilities.interrupt = 0.5;
    const queue = append('queue'); if (queue.kind === 'decision') { queue.route = 'queue'; queue.body.route = 'queue'; }
    await t.commit([low, queue, ...Array.from({ length: 4100 }, (_, i) => append(`cached-${i}`))]);
    await eventually(() => delivered.length, 4100);
    expect(delivered.slice(0, 2)).toEqual(['cached-0', 'cached-1']);
    expect(delivered.at(-1)).toBe('cached-4099');
    expect(t.context.fanout.cached('1')).toBeNull();
    expect(t.context.fanout.cached('4101')?.rows.map((row) => row.id)).toEqual(['cached-4099']);
    expect(t.context.fanout.cached('4102')).toEqual({ rows: [], checked: '4102', more: false });
    off(); off(); expect(t.context.fanout.size).toBe(1);
  } finally { await t.stop(); }
});

test('heartbeats continue while a catch-up store read is pending', async () => {
  const t = fixture(undefined, { heartbeatMs: 10 });
  const blocked = gate();
  try {
    await t.commit([...Array.from({ length: 1030 }, (_, i) => append(`other-${i}`, 'other/')), append('mine', 'mine/')]);
    const read = t.context.log.read.bind(t.context.log);
    let calls = 0;
    t.context.fanout.cached = () => null;
    t.context.log.read = async (query) => {
      if (query.kinds && ++calls === 2) await blocked.promise;
      return read(query);
    };
    const epoch = await t.context.epoch();
    const stream = t.open({ cursor: `v1.${epoch}.`, arguments: { scopePrefix: 'mine/' } });
    await eventually(() => stream.frames.filter((frame) => frame.method === 'notifications/events/heartbeat').length >= 2, true);
    expect(stream.ids()).toEqual([]);
    expect(stream.frames.filter((frame) => frame.method === 'notifications/events/heartbeat').slice(0, 2).map((frame) => frame.params.cursor)).toEqual([`v1.${epoch}.1024`, `v1.${epoch}.1024`]);
    blocked.resolve(); await eventually(stream.ids, ['mine']);
  } finally { blocked.resolve(); await t.stop(); }
});
