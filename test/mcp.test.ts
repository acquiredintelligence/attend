import { describe, expect, test } from 'vitest';
import type { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { createAttend, PermanentError } from '../src/attend.js';
import { mcpEventsSource, type McpEventsError, type McpEventsOptions } from '../src/mcp.js';
import { memoryStore } from '../src/memory.js';
import { drain, fakeClock, recordingSink, scriptedJudge, type ScriptedAnswer } from '../src/testing.js';
import type { DeadLetter, Entry, Event, Kind, StateStore, Trace } from '../src/types.js';
import { mcpEventsTestServer, type EventSpec } from './support/mcp-events-server.js';

const SUBSCRIPTION_KEY = 'io.modelcontextprotocol/subscriptionId';
const STAMP = '2026-10-15T12:00:00.000Z';
const KEY = 'mcp:alpha:{}';
const closing = (): Kind => 'closing';
const occ = (eventId: string, name = 'alpha') => ({ eventId, name, timestamp: STAMP, data: { n: eventId.length } });
const entry = (id: string, name = 'alpha'): Entry => ({ id, at: Date.parse(STAMP), name, data: { n: id.length }, kind: 'closing', seenAt: 1000 });

/** Let every microtask chain between the client, the server and the store run out. */
async function settle(): Promise<void> {
  for (let i = 0; i < 4; i += 1) await drain();
}

function setup(script: ScriptedAnswer[] = [{ route: 'queue' }], store: StateStore = memoryStore()) {
  const clock = fakeClock(1000);
  const judge = scriptedJudge(script);
  const sink = recordingSink();
  const deadLetters: DeadLetter[] = [];
  const traces: Trace[] = [];
  const errors: McpEventsError[] = [];
  const attend = createAttend({
    store,
    judge,
    sink,
    kind: closing,
    deadLetter: async (letter) => {
      deadLetters.push(letter);
    },
    observe: (t) => traces.push(t),
    clock,
  });
  const controller = new AbortController();
  const done = attend.run({ signal: controller.signal, sweepMs: 1000 });
  const attached: Promise<void>[] = [];
  const source = (client: Client, options: Partial<McpEventsOptions> & { name?: string } = {}) =>
    mcpEventsSource(client, { name: 'alpha', mode: 'poll', clock, onError: (e) => errors.push(e), ...options });
  const attach = (scope: string, client: Client, options?: Partial<McpEventsOptions>) => {
    const p = attend.attach(scope, source(client, options));
    attached.push(p);
    return p;
  };
  const stop = async () => {
    controller.abort();
    await done;
    await Promise.all(attached);
  };
  const delivered = () => sink.delivered.map((d) => ({ scope: d.scope, ids: d.window.events.map((e) => e.id), gap: d.window.gap }));
  const streamRequests = (server: { requests: { method: string; params: unknown }[] }) => server.requests.filter((r) => r.method === 'events/stream').map((r) => r.params);
  return { clock, store, judge, sink, deadLetters, traces, errors, attend, source, attach, stop, delivered, streamRequests };
}

const ALPHA: EventSpec[] = [{ name: 'alpha', delivery: ['poll', 'push'] }];

/** A store whose puts wait while held, the way a slow database stalls the source's emit. */
function heldStore(): { store: StateStore; hold(): void; release(): void } {
  const inner = memoryStore();
  let gate: Promise<void> | null = null;
  let open: () => void = () => undefined;
  return {
    store: {
      get: (scope) => inner.get(scope),
      async put(record, expectedVersion) {
        if (gate) await gate;
        return inner.put(record, expectedVersion);
      },
      due: (now, limit) => inner.due(now, limit),
      remove: (scope) => inner.remove(scope),
    },
    hold() {
      gate = new Promise((resolve) => {
        open = () => resolve();
      });
    },
    release() {
      gate = null;
      open();
    },
  };
}

const deliveredIds = (h: ReturnType<typeof setup>) => h.delivered().flatMap((d) => d.ids);

/** A store whose every put takes `ms` of real time, the way a database round trip does. */
function slowStore(ms: number): StateStore {
  const inner = memoryStore();
  return {
    get: (scope) => inner.get(scope),
    async put(record, expectedVersion) {
      await new Promise((resolve) => setTimeout(resolve, ms));
      return inner.put(record, expectedVersion);
    },
    due: (now, limit) => inner.due(now, limit),
    remove: (scope) => inner.remove(scope),
  };
}

const ingestedTotals = (h: ReturnType<typeof setup>) => {
  const ingested = h.traces.flatMap((t) => (t.kind === 'ingested' ? [t] : []));
  return { count: ingested.reduce((n, t) => n + t.count, 0), duplicates: ingested.reduce((n, t) => n + t.duplicates, 0) };
};

const WRITE_MS = 1;

/**
 * Drive the fake clock a second at a time until `total` events reached the sink or `budgetMs`
 * of wall time passed. Returns the wall time taken. Each fake second waits out a real store
 * write first: on a real clock a 1 ms write can never span a 10 s delivery lease, and the
 * fake clock must not outrun it either.
 */
async function catchUp(h: ReturnType<typeof setup>, total: number, budgetMs: number): Promise<number> {
  const started = performance.now();
  while (deliveredIds(h).length < total && performance.now() - started < budgetMs) {
    await new Promise((resolve) => setTimeout(resolve, 2 * WRITE_MS));
    await h.clock.advance(1000);
    await settle();
  }
  return performance.now() - started;
}

const BACKLOG = Array.from({ length: 2500 }, (_, i) => `e${i + 1}`);

describe('mcpEventsSource over a real Client and Server', () => {
  test('poll mode: events reach the sink as decisions and the cursor goes back to the server under the source key', async () => {
    const server = mcpEventsTestServer(ALPHA);
    server.nextPollMs = null;
    const client = await server.connect();
    const h = setup();
    await server.append('alpha', occ('e1'));
    void h.attach('s', client, { pollMs: 1000, arguments: { room: 'r1' } });
    await settle();
    await h.clock.advance(500);
    expect(h.sink.delivered).toEqual([
      expect.objectContaining({
        scope: 's',
        route: 'queue',
        window: expect.objectContaining({ events: [entry('e1')], reason: 'closing', gap: false }),
      }),
    ]);
    expect((await h.store.get('s'))?.cursors).toEqual({ 'mcp:alpha:{"room":"r1"}': '1' });
    await server.append('alpha', occ('e2'));
    await h.clock.advance(499);
    expect(server.requests.filter((r) => r.method === 'events/poll')).toHaveLength(1);
    await h.clock.advance(1);
    await settle();
    expect(server.requests.filter((r) => r.method === 'events/poll').map((r) => r.params)).toEqual([
      { name: 'alpha', arguments: { room: 'r1' }, cursor: null },
      { name: 'alpha', arguments: { room: 'r1' }, cursor: '1' },
    ]);
    await h.clock.advance(500);
    expect(h.delivered()).toEqual([
      { scope: 's', ids: ['e1'], gap: false },
      { scope: 's', ids: ['e2'], gap: false },
    ]);
    await h.stop();
  });

  test('poll mode: hasMore polls again at once and nextPollMs beats pollMs', async () => {
    const server = mcpEventsTestServer(ALPHA);
    server.nextPollMs = 250;
    const client = await server.connect();
    const h = setup();
    await server.append('alpha', occ('e1'), occ('e2'), occ('e3'));
    void h.attach('s', client, { maxEvents: 2, pollMs: 10_000 });
    await settle();
    expect(server.requests.filter((r) => r.method === 'events/poll').map((r) => r.params)).toEqual([
      { name: 'alpha', cursor: null, maxEvents: 2 },
      { name: 'alpha', cursor: '2', maxEvents: 2 },
    ]);
    await server.append('alpha', occ('e4'));
    await h.clock.advance(250);
    await settle();
    expect(server.requests.filter((r) => r.method === 'events/poll')).toHaveLength(3);
    await h.clock.advance(500);
    expect(h.delivered()).toEqual([{ scope: 's', ids: ['e1', 'e2', 'e3', 'e4'], gap: false }]);
    await h.stop();
  });

  test('stream mode: events reach the sink as decisions', async () => {
    const server = mcpEventsTestServer(ALPHA);
    const client = await server.connect();
    const h = setup();
    void h.attach('s', client, { mode: 'stream' });
    await settle();
    expect(server.requests.map((r) => r.method)).toEqual(['events/stream']);
    expect((await h.store.get('s'))?.cursors).toEqual({ [KEY]: '0' });
    await server.append('alpha', occ('e1'));
    await settle();
    expect((await h.store.get('s'))?.buffer.entries).toEqual([entry('e1')]);
    await h.clock.advance(500);
    expect(h.delivered()).toEqual([{ scope: 's', ids: ['e1'], gap: false }]);
    expect((await h.store.get('s'))?.cursors).toEqual({ [KEY]: '1' });
    await h.stop();
    expect(server.liveStreams('alpha')).toEqual([]);
  });

  test('a reconnect resumes from the committed cursor, after terminated and across a restart', async () => {
    const server = mcpEventsTestServer(ALPHA);
    const client = await server.connect();
    const store = memoryStore();
    const first = setup([{ route: 'queue' }], store);
    await server.append('alpha', occ('e1'));
    void first.attach('s', client, { mode: 'stream' });
    await settle();
    await first.clock.advance(500);
    expect(first.delivered()).toEqual([{ scope: 's', ids: ['e1'], gap: false }]);

    await server.terminate('alpha');
    await settle();
    expect(server.liveStreams('alpha')).toEqual([]);
    await server.append('alpha', occ('e2'));
    await first.clock.advance(1000);
    await settle();
    expect(first.streamRequests(server)).toEqual([
      { name: 'alpha', cursor: null },
      { name: 'alpha', cursor: '1' },
    ]);
    await first.clock.advance(500);
    expect(first.delivered().map((d) => d.ids)).toEqual([['e1'], ['e2']]);
    await first.stop();

    const second = setup([{ route: 'queue' }], store);
    const reconnected = await server.connect();
    await server.append('alpha', occ('e3'));
    void second.attach('s', reconnected, { mode: 'stream' });
    await settle();
    expect(server.requests.at(-1)).toEqual({ method: 'events/stream', params: { name: 'alpha', cursor: '2' } });
    await second.clock.advance(500);
    expect(second.delivered()).toEqual([{ scope: 's', ids: ['e3'], gap: false }]);
    await second.stop();
  });

  test('a redelivered eventId is deduped', async () => {
    const server = mcpEventsTestServer(ALPHA);
    const client = await server.connect();
    const h = setup();
    await server.append('alpha', occ('e1'));
    void h.attach('s', client, { pollMs: 1000 });
    await settle();
    await h.clock.advance(500);
    await server.append('alpha', occ('e1'), occ('e2'));
    await h.clock.advance(500);
    await settle();
    expect(h.traces).toContainEqual({ kind: 'ingested', scope: 's', count: 1, duplicates: 1 });
    await h.clock.advance(500);
    expect(h.delivered().map((d) => d.ids)).toEqual([['e1'], ['e2']]);
    await h.stop();
  });

  test('truncated sets window.gap on the delivered decision, from a poll result and from a stream activation', async () => {
    const server = mcpEventsTestServer(ALPHA);
    const client = await server.connect();
    const h = setup();
    server.truncateNext();
    await server.append('alpha', occ('p1'));
    void h.attach('polled', client, { pollMs: 1000 });
    await settle();
    expect(h.traces).toContainEqual({ kind: 'gap', scope: 'polled', source: KEY, cursor: '1' });
    await h.clock.advance(500);
    expect(h.delivered()).toEqual([{ scope: 'polled', ids: ['p1'], gap: true }]);

    server.truncateNext();
    void h.attach('streamed', client, { mode: 'stream' });
    await settle();
    expect(h.traces).toContainEqual({ kind: 'gap', scope: 'streamed', source: KEY, cursor: '0' });
    await server.append('alpha', occ('s1'));
    await h.clock.advance(500);
    expect(h.delivered().at(-1)).toEqual({ scope: 'streamed', ids: ['p1', 's1'], gap: true });
    await h.stop();
  });

  test('a heartbeat advances the stored cursor without opening a window', async () => {
    const server = mcpEventsTestServer(ALPHA);
    const client = await server.connect();
    const h = setup();
    void h.attach('s', client, { mode: 'stream' });
    await settle();
    expect((await h.store.get('s'))?.cursors).toEqual({ [KEY]: '0' });
    await server.heartbeat('alpha', '5');
    await settle();
    expect((await h.store.get('s'))?.cursors).toEqual({ [KEY]: '5' });
    await h.clock.advance(1000);
    expect(h.judge.calls).toEqual([]);
    await h.stop();
  });

  test('two concurrent streams on one Client route by subscription id, even when the second activates first', async () => {
    const server = mcpEventsTestServer([...ALPHA, { name: 'beta', delivery: ['push'] }]);
    const client = await server.connect();
    const h = setup();
    server.hold('alpha');
    void h.attach('x', client, { name: 'alpha', mode: 'stream' });
    void h.attach('y', client, { name: 'beta', mode: 'stream' });
    await settle();
    expect(server.liveStreams('beta')).toHaveLength(1);
    expect(server.liveStreams('alpha')).toHaveLength(0);
    expect((await h.store.get('y'))?.cursors).toEqual({ 'mcp:beta:{}': '0' });
    expect(await h.store.get('x')).toBeNull();
    await server.release('alpha');
    await settle();
    expect(server.liveStreams('alpha')).toHaveLength(1);
    expect(server.liveStreams('alpha')).not.toEqual(server.liveStreams('beta'));
    await server.append('beta', occ('b1', 'beta'));
    await server.append('alpha', occ('a1'));
    await h.clock.advance(500);
    expect(h.delivered().sort((a, b) => a.scope.localeCompare(b.scope))).toEqual([
      { scope: 'x', ids: ['a1'], gap: false },
      { scope: 'y', ids: ['b1'], gap: false },
    ]);
    expect(h.errors).toEqual([]);
    await h.stop();
  });

  test('-32026 in auto mode falls back to poll; in stream mode it rejects for good', async () => {
    const server = mcpEventsTestServer([{ name: 'alpha', delivery: ['push'] }, { name: 'beta', delivery: ['poll'] }]);
    const client = await server.connect();
    const h = setup();
    server.failNext('events/stream', -32026, 'push is off');
    void h.attach('s', client, { mode: 'auto' });
    await settle();
    expect(server.requests.map((r) => r.method)).toEqual(['events/list', 'events/stream', 'events/poll']);
    await server.append('alpha', occ('e1'));
    await h.clock.advance(1000);
    await h.clock.advance(500);
    expect(h.delivered()).toEqual([{ scope: 's', ids: ['e1'], gap: false }]);

    void h.attach('listed-poll', client, { name: 'beta', mode: 'auto' });
    await settle();
    expect(server.requests.slice(-3).map((r) => [r.method, r.params])).toEqual([
      ['events/list', {}],
      ['events/list', { cursor: '1' }],
      ['events/poll', { name: 'beta', cursor: null }],
    ]);

    server.failNext('events/stream', -32026, 'push is off');
    await expect(h.attend.attach('strict', h.source(client, { mode: 'stream' }))).rejects.toMatchObject({
      name: 'PermanentError',
      message: 'attend/mcp: MCP error -32026: push is off',
      cause: { code: -32026 },
    });
    await h.stop();
  });

  test('-32023 rejects attach for good; -32025 is thrown plain, so attach backs off and resubscribes', async () => {
    const server = mcpEventsTestServer(ALPHA);
    const client = await server.connect();
    const h = setup();
    await expect(h.attend.attach('missing', h.source(client, { name: 'nope' }))).rejects.toMatchObject({ name: 'PermanentError', cause: { code: -32023 } });

    server.failNext('events/poll', -32025, 'slow down');
    void h.attach('s', client, { pollMs: 1000 });
    await settle();
    expect(h.errors).toEqual([]);
    expect(h.traces).toContainEqual({ kind: 'retrying', scope: 's', stage: 'source', id: KEY, attempt: 1, at: 2000, message: 'MCP error -32025: slow down' });
    await server.append('alpha', occ('e1'));
    await h.clock.advance(1000);
    await h.clock.advance(500);
    expect(h.delivered()).toEqual([{ scope: 's', ids: ['e1'], gap: false }]);
    await h.stop();
  });

  test('an events/list error follows the same policy: retryable resubscribes, the rest reject for good', async () => {
    const server = mcpEventsTestServer(ALPHA);
    const client = await server.connect();
    const h = setup();
    server.failNext('events/list', -32025, 'busy');
    void h.attach('s', client, { mode: 'auto' });
    await settle();
    expect(h.traces).toContainEqual({ kind: 'retrying', scope: 's', stage: 'source', id: KEY, attempt: 1, at: 2000, message: 'MCP error -32025: busy' });
    await h.clock.advance(1000);
    await settle();
    expect(server.requests.map((r) => r.method)).toEqual(['events/list', 'events/list', 'events/stream']);
    server.failNext('events/list', -32027, 'forbidden');
    await expect(h.attend.attach('t', h.source(client, { mode: 'auto' }))).rejects.toMatchObject({
      name: 'PermanentError',
      message: 'attend/mcp: MCP error -32027: forbidden',
      cause: { code: -32027 },
    });
    await h.stop();
  });

  test('a stream error notification is reported and the stream stays open', async () => {
    const server = mcpEventsTestServer(ALPHA);
    const client = await server.connect();
    const h = setup();
    void h.attach('s', client, { mode: 'stream' });
    await settle();
    await server.sendError('alpha', { code: -32025, message: 'slow down', data: { retryAfterMs: 5 } });
    await settle();
    expect(h.errors).toEqual([{ kind: 'stream', code: -32025, message: 'slow down', data: { retryAfterMs: 5 } }]);
    await server.append('alpha', occ('e1'));
    await h.clock.advance(500);
    expect(h.delivered()).toEqual([{ scope: 's', ids: ['e1'], gap: false }]);
    await h.stop();
  });

  test('a malformed occurrence on a stream is skipped and reported, and its cursor is kept', async () => {
    const server = mcpEventsTestServer(ALPHA);
    const client = await server.connect();
    const h = setup();
    void h.attach('s', client, { mode: 'stream' });
    await settle();
    const id = server.liveStreams('alpha')[0];
    await server.append('alpha', { name: 'alpha', timestamp: 'yesterday', data: {} }, occ('e2'));
    await settle();
    expect(h.errors).toEqual([
      {
        kind: 'malformed',
        method: 'notifications/events/event',
        message: expect.stringContaining('eventId'),
        payload: { name: 'alpha', timestamp: 'yesterday', data: {}, cursor: '1', _meta: { [SUBSCRIPTION_KEY]: id } },
      },
    ]);
    expect((await h.store.get('s'))?.cursors).toEqual({ [KEY]: '2' });
    await h.clock.advance(500);
    expect(h.delivered()).toEqual([{ scope: 's', ids: ['e2'], gap: false }]);
    await h.stop();
  });

  test('a malformed occurrence in a poll result is skipped and reported, and the rest of the page lands', async () => {
    const server = mcpEventsTestServer(ALPHA);
    const client = await server.connect();
    const h = setup();
    await server.append('alpha', occ('e1'), { eventId: 'e2', name: 'alpha', timestamp: STAMP, data: 'not an object' }, occ('e3'));
    void h.attach('s', client, { pollMs: 1000 });
    await settle();
    expect(h.errors).toEqual([
      {
        kind: 'malformed',
        method: 'events/poll',
        message: expect.stringContaining('data'),
        payload: { eventId: 'e2', name: 'alpha', timestamp: STAMP, data: 'not an object' },
      },
    ]);
    expect((await h.store.get('s'))?.cursors).toEqual({ [KEY]: '3' });
    await h.clock.advance(500);
    expect(h.delivered()).toEqual([{ scope: 's', ids: ['e1', 'e3'], gap: false }]);
    await h.stop();
  });

  test('past maxBuffered the stream closes, reopens at once from the committed cursor, and every event reaches the sink once', async () => {
    const server = mcpEventsTestServer(ALPHA);
    const client = await server.connect();
    const slow = heldStore();
    const h = setup([{ route: 'queue' }], slow.store);
    void h.attach('s', client, { mode: 'stream', maxBuffered: 2 });
    await settle();
    slow.hold();
    await server.append('alpha', occ('e1'), occ('e2'), occ('e3'), occ('e4'), occ('e5'), occ('e6'));
    await settle();
    expect(server.liveStreams('alpha')).toEqual([]);

    slow.release();
    await settle();
    expect(h.errors).toEqual([{ kind: 'overflow', buffered: 2 }]);
    expect(h.streamRequests(server)).toEqual([
      { name: 'alpha', cursor: null },
      { name: 'alpha', cursor: '3' },
    ]);
    expect((await h.store.get('s'))?.cursors).toEqual({ [KEY]: '6' });

    for (let round = 0; round < 10 && deliveredIds(h).length < 6; round += 1) await h.clock.advance(1000);
    expect(deliveredIds(h)).toEqual(['e1', 'e2', 'e3', 'e4', 'e5', 'e6']);
    expect(h.errors.filter((e) => !(e.kind === 'overflow' && e.buffered === 2))).toEqual([]);
    const ingested = h.traces.flatMap((t) => (t.kind === 'ingested' ? [t] : []));
    expect({ count: ingested.reduce((n, t) => n + t.count, 0), duplicates: ingested.reduce((n, t) => n + t.duplicates, 0) }).toEqual({ count: 6, duplicates: 0 });
    await h.stop();
  });

  test('without cursors an overflow still reports and reopens, and only the dropped events are lost', async () => {
    const server = mcpEventsTestServer([{ name: 'alpha', delivery: ['push'], replay: false }]);
    const client = await server.connect();
    const slow = heldStore();
    const h = setup([{ route: 'queue' }], slow.store);
    void h.attach('s', client, { mode: 'stream', maxBuffered: 2 });
    await settle();
    slow.hold();
    await server.append('alpha', occ('e1'), occ('e2'), occ('e3'), occ('e4'));
    await settle();
    slow.release();
    await settle();
    expect(h.errors).toEqual([{ kind: 'overflow', buffered: 2 }]);
    await h.clock.advance(1000);
    expect(h.streamRequests(server)).toEqual([
      { name: 'alpha', cursor: null },
      { name: 'alpha', cursor: null },
    ]);
    await server.append('alpha', occ('e5'));
    await h.clock.advance(1000);
    expect(deliveredIds(h)).toEqual(['e1', 'e2', 'e3', 'e5']);
    await h.stop();
  });

  test('a burst backlog past maxBuffered on a slow store catches up: each overflow persists what it holds before reopening', async () => {
    const server = mcpEventsTestServer(ALPHA);
    server.burst = true;
    const client = await server.connect();
    const h = setup([{ route: 'queue' }], slowStore(WRITE_MS));
    await server.append('alpha', ...BACKLOG.map((id) => occ(id)));
    void h.attach('s', client, { mode: 'stream' });
    // Three stream cycles, each a handful of 1 ms writes: well under a second. The livelock
    // never finishes, and one write per event would spend 2.5 s in the store alone.
    const elapsed = await catchUp(h, BACKLOG.length, 5000);
    expect({ delivered: deliveredIds(h).length, under5s: elapsed < 5000 }).toEqual({ delivered: 2500, under5s: true });
    expect(deliveredIds(h)).toEqual(BACKLOG);
    expect(ingestedTotals(h)).toEqual({ count: 2500, duplicates: 0 });
    expect(h.errors).toEqual([
      { kind: 'overflow', buffered: 1000 },
      { kind: 'overflow', buffered: 1000 },
    ]);
    expect(h.streamRequests(server)).toEqual([
      { name: 'alpha', cursor: null },
      { name: 'alpha', cursor: '1000' },
      { name: 'alpha', cursor: '2000' },
    ]);
    expect((await h.store.get('s'))?.cursors).toEqual({ [KEY]: '2500' });
    await h.stop();
  }, 15_000);

  test('a burst backlog past maxBuffered on a memory store reaches the sink once each, in order', async () => {
    const server = mcpEventsTestServer(ALPHA);
    server.burst = true;
    const client = await server.connect();
    const h = setup();
    await server.append('alpha', ...BACKLOG.map((id) => occ(id)));
    void h.attach('s', client, { mode: 'stream' });
    const elapsed = await catchUp(h, BACKLOG.length, 5000);
    expect({ delivered: deliveredIds(h).length, under5s: elapsed < 5000 }).toEqual({ delivered: 2500, under5s: true });
    expect(deliveredIds(h)).toEqual(BACKLOG);
    expect(ingestedTotals(h)).toEqual({ count: 2500, duplicates: 0 });
    expect((await h.store.get('s'))?.cursors).toEqual({ [KEY]: '2500' });
    await h.stop();
  }, 15_000);

  test('a server that replays in bursts at 3000 events a second is kept up with, because the stream reopens at once after an overflow', async () => {
    const server = mcpEventsTestServer(ALPHA);
    server.burst = true;
    const client = await server.connect();
    const h = setup();
    void h.attach('s', client, { mode: 'stream' });
    await settle();
    const RATE = 3000;
    const SECONDS = 4;
    const all = Array.from({ length: RATE * SECONDS }, (_, i) => `e${i + 1}`);
    const cursorAt = async () => (await h.store.get('s'))?.cursors[KEY] ?? null;
    const committed: (string | null)[] = [];
    const started = performance.now();
    for (let second = 1; second <= SECONDS; second += 1) {
      await server.append('alpha', ...all.slice((second - 1) * RATE, second * RATE).map((id) => occ(id)));
      await h.clock.advance(1000);
      const budget = performance.now() + 1500;
      while ((await cursorAt()) !== String(second * RATE) && performance.now() < budget) await settle();
      committed.push(await cursorAt());
    }
    const elapsed = performance.now() - started;
    expect({ committed, under3s: elapsed < 3000 }).toEqual({ committed: ['3000', '6000', '9000', '12000'], under3s: true });
    expect(ingestedTotals(h)).toEqual({ count: RATE * SECONDS, duplicates: 0 });
    expect(h.errors.filter((e) => e.kind !== 'overflow')).toEqual([]);
    await h.clock.advance(1000);
    await h.clock.advance(1000);
    expect(deliveredIds(h)).toEqual(all);
    await h.stop();
  }, 30_000);

  test('a stream with no notification for idleTimeoutMs is reported idle and reopened from the committed cursor', async () => {
    const server = mcpEventsTestServer(ALPHA);
    const client = await server.connect();
    const h = setup();
    void h.attach('s', client, { mode: 'stream', idleTimeoutMs: 5000 });
    await settle();
    expect(server.liveStreams('alpha')).toHaveLength(1);
    await h.clock.advance(4999);
    await settle();
    expect(h.errors).toEqual([]);
    await h.clock.advance(1);
    await settle();
    expect(h.errors).toEqual([{ kind: 'idle' }]);
    expect(server.liveStreams('alpha')).toEqual([]);
    await h.clock.advance(1000);
    await settle();
    expect(h.streamRequests(server)).toEqual([
      { name: 'alpha', cursor: null },
      { name: 'alpha', cursor: '0' },
    ]);
    await h.clock.advance(3000);
    await server.heartbeat('alpha', '0');
    await settle();
    await h.clock.advance(3000);
    await settle();
    expect(h.errors).toEqual([{ kind: 'idle' }]);
    expect(server.liveStreams('alpha')).toHaveLength(1);
    await h.stop();
  });

  test('a closed Client ends the subscription with a PermanentError that says to reconnect', async () => {
    const server = mcpEventsTestServer(ALPHA);
    const client = await server.connect();
    const h = setup();
    const attached = h.attend.attach('s', h.source(client, { mode: 'stream' }));
    await settle();
    expect(server.liveStreams('alpha')).toHaveLength(1);
    await client.close();
    await expect(attached).rejects.toThrow('attend/mcp: the Client closed; reconnect it and attach again');
    await expect(attached).rejects.toBeInstanceOf(PermanentError);
    await expect(attached).rejects.toMatchObject({ cause: { code: -32000 } });
    await h.stop();
  });

  test('a Client closed between polls, or before an auto-mode list, ends the subscription with the same PermanentError', async () => {
    const server = mcpEventsTestServer(ALPHA);
    const client = await server.connect();
    const h = setup();
    const polling = h.attend.attach('s', h.source(client, { pollMs: 1000 }));
    void polling.catch(() => undefined);
    await settle();
    expect(server.requests.map((r) => r.method)).toEqual(['events/poll']);
    await client.close();
    await h.clock.advance(1000);
    await expect(polling).rejects.toThrow('attend/mcp: the Client closed; reconnect it and attach again');
    await expect(polling).rejects.toBeInstanceOf(PermanentError);
    await expect(h.attend.attach('t', h.source(client, { mode: 'auto' }))).rejects.toThrow('attend/mcp: the Client closed; reconnect it and attach again');
    await h.stop();
  });

  test('a Client closed while a poll source waits between polls ends the subscription at once, not at the next poll', async () => {
    const server = mcpEventsTestServer(ALPHA);
    server.nextPollMs = null;
    const client = await server.connect();
    const h = setup();
    const polling = h.attend.attach('s', h.source(client, { pollMs: 10_000 }));
    void polling.catch(() => undefined);
    await settle();
    expect(server.requests.map((r) => r.method)).toEqual(['events/poll']);
    await client.close();
    const outcome = await Promise.race([polling.then(() => 'resolved', (error: unknown) => error), settle().then(() => 'pending')]);
    expect(outcome).toBeInstanceOf(PermanentError);
    expect(outcome).toMatchObject({ message: 'attend/mcp: the Client closed; reconnect it and attach again' });
    await h.stop();
  });

  test('a Client closed during the pause before a terminated stream reopens ends the subscription at once', async () => {
    const server = mcpEventsTestServer(ALPHA);
    const client = await server.connect();
    const h = setup();
    const streaming = h.attend.attach('s', h.source(client, { mode: 'stream' }));
    void streaming.catch(() => undefined);
    await settle();
    await server.terminate('alpha');
    await settle();
    expect(server.liveStreams('alpha')).toEqual([]);
    await client.close();
    const outcome = await Promise.race([streaming.then(() => 'resolved', (error: unknown) => error), settle().then(() => 'pending')]);
    expect(outcome).toBeInstanceOf(PermanentError);
    expect(outcome).toMatchObject({ message: 'attend/mcp: the Client closed; reconnect it and attach again' });
    await h.stop();
  });

  test('a notification for a subscription no stream here opened is reported as unrouted and changes nothing', async () => {
    const server = mcpEventsTestServer(ALPHA);
    const client = await server.connect();
    const h = setup();
    void h.attach('s', client, { mode: 'stream' });
    await settle();
    await server.stray('event', 'nope', { ...occ('x'), cursor: '9' });
    await server.stray('heartbeat', null, { cursor: '9' });
    await settle();
    expect(h.errors).toEqual([
      { kind: 'unrouted', method: 'notifications/events/event', subscriptionId: 'nope' },
      { kind: 'unrouted', method: 'notifications/events/heartbeat', subscriptionId: null },
    ]);
    expect((await h.store.get('s'))?.cursors).toEqual({ [KEY]: '0' });
    expect((await h.store.get('s'))?.buffer.entries).toEqual([]);
    await h.stop();
  });

  test('maxBuffered below one is rejected when the source is made', async () => {
    const server = mcpEventsTestServer(ALPHA);
    const client = await server.connect();
    expect(() => mcpEventsSource(client, { name: 'alpha', maxBuffered: 0 })).toThrow('attend/mcp: maxBuffered must be a positive integer, got 0');
    expect(mcpEventsSource(client, { name: 'alpha', maxBuffered: 1 })).toEqual({ key: KEY, subscribe: expect.any(Function) });
  });
});
