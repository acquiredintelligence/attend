import { describe, expect, test } from 'vitest';
import type { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { createAttend } from '../src/attend.js';
import { mcpEventsSource, type McpEventsError, type McpEventsOptions } from '../src/mcp.js';
import { memoryStore } from '../src/memory.js';
import { drain, fakeClock, recordingSink, scriptedJudge, type ScriptedAnswer } from '../src/testing.js';
import type { DeadLetter, Event, Kinded, StateStore, Trace } from '../src/types.js';
import { mcpEventsTestServer, type EventSpec } from './support/mcp-events-server.js';

const SUBSCRIPTION_KEY = 'io.modelcontextprotocol/subscriptionId';
const STAMP = '2026-10-15T12:00:00.000Z';
const closing = (): Kinded => ({ kind: 'closing' });
const occ = (eventId: string, name = 'alpha') => ({ eventId, name, timestamp: STAMP, data: { n: eventId.length } });
const entry = (id: string, name = 'alpha'): Event & Kinded & { seenAt: number } => ({ id, at: Date.parse(STAMP), name, data: { n: id.length }, kind: 'closing', seenAt: 1000 });

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
  return { clock, store, judge, sink, deadLetters, traces, errors, attend, source, attach, stop, delivered };
}

const ALPHA: EventSpec[] = [{ name: 'alpha', delivery: ['poll', 'push'] }];

describe('mcpEventsSource over a real Client and Server', () => {
  test('poll mode: events reach the sink as decisions and the cursor goes back to the server', async () => {
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
    expect((await h.store.get('s'))?.cursor).toBe('1');
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
    expect((await h.store.get('s'))?.cursor).toBe('0');
    await server.append('alpha', occ('e1'));
    await settle();
    expect((await h.store.get('s'))?.buffer.entries).toEqual([entry('e1')]);
    await h.clock.advance(500);
    expect(h.delivered()).toEqual([{ scope: 's', ids: ['e1'], gap: false }]);
    expect((await h.store.get('s'))?.cursor).toBe('1');
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
    expect(server.requests.filter((r) => r.method === 'events/stream').map((r) => r.params)).toEqual([
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
    expect(h.traces).toContainEqual({ kind: 'gap', scope: 'polled', cursor: '1' });
    await h.clock.advance(500);
    expect(h.delivered()).toEqual([{ scope: 'polled', ids: ['p1'], gap: true }]);

    server.truncateNext();
    void h.attach('streamed', client, { mode: 'stream' });
    await settle();
    expect(h.traces).toContainEqual({ kind: 'gap', scope: 'streamed', cursor: '0' });
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
    expect((await h.store.get('s'))?.cursor).toBe('0');
    await server.heartbeat('alpha', '5');
    await settle();
    expect((await h.store.get('s'))?.cursor).toBe('5');
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
    expect((await h.store.get('y'))?.cursor).toBe('0');
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
    await h.stop();
  });

  test('-32026 in auto mode falls back to poll; in stream mode it rejects', async () => {
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
    await expect(h.attend.attach('strict', h.source(client, { mode: 'stream' }))).rejects.toMatchObject({ code: -32026 });
    await h.stop();
  });

  test('-32023 rejects attach and -32025 is retried after the backoff and reported', async () => {
    const server = mcpEventsTestServer(ALPHA);
    const client = await server.connect();
    const h = setup();
    await expect(h.attend.attach('missing', h.source(client, { name: 'nope' }))).rejects.toMatchObject({ code: -32023 });

    server.failNext('events/poll', -32025, 'slow down');
    void h.attach('s', client, { pollMs: 1000 });
    await settle();
    expect(h.errors).toEqual([{ kind: 'retry', message: 'MCP error -32025: slow down', code: -32025, delayMs: 1000 }]);
    await server.append('alpha', occ('e1'));
    await h.clock.advance(1000);
    await h.clock.advance(500);
    expect(h.delivered()).toEqual([{ scope: 's', ids: ['e1'], gap: false }]);
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
    expect((await h.store.get('s'))?.cursor).toBe('2');
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
    expect((await h.store.get('s'))?.cursor).toBe('3');
    await h.clock.advance(500);
    expect(h.delivered()).toEqual([{ scope: 's', ids: ['e1', 'e3'], gap: false }]);
    await h.stop();
  });
});
