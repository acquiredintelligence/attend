/**
 * A real SDK `Server` that speaks the MCP Events draft (`events/list`, `events/poll`,
 * `events/stream`) over `InMemoryTransport`, scriptable from tests: append occurrences,
 * force `truncated`, hold and release a stream's activation, terminate a stream, send an
 * error notification, send a heartbeat, and answer the next request with an error code.
 * Cursors are the count of occurrences before the position, as strings. Errors go out as a
 * plain `Error` with a `code`, so the wire message is exactly what the test scripted.
 */
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import type { RequestId, ServerCapabilities } from '@modelcontextprotocol/sdk/types.js';
import { z } from 'zod';

const SUBSCRIPTION_KEY = 'io.modelcontextprotocol/subscriptionId';

type Method = 'events/list' | 'events/poll' | 'events/stream';
type Occurrence = Record<string, unknown>;

export interface EventSpec {
  name: string;
  delivery: ('poll' | 'push' | 'webhook')[];
}

export interface McpEventsTestServer {
  /** Connect a fresh Client over a new in-memory pair, closing the previous one. */
  connect(): Promise<Client>;
  /** Every request received, oldest first. */
  requests: { method: Method; params: unknown }[];
  /** Request ids of the streams open right now, by event name. */
  liveStreams(name: string): RequestId[];
  /** Append occurrences to an event's log; open streams hear them at once. */
  append(name: string, ...occurrences: Occurrence[]): Promise<void>;
  /** The next poll result or stream activation reports `truncated: true`. */
  truncateNext(): void;
  /** Streams for `name` wait to send `active` until `release(name)`. */
  hold(name: string): void;
  release(name: string): Promise<void>;
  terminate(name: string): Promise<void>;
  sendError(name: string, error: { code: number; message: string; data?: unknown }): Promise<void>;
  heartbeat(name: string, cursor?: string): Promise<void>;
  /** Answer the next request for `method` with this error instead. */
  failNext(method: Method, code: number, message?: string): void;
  /** `nextPollMs` in poll results; null leaves the field out. Default 1000. */
  nextPollMs: number | null;
}

const ListRequest = z.object({ method: z.literal('events/list'), params: z.object({ cursor: z.string().optional() }).optional() });
const SubscriptionParams = z.object({
  name: z.string(),
  arguments: z.record(z.string(), z.unknown()).optional(),
  cursor: z.string().nullable(),
  maxAgeMs: z.number().optional(),
  maxEvents: z.number().optional(),
});
const PollRequest = z.object({ method: z.literal('events/poll'), params: SubscriptionParams });
const StreamRequest = z.object({ method: z.literal('events/stream'), params: SubscriptionParams });

interface LiveStream {
  name: string;
  id: RequestId;
  /** Index of the next occurrence to send. */
  next: number;
  pumping: boolean;
  send(kind: string, params: Record<string, unknown>): Promise<void>;
  end(): void;
}

export function mcpEventsTestServer(events: EventSpec[]): McpEventsTestServer {
  const logs = new Map<string, Occurrence[]>(events.map((e) => [e.name, []]));
  const streams = new Set<LiveStream>();
  const held = new Map<string, (() => void)[]>();
  const failures = new Map<Method, { code: number; message: string }>();
  const requests: McpEventsTestServer['requests'] = [];
  let truncated = false;
  let transport: InMemoryTransport | null = null;

  const capabilities: ServerCapabilities & { events: { listChanged: boolean } } = { events: { listChanged: true } };
  const server = new Server({ name: 'attend-events-test', version: '0.0.0' }, { capabilities });

  function admit(method: Method, params: unknown): void {
    requests.push({ method, params });
    const failure = failures.get(method);
    if (failure) {
      failures.delete(method);
      throw Object.assign(new Error(failure.message), { code: failure.code });
    }
  }

  function logOf(name: string): Occurrence[] {
    const log = logs.get(name);
    if (!log) throw Object.assign(new Error(`unknown event ${name}`), { code: -32023 });
    return log;
  }

  function index(cursor: string | null): number {
    return cursor === null ? 0 : Number(cursor);
  }

  function takeTruncated(): boolean {
    const value = truncated;
    truncated = false;
    return value;
  }

  async function pump(stream: LiveStream): Promise<void> {
    if (stream.pumping) return;
    stream.pumping = true;
    try {
      const log = logOf(stream.name);
      while (stream.next < log.length) {
        const occurrence = log[stream.next];
        stream.next += 1;
        await stream.send('event', { ...occurrence, cursor: String(stream.next) });
      }
    } finally {
      stream.pumping = false;
    }
  }

  server.setRequestHandler(ListRequest, async (request) => {
    admit('events/list', request.params);
    const at = request.params?.cursor === undefined ? 0 : Number(request.params.cursor);
    const spec = events[at];
    const page = spec ? [{ name: spec.name, description: `the ${spec.name} event`, delivery: spec.delivery, inputSchema: { type: 'object' }, payloadSchema: { type: 'object' } }] : [];
    return at + 1 < events.length ? { events: page, nextCursor: String(at + 1) } : { events: page };
  });

  server.setRequestHandler(PollRequest, async (request) => {
    admit('events/poll', request.params);
    const log = logOf(request.params.name);
    const from = index(request.params.cursor);
    const page = log.slice(from, from + (request.params.maxEvents ?? 100));
    const to = from + page.length;
    const result: Record<string, unknown> = { events: page, cursor: String(to), truncated: takeTruncated(), hasMore: to < log.length };
    if (api.nextPollMs !== null) result.nextPollMs = api.nextPollMs;
    return result;
  });

  server.setRequestHandler(StreamRequest, async (request, extra) => {
    admit('events/stream', request.params);
    const name = request.params.name;
    logOf(name);
    const meta = { [SUBSCRIPTION_KEY]: extra.requestId };
    const stream: LiveStream = {
      name,
      id: extra.requestId,
      next: index(request.params.cursor),
      pumping: false,
      send: (kind, params) => extra.sendNotification({ method: `notifications/events/${kind}`, params: { ...params, _meta: meta } }),
      end: () => undefined,
    };
    const done = new Promise<void>((resolve) => {
      stream.end = resolve;
      extra.signal.addEventListener('abort', () => resolve(), { once: true });
    });
    const gate = held.get(name);
    if (gate) await new Promise<void>((resolve) => gate.push(resolve));
    streams.add(stream);
    await stream.send('active', { cursor: String(stream.next), truncated: takeTruncated() });
    await pump(stream);
    await done;
    streams.delete(stream);
    return { _meta: {} };
  });

  const of = (name: string) => [...streams].filter((s) => s.name === name);

  const api: McpEventsTestServer = {
    requests,
    nextPollMs: 1000,
    async connect() {
      if (transport) await transport.close();
      const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
      transport = serverTransport;
      await server.connect(serverTransport);
      const client = new Client({ name: 'attend-test', version: '0.0.0' });
      await client.connect(clientTransport);
      return client;
    },
    liveStreams: (name) => of(name).map((s) => s.id),
    async append(name, ...occurrences) {
      logOf(name).push(...occurrences);
      await Promise.all(of(name).map(pump));
    },
    truncateNext() {
      truncated = true;
    },
    hold(name) {
      held.set(name, []);
    },
    async release(name) {
      const gate = held.get(name) ?? [];
      held.delete(name);
      for (const open of gate) open();
    },
    async terminate(name) {
      for (const s of of(name)) {
        await s.send('terminated', {});
        s.end();
      }
    },
    async sendError(name, error) {
      for (const s of of(name)) await s.send('error', { error });
    },
    async heartbeat(name, cursor) {
      for (const s of of(name)) await s.send('heartbeat', { cursor: cursor ?? String(s.next) });
    },
    failNext(method, code, message = `error ${code}`) {
      failures.set(method, { code, message });
    },
  };
  return api;
}
