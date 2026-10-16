/**
 * `attend/mcp`: an MCP Events subscription (`events/stream` or `events/poll`) as a
 * `Source`. Peer dependency: `@modelcontextprotocol/sdk`. The draft's wire shapes stay
 * inside this module; the public surface takes an SDK `Client` and emits domain events.
 */
import type { Client } from '@modelcontextprotocol/sdk/client/index.js';
import type { Transport } from '@modelcontextprotocol/sdk/shared/transport.js';
import type { JSONRPCMessage } from '@modelcontextprotocol/sdk/types.js';
import { ErrorCode } from '@modelcontextprotocol/sdk/types.js';
import { z } from 'zod';
import { sleep, systemClock } from './internal/clock.js';
import type { Clock, Event, JsonObject, Source, SourceBatch } from './types.js';
import { DEFAULT_POLICY } from './types.js';

export interface McpEventsOptions {
  /** An event name from `events/list`. */
  name: string;
  arguments?: JsonObject;
  /** `auto` streams when `events/list` says the event supports `push`, else polls. Default `auto`. */
  mode?: 'stream' | 'poll' | 'auto';
  /** Bound on replay when resuming from a cursor; passed through as `maxAgeMs`. */
  maxAgeMs?: number;
  /** Poll page size; passed through as `maxEvents`. */
  maxEvents?: number;
  /** Poll interval when the server sends no `nextPollMs`. Default 10000. */
  pollMs?: number;
  /**
   * How many stream notifications may wait while `emit` persists an earlier one. Past that, the
   * source drops what it holds, reports `overflow`, and reopens the stream from the last
   * committed cursor, so the server sends them again. Dropped events are lost only for an event
   * type the server cannot replay from a cursor. Default 1000.
   */
  maxBuffered?: number;
  clock?: Clock;
  /** Everything the source survives: a server error notification, a skipped payload, a retry, an overflow. */
  onError?: (error: McpEventsError) => void;
}

export type McpEventsError =
  /** The server's `notifications/events/error`; the stream stays open. */
  | { kind: 'stream'; code: number; message: string; data: unknown }
  /** A payload that failed validation and was skipped, with its cursor kept. */
  | { kind: 'malformed'; method: string; message: string; payload: unknown }
  /** A request failed with a retryable error; the source retries after `delayMs`. */
  | { kind: 'retry'; message: string; code: number | null; delayMs: number }
  /** `buffered` notifications were waiting on `emit`; the stream reopens from the committed cursor. */
  | { kind: 'overflow'; buffered: number };

type Delivery = 'stream' | 'poll';
type Policy = 'permanent' | 'retry' | 'fallback';

/** What a request error means to the subscription. Codes not listed are permanent. */
const ERROR_POLICY: Readonly<Record<number, Policy>> = {
  [-32023]: 'permanent',
  [-32024]: 'permanent',
  [-32025]: 'retry',
  [-32026]: 'fallback',
  [-32027]: 'permanent',
  [ErrorCode.RequestTimeout]: 'retry',
};

/** The longest delay `setTimeout` honours; past it the SDK's timeout fires at once. */
const STREAM_TIMEOUT_MS = 2 ** 31 - 1;
const SUBSCRIPTION_KEY = 'io.modelcontextprotocol/subscriptionId';
const STREAM_METHODS = ['active', 'event', 'heartbeat', 'error', 'terminated'] as const;
type StreamMethod = (typeof STREAM_METHODS)[number];

// ─── wire schemas ────────────────────────────────────────────────────────────

const Occurrence = z.object({
  eventId: z.string().min(1),
  name: z.string().min(1),
  timestamp: z.string().refine((s) => Number.isFinite(Date.parse(s)), 'not a parseable timestamp'),
  data: z.record(z.string(), z.json()),
  cursor: z.string().nullable().optional(),
});
const Active = z.object({ cursor: z.string().nullable().optional(), truncated: z.boolean().optional() });
const Heartbeat = z.object({ cursor: z.string().nullable().optional() });
const StreamError = z.object({ error: z.object({ code: z.number(), message: z.string(), data: z.unknown().optional() }) });
const StreamResult = z.looseObject({});
const PollResult = z.object({
  events: z.array(z.unknown()),
  cursor: z.string().nullable(),
  truncated: z.boolean().optional(),
  hasMore: z.boolean().optional(),
  nextPollMs: z.number().optional(),
});
const ListResult = z.object({
  events: z.array(z.looseObject({ name: z.string(), delivery: z.array(z.string()).optional() })),
  nextCursor: z.string().optional(),
});
const Routed = z.looseObject({ _meta: z.looseObject({ [SUBSCRIPTION_KEY]: z.union([z.string(), z.number()]).optional() }).optional() });
const envelope = <M extends string>(method: M) => z.object({ method: z.literal(method), params: Routed.optional() });

// ─── the per-Client demux ────────────────────────────────────────────────────

type Note = { kind: StreamMethod; params: unknown };
type Item = Note | { kind: 'done' } | { kind: 'failed'; error: unknown } | { kind: 'overflow'; buffered: number };

interface Subscription {
  id: string | null;
  push(note: Note): void;
}

interface Demux {
  /** Streams whose request id is known, by id. */
  live: Map<string, Subscription>;
  /** Streams whose request has not been sent yet, by the params object the request will carry. */
  pending: WeakMap<object, Subscription>;
}

const demuxes = new WeakMap<Client, Demux>();
const wrappedTransports = new WeakSet<Transport>();

/**
 * The SDK never returns a request's JSON-RPC id, so the demux wraps `transport.send` and
 * reads the id off each outgoing `events/stream` as it leaves. `request()` sends inside its
 * Promise executor with the caller's `params` object by reference, so identity pairs the
 * outgoing message with its subscription even when several streams open at once.
 */
function demuxFor(client: Client): Demux {
  let demux = demuxes.get(client);
  if (!demux) {
    demux = { live: new Map(), pending: new WeakMap() };
    demuxes.set(client, demux);
    for (const kind of STREAM_METHODS) {
      const d = demux;
      client.setNotificationHandler(envelope(`notifications/events/${kind}`), (notification) => {
        const id = notification.params?._meta?.[SUBSCRIPTION_KEY];
        if (id !== undefined) d.live.get(String(id))?.push({ kind, params: notification.params });
      });
    }
  }
  const transport = client.transport;
  if (transport && !wrappedTransports.has(transport)) {
    wrappedTransports.add(transport);
    const d = demux;
    const send = transport.send.bind(transport);
    transport.send = (message, options) => {
      if (isStreamRequest(message)) {
        const subscription = d.pending.get(message.params);
        if (subscription) {
          d.pending.delete(message.params);
          subscription.id = String(message.id);
          d.live.set(subscription.id, subscription);
        }
      }
      return send(message, options);
    };
  }
  return demux;
}

function isStreamRequest(message: JSONRPCMessage): message is JSONRPCMessage & { id: string | number; method: 'events/stream'; params: object } {
  return 'id' in message && 'method' in message && message.method === 'events/stream' && typeof message.params === 'object' && message.params !== null;
}

/**
 * The queue between the SDK's notification handlers and the stream loop, which takes the next
 * item only once `emit` resolves. A notification past `max` empties it, drops every later one,
 * calls `onOverflow`, and leaves `overflow` as the next item. The request's outcome always queues.
 */
function inbox(max: number, onOverflow: () => void): { push(note: Note): void; close(item: Item): void; next(): Promise<Item> } {
  let items: Item[] = [];
  let overflowed = false;
  let wake: (() => void) | null = null;
  const enqueue = (item: Item) => {
    items.push(item);
    wake?.();
    wake = null;
  };
  return {
    push(note) {
      if (overflowed) return;
      if (items.length < max) {
        enqueue(note);
        return;
      }
      overflowed = true;
      const buffered = items.length;
      items = [];
      enqueue({ kind: 'overflow', buffered });
      onOverflow();
    },
    close: enqueue,
    async next() {
      for (;;) {
        const item = items.shift();
        if (item !== undefined) return item;
        await new Promise<void>((resolve) => (wake = resolve));
      }
    },
  };
}

// ─── the source ──────────────────────────────────────────────────────────────

type Outcome = { kind: 'ended' } | { kind: 'failed'; error: unknown };

/**
 * One subscription (name + arguments) as a `Source`. It holds no cursor of its own: every
 * emit carries the server's cursor, the record persists it, and every open resumes from the
 * committed one. `eventId` becomes `Event.id`, so the record's seen ring dedups redelivery.
 * `truncated` becomes the batch's `truncated`, which marks the window as a gap.
 *
 * The subscription runs until `signal` aborts. A terminated or overflowed stream reopens
 * from the last committed cursor; a retryable error backs off through the core's retry
 * ladder; a permanent one rejects, and `attach` rejects with it.
 */
export function mcpEventsSource(client: Client, options: McpEventsOptions): Source {
  const mode = options.mode ?? 'auto';
  const pollMs = options.pollMs ?? 10_000;
  const maxBuffered = options.maxBuffered ?? 1000;
  if (!Number.isInteger(maxBuffered) || maxBuffered < 1) throw new RangeError(`attend/mcp: maxBuffered must be a positive integer, got ${maxBuffered}`);
  const clock = options.clock ?? systemClock();
  const report = options.onError ?? (() => undefined);
  const backoff = DEFAULT_POLICY.retryBackoffMs;
  const base: { name: string; arguments?: JsonObject; maxAgeMs?: number } = { name: options.name };
  if (options.arguments !== undefined) base.arguments = options.arguments;
  if (options.maxAgeMs !== undefined) base.maxAgeMs = options.maxAgeMs;

  return {
    async subscribe(cursor, emit, signal) {
      const position = { cursor };
      const deliver = async (batch: SourceBatch) => {
        const moved = typeof batch.cursor === 'string' && batch.cursor !== position.cursor;
        if (batch.events.length === 0 && !batch.truncated && !moved) return;
        await emit(batch);
        if (typeof batch.cursor === 'string') position.cursor = batch.cursor;
      };

      let delivery = mode === 'auto' ? await listedDelivery(signal) : mode;
      let failures = 0;
      while (delivery !== null && !signal.aborted) {
        const outcome = delivery === 'stream' ? await runStream(position.cursor, deliver, signal) : await runPoll(position, deliver, signal);
        if (signal.aborted) return;
        if (outcome.kind === 'ended') {
          failures = 0;
          await sleep(clock, backoff[0] ?? pollMs, signal);
          continue;
        }
        const code = codeOf(outcome.error);
        const policy = code === null ? 'permanent' : (ERROR_POLICY[code] ?? 'permanent');
        if (policy === 'fallback' && mode === 'auto' && delivery === 'stream') {
          delivery = 'poll';
          continue;
        }
        if (policy === 'retry') {
          const delayMs = backoff[Math.min(failures, backoff.length - 1)] ?? pollMs;
          failures += 1;
          report({ kind: 'retry', message: messageOf(outcome.error), code, delayMs });
          await sleep(clock, delayMs, signal);
          continue;
        }
        throw outcome.error;
      }
    },
  };

  /** `events/list`, paged, until the event is found. Unlisted events poll; null means aborted. */
  async function listedDelivery(signal: AbortSignal): Promise<Delivery | null> {
    let cursor: string | undefined;
    do {
      let page: z.output<typeof ListResult>;
      try {
        page = await client.request({ method: 'events/list', params: cursor === undefined ? {} : { cursor } }, ListResult, { signal });
      } catch (error) {
        if (signal.aborted) return null;
        throw error;
      }
      const listed = page.events.find((e) => e.name === options.name);
      if (listed) return listed.delivery?.includes('push') ? 'stream' : 'poll';
      cursor = page.nextCursor;
    } while (cursor !== undefined);
    return 'poll';
  }

  async function runPoll(position: { cursor: string | null }, deliver: (batch: SourceBatch) => Promise<void>, signal: AbortSignal): Promise<Outcome> {
    while (!signal.aborted) {
      const params: typeof base & { cursor: string | null; maxEvents?: number } = { ...base, cursor: position.cursor };
      if (options.maxEvents !== undefined) params.maxEvents = options.maxEvents;
      let result: z.output<typeof PollResult>;
      try {
        result = await client.request({ method: 'events/poll', params }, PollResult, { signal });
      } catch (error) {
        return signal.aborted ? { kind: 'ended' } : { kind: 'failed', error };
      }
      const events: Event[] = [];
      for (const raw of result.events) {
        const parsed = parseOccurrence(raw);
        if (parsed.ok) events.push(parsed.event);
        else report({ kind: 'malformed', method: 'events/poll', message: parsed.message, payload: raw });
      }
      await deliver(batch(events, result.cursor, result.truncated));
      if (result.hasMore) continue;
      await sleep(clock, result.nextPollMs ?? pollMs, signal);
    }
    return { kind: 'ended' };
  }

  async function runStream(cursor: string | null, deliver: (batch: SourceBatch) => Promise<void>, signal: AbortSignal): Promise<Outcome> {
    const demux = demuxFor(client);
    const controller = new AbortController();
    const items = inbox(maxBuffered, () => controller.abort(new Error('attend: stream inbox overflowed')));
    const subscription: Subscription = { id: null, push: items.push };
    const params = { ...base, cursor };
    const onAbort = () => controller.abort(signal.reason);
    signal.addEventListener('abort', onAbort, { once: true });
    let settled = false;
    demux.pending.set(params, subscription);
    client.request({ method: 'events/stream', params }, StreamResult, { signal: controller.signal, timeout: STREAM_TIMEOUT_MS }).then(
      () => {
        settled = true;
        items.close({ kind: 'done' });
      },
      (error: unknown) => {
        settled = true;
        items.close({ kind: 'failed', error });
      },
    );
    try {
      for (;;) {
        const item = await items.next();
        switch (item.kind) {
          case 'done':
          case 'terminated':
            return { kind: 'ended' };
          case 'failed':
            return signal.aborted ? { kind: 'ended' } : { kind: 'failed', error: item.error };
          case 'overflow':
            report({ kind: 'overflow', buffered: item.buffered });
            return { kind: 'ended' };
          case 'active': {
            const active = parsePayload(Active, item.params, 'notifications/events/active');
            if (active) await deliver(batch([], active.cursor, active.truncated));
            break;
          }
          case 'event': {
            const parsed = parseOccurrence(item.params);
            if (parsed.ok) await deliver(batch([parsed.event], parsed.cursor));
            else {
              report({ kind: 'malformed', method: 'notifications/events/event', message: parsed.message, payload: item.params });
              await deliver(batch([], cursorOf(item.params)));
            }
            break;
          }
          case 'heartbeat': {
            const heartbeat = parsePayload(Heartbeat, item.params, 'notifications/events/heartbeat');
            if (heartbeat) await deliver(batch([], heartbeat.cursor));
            break;
          }
          case 'error': {
            const failure = parsePayload(StreamError, item.params, 'notifications/events/error');
            if (failure) report({ kind: 'stream', code: failure.error.code, message: failure.error.message, data: failure.error.data });
            break;
          }
          default:
            return assertNever(item);
        }
      }
    } finally {
      signal.removeEventListener('abort', onAbort);
      if (subscription.id !== null) demux.live.delete(subscription.id);
      if (!settled) controller.abort(new Error('attend: stream closed'));
    }
  }

  function parsePayload<S extends z.ZodType>(schema: S, payload: unknown, method: string): z.output<S> | null {
    const result = schema.safeParse(payload);
    if (result.success) return result.data;
    report({ kind: 'malformed', method, message: z.prettifyError(result.error), payload });
    return null;
  }
}

// ─── parsing at the boundary ─────────────────────────────────────────────────

/** The draft documents no summary key in `_meta`, so `text` is left unset for the sink to build. */
function parseOccurrence(raw: unknown): { ok: true; event: Event; cursor: string | null | undefined } | { ok: false; message: string } {
  const result = Occurrence.safeParse(raw);
  if (!result.success) return { ok: false, message: z.prettifyError(result.error) };
  const o = result.data;
  return { ok: true, event: { id: o.eventId, at: Date.parse(o.timestamp), name: o.name, data: o.data }, cursor: o.cursor };
}

function cursorOf(payload: unknown): string | undefined {
  return typeof payload === 'object' && payload !== null && 'cursor' in payload && typeof payload.cursor === 'string' ? payload.cursor : undefined;
}

/** A batch whose cursor is set only when the server gave one, so a missing cursor leaves the record's alone. */
function batch(events: Event[], cursor: string | null | undefined, truncated?: boolean): SourceBatch {
  const out: SourceBatch = { events };
  if (typeof cursor === 'string') out.cursor = cursor;
  if (truncated) out.truncated = true;
  return out;
}

function codeOf(error: unknown): number | null {
  return typeof error === 'object' && error !== null && 'code' in error && typeof error.code === 'number' ? error.code : null;
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function assertNever(value: never): never {
  throw new Error(`attend: unexpected ${JSON.stringify(value)}`);
}
