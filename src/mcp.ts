/**
 * `attend/mcp`: an MCP Events subscription (`events/stream` or `events/poll`) as a
 * `Source`. Peer dependency: `@modelcontextprotocol/sdk`. The draft's wire shapes stay
 * inside this module; the public surface takes an SDK `Client` and emits domain events.
 *
 * The source installs one notification handler on the Client for each of the five
 * `notifications/events/*` methods (`active`, `event`, `heartbeat`, `error`, `terminated`)
 * the first time it is used on that Client, and routes by subscription id from there. The
 * app must not replace those handlers.
 *
 * When the Client's transport closes, the SDK fails every open request with
 * `ConnectionClosed` (-32000). The source turns that into a `PermanentError`, so `attach`
 * rejects: the app must reconnect the Client (or make a new one) and attach again. A source
 * waiting between polls or before a reopen is woken by the close and rejects the same way.
 */
import type { Client } from '@modelcontextprotocol/sdk/client/index.js';
import type { Transport } from '@modelcontextprotocol/sdk/shared/transport.js';
import type { JSONRPCMessage } from '@modelcontextprotocol/sdk/types.js';
import { ErrorCode } from '@modelcontextprotocol/sdk/types.js';
import { z } from 'zod/v4';
import { PermanentError } from './errors.js';
import { sleep, systemClock } from './internal/clock.js';
import { stableJson } from './internal/json.js';
import { assertNever, messageOf } from './internal/shared.js';
import type { Clock, Event, JsonObject, Source, SourceBatch } from './types.js';

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
   * How many stream notifications may wait while `emit` persists earlier ones. Past that, the
   * source stops the stream, persists everything it holds, reports `overflow`, and reopens from
   * the committed cursor, so the server sends the rest again. Notifications that arrive after
   * the stop are lost only for an event type the server cannot replay from a cursor. Default 1000.
   */
  maxBuffered?: number;
  /**
   * How long a stream may go without any notification, heartbeats included, before the source
   * treats it as dead: it reports `idle`, aborts the stream and reopens from the committed
   * cursor. Default 300000.
   */
  idleTimeoutMs?: number;
  clock?: Clock;
  /** Everything the source survives: a server error notification, a skipped payload, an overflow, an idle stream, a stray notification. */
  onError?: (error: McpEventsError) => void;
}

export type McpEventsError =
  /** The server's `notifications/events/error`; the stream stays open. */
  | { kind: 'stream'; code: number; message: string; data: unknown }
  /** A payload that failed validation and was skipped, with its cursor kept. */
  | { kind: 'malformed'; method: string; message: string; payload: unknown }
  /**
   * `buffered` notifications were waiting on `emit`; they are persisted, then the stream reopens
   * from the committed cursor after `reopenMs`: at once when the overflowed stream moved that
   * cursor, else after the reopen pause, since a server that cannot resume from a cursor would
   * send the same backlog again.
   */
  | { kind: 'overflow'; buffered: number; reopenMs: number }
  /** Nothing arrived for `idleTimeoutMs`; the stream reopens from the committed cursor. */
  | { kind: 'idle' }
  /** A notification on this Client named a subscription that no live stream here opened. */
  | { kind: 'unrouted'; method: string; subscriptionId: string | null };

type Delivery = 'stream' | 'poll';
type Policy = 'permanent' | 'retry' | 'fallback' | 'closed';

/** What a request error means to the subscription. Codes not listed are permanent. */
const ERROR_POLICY: Readonly<Record<number, Policy>> = {
  [-32023]: 'permanent',
  [-32024]: 'permanent',
  [-32025]: 'retry',
  [-32026]: 'fallback',
  [-32027]: 'permanent',
  [ErrorCode.RequestTimeout]: 'retry',
  [ErrorCode.ConnectionClosed]: 'closed',
};

/** The longest delay `setTimeout` honours; past it the SDK's timeout fires at once. */
const STREAM_TIMEOUT_MS = 2 ** 31 - 1;
/**
 * Pause before reopening a stream the server ended or the source found idle, so a server that
 * ends every stream at once never spins. An overflow that moved the committed cursor reopens at
 * once: the source closed that stream itself, and the backlog behind it is real. One that did
 * not pauses too, or a server that sends no cursors would flood the reopened stream with the
 * same backlog at once.
 */
const REOPEN_MS = 1000;
/** Events already waiting in the inbox fold into one `emit`, so a slow store costs one write per batch rather than per event. */
const EMIT_BATCH = 500;
/** Closed subscription ids remembered, so a straggling notification from a stream this Client just closed is dropped rather than reported. */
const CLOSED_REMEMBERED = 256;
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

type Note = { [K in StreamMethod]: { kind: K; params: unknown } }[StreamMethod];
type Item = Note | { kind: 'done' } | { kind: 'failed'; error: unknown } | { kind: 'overflow'; buffered: number } | { kind: 'idle' };

interface Subscription {
  id: string | null;
  push(note: Note): void;
  report(error: McpEventsError): void;
}

interface Demux {
  /** Streams whose request id is known, by id. */
  live: Map<string, Subscription>;
  /** Streams whose request has not been sent yet, by the params object the request will carry. */
  pending: WeakMap<object, Subscription>;
  /** Ids of streams closed here lately, oldest first. */
  closed: Set<string>;
}

const demuxes = new WeakMap<Client, Demux>();
const wrappedTransports = new WeakSet<Transport>();
const closings = new WeakMap<Transport, AbortSignal>();

/**
 * Aborts when the connection `transport` carries right now closes. The SDK sets
 * `transport.onclose` at connect and clears `client.transport` inside it, so a wrapper
 * installed after connect fires once the Client already reads as closed. A signal that has
 * fired belongs to a connection that is gone; a live transport that still maps to one has been
 * reconnected since, and gets a fresh signal around the new connection's `onclose`.
 */
function closingOf(transport: Transport): AbortSignal {
  let signal = closings.get(transport);
  if (!signal || signal.aborted) {
    const controller = new AbortController();
    const onclose = transport.onclose;
    transport.onclose = () => {
      onclose?.();
      controller.abort();
    };
    signal = controller.signal;
    closings.set(transport, signal);
  }
  return signal;
}

/**
 * The SDK never returns a request's JSON-RPC id, so the demux wraps `transport.send` and
 * reads the id off each outgoing `events/stream` as it leaves. `request()` sends inside its
 * Promise executor with the caller's `params` object by reference, so identity pairs the
 * outgoing message with its subscription even when several streams open at once.
 */
function demuxFor(client: Client): Demux {
  let demux = demuxes.get(client);
  if (!demux) {
    demux = { live: new Map(), pending: new WeakMap(), closed: new Set() };
    demuxes.set(client, demux);
    for (const kind of STREAM_METHODS) {
      const d = demux;
      const method = `notifications/events/${kind}` as const;
      client.setNotificationHandler(envelope(method), (notification) => {
        const raw = notification.params?._meta?.[SUBSCRIPTION_KEY];
        const id = raw === undefined ? null : String(raw);
        const subscription = id === null ? undefined : d.live.get(id);
        if (subscription) subscription.push({ kind, params: notification.params });
        else if (id === null || !d.closed.has(id)) for (const live of d.live.values()) live.report({ kind: 'unrouted', method, subscriptionId: id });
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

function forget(demux: Demux, id: string): void {
  demux.live.delete(id);
  demux.closed.add(id);
  if (demux.closed.size <= CLOSED_REMEMBERED) return;
  const oldest = demux.closed.values().next().value;
  if (oldest !== undefined) demux.closed.delete(oldest);
}

/**
 * The queue between the SDK's notification handlers and the stream loop, which takes the next
 * run of items only once `emit` resolves. A notification past `max` closes the intake: it and
 * every later one are dropped, `onOverflow` runs, and `overflow` queues behind what is held, so
 * the loop persists all of it before it sees the marker. The request's outcome always queues.
 */
function inbox(max: number, onOverflow: () => void): { push(note: Note): void; close(item: Item): void; take(limit: number): Promise<Item[]> } {
  const items: Item[] = [];
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
      enqueue({ kind: 'overflow', buffered: items.length });
      onOverflow();
    },
    close: enqueue,
    async take(limit) {
      while (items.length === 0) await new Promise<void>((resolve) => (wake = resolve));
      return items.splice(0, limit);
    },
  };
}

// ─── the source ──────────────────────────────────────────────────────────────

type Outcome = { kind: 'ended' } | { kind: 'overflowed' } | { kind: 'failed'; error: unknown };

/**
 * One subscription (name + arguments) as a `Source`, keyed `mcp:<name>:<arguments as sorted
 * JSON>`. It holds no cursor of its own: every emit carries the server's cursor, the record
 * persists it, and every open resumes from the committed one. `eventId` becomes `Event.id`, so
 * the record's seen ring dedups redelivery. `truncated` becomes the batch's `truncated`, which
 * marks the window as a gap.
 *
 * The subscription runs until `signal` aborts. A terminated or idle stream reopens from the last
 * committed cursor after a short pause; an overflowed one at once, when it moved that cursor. A request error that the server says to
 * retry (-32025, a request timeout) is thrown as a plain Error, so `attach` backs off through the
 * app's `retryBackoffMs` and resubscribes; any other error, and a closed Client, is thrown as a
 * `PermanentError` with the SDK error as its `cause`, and `attach` rejects.
 */
export function mcpEventsSource(client: Client, options: McpEventsOptions): Source {
  const mode = options.mode ?? 'auto';
  const pollMs = options.pollMs ?? 10_000;
  const maxBuffered = options.maxBuffered ?? 1000;
  if (!Number.isInteger(maxBuffered) || maxBuffered < 1) throw new RangeError(`attend/mcp: maxBuffered must be a positive integer, got ${maxBuffered}`);
  const idleTimeoutMs = options.idleTimeoutMs ?? 300_000;
  const clock = options.clock ?? systemClock();
  const report = options.onError ?? (() => undefined);
  const base: { name: string; arguments?: JsonObject; maxAgeMs?: number } = { name: options.name };
  if (options.arguments !== undefined) base.arguments = options.arguments;
  if (options.maxAgeMs !== undefined) base.maxAgeMs = options.maxAgeMs;

  return {
    key: `mcp:${options.name}:${stableJson(options.arguments ?? {})}`,
    async subscribe(cursor, emit, signal) {
      const position = { cursor };
      const deliver = async (batch: SourceBatch) => {
        const moved = typeof batch.cursor === 'string' && batch.cursor !== position.cursor;
        if (batch.events.length === 0 && !batch.truncated && !moved) return;
        await emit(batch);
        if (typeof batch.cursor === 'string') position.cursor = batch.cursor;
      };

      let delivery = mode === 'auto' ? await listedDelivery(signal) : mode;
      while (delivery !== null && !signal.aborted) {
        const outcome = delivery === 'stream' ? await runStream(position, deliver, signal) : await runPoll(position, deliver, signal);
        if (signal.aborted) return;
        if (outcome.kind === 'overflowed') continue;
        if (outcome.kind === 'ended') {
          await pause(REOPEN_MS, signal);
          continue;
        }
        if (policyOf(outcome.error) === 'fallback' && mode === 'auto' && delivery === 'stream') {
          delivery = 'poll';
          continue;
        }
        throw classify(outcome.error);
      }
    },
  };

  /** `events/list`, paged, until the event is found. Unlisted events poll; null means aborted. */
  async function listedDelivery(signal: AbortSignal): Promise<Delivery | null> {
    let cursor: string | undefined;
    do {
      if (!client.transport) throw closedClient(undefined);
      let page: z.output<typeof ListResult>;
      try {
        page = await client.request({ method: 'events/list', params: cursor === undefined ? {} : { cursor } }, ListResult, { signal });
      } catch (error) {
        if (signal.aborted) return null;
        throw classify(error);
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
      if (!client.transport) throw closedClient(undefined);
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
      await pause(result.nextPollMs ?? pollMs, signal);
    }
    return { kind: 'ended' };
  }

  async function runStream(position: { cursor: string | null }, deliver: (batch: SourceBatch) => Promise<void>, signal: AbortSignal): Promise<Outcome> {
    if (!client.transport) throw closedClient(undefined);
    const opened = position.cursor;
    const demux = demuxFor(client);
    const controller = new AbortController();
    const items = inbox(maxBuffered, () => controller.abort(new Error('attend: stream inbox overflowed')));
    let cancelIdle: () => void = () => undefined;
    const armIdle = () => {
      cancelIdle();
      cancelIdle = clock.at(clock.now() + idleTimeoutMs, () => items.close({ kind: 'idle' }));
    };
    const subscription: Subscription = {
      id: null,
      push(note) {
        armIdle();
        items.push(note);
      },
      report,
    };
    const params = { ...base, cursor: opened };
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
      if (subscription.id === null) {
        throw new PermanentError(
          'attend/mcp: the events/stream request left the Client without the demux seeing its id, so its notifications cannot be routed. Please report this at https://github.com/acquiredintelligence/attend/issues',
        );
      }
      armIdle();
      // Consecutive events fold into one batch, which carries the last cursor among them: the
      // same position a one-by-one emit would have left, in one store write.
      let folded: Event[] = [];
      let foldedCursor: string | undefined;
      const flush = async () => {
        if (signal.aborted || (folded.length === 0 && foldedCursor === undefined)) return;
        const out = batch(folded, foldedCursor);
        folded = [];
        foldedCursor = undefined;
        await deliver(out);
      };
      while (!signal.aborted) {
        for (const item of await items.take(EMIT_BATCH)) {
          if (signal.aborted) return { kind: 'ended' };
          if (item.kind === 'event') {
            const parsed = parseOccurrence(item.params);
            if (parsed.ok) folded.push(parsed.event);
            else report({ kind: 'malformed', method: 'notifications/events/event', message: parsed.message, payload: item.params });
            const cursor = parsed.ok ? parsed.cursor : cursorOf(item.params);
            if (typeof cursor === 'string') foldedCursor = cursor;
            continue;
          }
          await flush();
          if (signal.aborted) return { kind: 'ended' };
          switch (item.kind) {
            case 'done':
            case 'terminated':
              return { kind: 'ended' };
            case 'failed':
              return signal.aborted ? { kind: 'ended' } : { kind: 'failed', error: item.error };
            case 'overflow': {
              const moved = position.cursor !== opened;
              report({ kind: 'overflow', buffered: item.buffered, reopenMs: moved ? 0 : REOPEN_MS });
              return moved ? { kind: 'overflowed' } : { kind: 'ended' };
            }
            case 'idle':
              report({ kind: 'idle' });
              return { kind: 'ended' };
            case 'active': {
              const active = parsePayload(Active, item.params, 'notifications/events/active');
              if (active) await deliver(batch([], active.cursor, active.truncated));
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
        await flush();
      }
      return { kind: 'ended' };
    } finally {
      cancelIdle();
      signal.removeEventListener('abort', onAbort);
      demux.pending.delete(params);
      if (subscription.id !== null) forget(demux, subscription.id);
      if (!settled) controller.abort(new Error('attend: stream closed'));
    }
  }

  /** Sleep `ms` on the clock, or return at once when `signal` aborts or the Client's transport closes. */
  function pause(ms: number, signal: AbortSignal): Promise<void> {
    const transport = client.transport;
    return transport ? sleep(clock, ms, AbortSignal.any([signal, closingOf(transport)])) : Promise.resolve();
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

function policyOf(error: unknown): Policy {
  const code = typeof error === 'object' && error !== null && 'code' in error && typeof error.code === 'number' ? error.code : null;
  return code === null ? 'permanent' : (ERROR_POLICY[code] ?? 'permanent');
}

/** A retryable error passes through as it is, for `attach` to back off on; anything else ends the subscription for good. */
function classify(error: unknown): Error {
  switch (policyOf(error)) {
    case 'retry':
      return error instanceof Error ? error : new Error(messageOf(error));
    case 'closed':
      return closedClient(error);
    default:
      return new PermanentError(`attend/mcp: ${messageOf(error)}`, { cause: error });
  }
}

/** The SDK fails an in-flight request with -32000 on close, but a later one with a bare, code-less "Not connected"; both get this. */
function closedClient(cause: unknown): PermanentError {
  return new PermanentError('attend/mcp: the Client closed; reconnect it and attach again', cause === undefined ? undefined : { cause });
}
