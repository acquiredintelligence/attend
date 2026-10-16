import { setImmediate as yieldTurn } from 'node:timers/promises';
import { encodeCursor, lostBefore, resolveStart } from './cursor.js';
import { eventsError, matches, parseSubscription, queryFor } from './events.js';
import { eventNotification, notificationJSON } from './fanout.js';
import type { DecisionPage, Position } from '../store/types.js';
import { SUBSCRIPTION_ID_META, type Principal, type ServerContext, type StreamEventsResult, type StreamNotification, type StreamParams } from './types.js';

interface Outbox { principal: string; bytes: number; writeBytes: number; pageRows: number }
const streams = new WeakMap<ServerContext, Set<Outbox>>();

/** Owned queue and in-flight work, for checking stream memory bounds. */
export function streamUsage(context: ServerContext): { count: number; queuedBytes: number; writeBytes: number; pageRows: number } {
  const active = streams.get(context);
  return { count: active?.size ?? 0, queuedBytes: [...active ?? []].reduce((sum, stream) => sum + stream.bytes, 0), writeBytes: [...active ?? []].reduce((sum, stream) => sum + stream.writeBytes, 0), pageRows: [...active ?? []].reduce((sum, stream) => sum + stream.pageRows, 0) };
}

function abortable<T>(promise: Promise<T>, signal: AbortSignal): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const abort = () => reject(new DOMException('Stream aborted', 'AbortError'));
    signal.addEventListener('abort', abort, { once: true });
    promise.then(resolve, reject).finally(() => signal.removeEventListener('abort', abort));
    if (signal.aborted) abort();
  });
}

/** Sends a subscription in order, replaying the log whenever its live outbox fills. */
export async function streamEvents(context: ServerContext, params: StreamParams, principal: Principal, subscriptionId: string | number, send: (notification: StreamNotification) => Promise<void>, signal: AbortSignal): Promise<StreamEventsResult> {
  const { filter, cursor, maxAgeMs } = parseSubscription(params, principal);
  const max = context.options.maxStreamsPerPrincipal ?? 256;
  const active = streams.get(context) ?? new Set<Outbox>();
  streams.set(context, active);
  if ([...active].filter((stream) => stream.principal === principal.id).length >= max) throw eventsError('ResourceExhausted', 'Stream quota exceeded', { limit: 'streams', max });
  const outbox: Outbox = { principal: principal.id, bytes: 0, writeBytes: 0, pageRows: 0 };
  active.add(outbox);
  const combined = AbortSignal.any([signal, context.signal]);
  const _meta = { [SUBSCRIPTION_ID_META]: subscriptionId };
  const complete: StreamEventsResult = { resultType: 'complete', _meta };
  let unsubscribe: () => void = () => undefined;
  let wake: () => void = () => undefined;
  try {
    const now = Date.now();
    const epoch = await abortable(context.epoch(), combined);
    const start = await abortable(resolveStart(context.log, epoch, cursor), combined);
    let checked = start.after;
    const sinceAt = maxAgeMs !== undefined && !start.fromNow ? now - maxAgeMs : undefined;
    let agePending = sinceAt;
    let truncated = start.truncated;
    let behind = !start.fromNow;
    let pendingError: { reason: unknown } | undefined;
    let lastFrame = Date.now();
    let sent = 0;
    const queue: { position: Position; notification: StreamNotification; bytes: number }[] = [];
    const budget = context.options.streamBufferBytes ?? 1_048_576;
    const heartbeatMs = Math.max(1, Math.min(context.options.heartbeatMs ?? 15_000, 30_000));
    const fanout = context.fanout;
    const clear = () => { queue.length = 0; outbox.bytes = 0; };
    const later = (position: Position | null, than: Position | null) => position !== null && (than === null || context.log.compare(position, than) > 0);
    const positionCursor = () => encodeCursor({ epoch, position: checked ?? '' });
    async function write(notification: StreamNotification) {
      combined.throwIfAborted();
      outbox.writeBytes = Buffer.byteLength(notificationJSON(notification)) + 8;
      try { await abortable(send(notification), combined); } finally { outbox.writeBytes = 0; }
      lastFrame = Date.now();
      if (++sent % 64 === 0) await abortable(yieldTurn(), combined);
    }
    async function pause(ms: number) {
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        await abortable(new Promise<void>((resolve) => { wake = resolve; timer = setTimeout(resolve, ms); }), combined);
      } finally { clearTimeout(timer); wake = () => undefined; }
    }
    async function reading<T>(promise: Promise<T>): Promise<T> {
      let settled: { value: T } | { error: unknown } | undefined;
      promise.then((value) => { settled = { value }; wake(); }, (error: unknown) => { settled = { error }; wake(); });
      const outcome = () => settled;
      let result = outcome();
      while (result === undefined) {
        await pause(Math.max(1, heartbeatMs - (Date.now() - lastFrame)));
        result = outcome();
        if (result === undefined && Date.now() - lastFrame >= heartbeatMs) await write({ method: 'notifications/events/heartbeat', params: { cursor: positionCursor(), _meta } });
      }
      if ('error' in result) throw result.error;
      return result.value;
    }
    unsubscribe = fanout.subscribe(filter, (row) => {
      if (behind || !later(row.position, checked) || combined.aborted) return;
      const notification = eventNotification(row, filter, context.options.payload?.maxEventsBytes, epoch, subscriptionId);
      const bytes = Buffer.byteLength(notificationJSON(notification)) + 8;
      if (outbox.bytes + bytes > budget) { clear(); behind = true; }
      else { queue.push({ position: row.position, notification, bytes }); outbox.bytes += bytes; }
      wake();
    }, {
      change: () => wake(),
      gap: () => { clear(); behind = true; wake(); },
      error: (error) => { pendingError = { reason: error }; behind = true; clear(); wake(); },
    });
    async function read(): Promise<DecisionPage> {
      const cached = sinceAt === undefined ? fanout.cached(checked) : null;
      return cached ?? context.log.read({ after: checked, ...queryFor(filter), ...(sinceAt === undefined ? {} : { sinceAt }), limit: 256, scanLimit: 1024 });
    }
    await abortable(fanout.ready, combined);
    if (later(fanout.checked(), checked)) behind = true;
    let page: DecisionPage | undefined;
    if (behind) {
      try {
        page = await abortable(read(), combined);
        if (await abortable(lostBefore(context.log, checked, page, agePending), combined)) { checked = null; truncated = true; page = undefined; }
        agePending = undefined;
      } catch (error) { if (combined.aborted) throw error; pendingError = { reason: error }; }
    }
    await write({ method: 'notifications/events/active', params: { cursor: positionCursor(), truncated, _meta } });
    while (!combined.aborted) {
      if (!context.descriptors.some((descriptor) => descriptor.name === filter.name)) {
        await write({ method: 'notifications/events/terminated', params: { error: eventsError('NotFound', 'Event type removed', { kind: 'event' }).toJSON(), _meta } });
        throw new DOMException('Event type removed', 'AbortError');
      }
      if (pendingError !== undefined) {
        const error = pendingError.reason; pendingError = undefined;
        await write({ method: 'notifications/events/error', params: { error: eventsError('Transient', 'Store temporarily unavailable', { reason: error instanceof Error ? error.message : String(error) }).toJSON(), _meta } });
        await pause(Math.min(100, heartbeatMs));
      }
      if (behind) {
        let lost = false;
        try {
          page ??= await reading(read());
          lost = await reading(lostBefore(context.log, checked, page, agePending));
          agePending = undefined;
        } catch (error) { if (combined.aborted) throw error; pendingError = { reason: error }; continue; }
        if (lost) {
          checked = null; page = undefined;
          await write({ method: 'notifications/events/active', params: { cursor: positionCursor(), truncated: true, _meta } });
          continue;
        }
        outbox.pageRows = page.rows.length;
        for (const row of page.rows) {
          if (later(row.position, checked) && matches(filter, row) && (sinceAt === undefined || row.at >= sinceAt)) {
            await write(eventNotification(row, filter, context.options.payload?.maxEventsBytes, epoch, subscriptionId));
            checked = row.position;
          }
        }
        if (later(page.checked, checked)) checked = page.checked;
        const more = page.more;
        page = undefined; outbox.pageRows = 0;
        if (Date.now() - lastFrame >= heartbeatMs) await write({ method: 'notifications/events/heartbeat', params: { cursor: positionCursor(), _meta } });
        if (more || later(fanout.checked(), checked)) continue;
        behind = false;
        continue;
      }
      const next = queue.shift();
      if (next) {
        outbox.bytes -= next.bytes;
        if (later(next.position, checked)) { await write(next.notification); checked = next.position; }
        continue;
      }
      if (later(fanout.checked(), checked)) checked = fanout.checked();
      if (Date.now() - lastFrame >= heartbeatMs) await write({ method: 'notifications/events/heartbeat', params: { cursor: positionCursor(), _meta } });
      await pause(Math.max(1, heartbeatMs - (Date.now() - lastFrame)));
    }
    if (signal.aborted) throw new DOMException('Stream aborted', 'AbortError');
    return complete;
  } catch (error) {
    if (context.signal.aborted && !signal.aborted) return complete;
    throw error;
  } finally { unsubscribe(); wake(); active.delete(outbox); }
}
