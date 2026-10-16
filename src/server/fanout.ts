import { encodeCursor } from './cursor.js';
import { matches, occurrenceFor } from './events.js';
import type { DecisionLog, DecisionPage, DecisionRow, Position } from '../store/types.js';
import { SUBSCRIPTION_ID_META, type StreamNotification, type SubscriptionFilter } from './types.js';

interface Subscriber {
  filter: SubscriptionFilter;
  row: (row: DecisionRow) => void;
  change?: () => void;
  error?: (error: unknown) => void;
  gap?: () => void;
}

export interface Fanout {
  subscribe(filter: SubscriptionFilter, onRow: (row: DecisionRow) => void, hooks?: Pick<Subscriber, 'change' | 'error' | 'gap'>): () => void;
  checked(): Position | null;
  /** Resolves once the reader knows where it starts, so a stream can tell which rows it missed. */
  readonly ready: Promise<void>;
  cached(after: Position | null): DecisionPage | null;
  readonly size: number;
  close(): void;
}

const encoded = new WeakMap<StreamNotification, string>();
const shapes = new WeakMap<DecisionRow, Map<string, { json: string; meta: object }>>();

/** Encodes notifications once; event payload bytes are shared across subscribers. */
export function notificationJSON(notification: StreamNotification): string {
  return encoded.get(notification) ?? JSON.stringify({ jsonrpc: '2.0', ...notification });
}

/** Splices a subscription's envelope around the shared row payload. */
export function eventNotification(row: DecisionRow, filter: SubscriptionFilter, maxBytes: number | undefined, epoch: string, id: string | number): Extract<StreamNotification, { method: 'notifications/events/event' }> {
  let variants = shapes.get(row);
  if (!variants) { variants = new Map(); shapes.set(row, variants); }
  const key = `${filter.include}:${maxBytes ?? 1048576}`;
  let shape = variants.get(key);
  if (!shape) {
    const { _meta, ...event } = occurrenceFor(row, filter.include, maxBytes);
    shape = { json: JSON.stringify(event).slice(0, -1), meta: _meta ?? {} };
    variants.set(key, shape);
  }
  const json = `{"jsonrpc":"2.0","method":"notifications/events/event","params":${shape.json},"cursor":${JSON.stringify(encodeCursor({ epoch, position: row.position }))},"_meta":${JSON.stringify({ ...shape.meta, [SUBSCRIPTION_ID_META]: id })}}}`;
  const notification: Extract<StreamNotification, { method: 'notifications/events/event' }> = JSON.parse(json);
  encoded.set(notification, json);
  return notification;
}

/** The process's tail cache and indexed, nonblocking subscription reader. */
export function createFanout(log: DecisionLog): Fanout {
  const subscribers = new Set<Subscriber>();
  const exact = new Map<string, Set<Subscriber>>();
  const all = new Set<Subscriber>();
  const prefixes: { prefix: string; subscribers: Set<Subscriber> }[] = [];
  const ring: DecisionRow[] = [];
  let head = 0;
  let checked: Position | null = null;
  let closed = false;
  let reader: AbortController | undefined;
  let running: Promise<void> | undefined;
  let ready: Promise<void> = Promise.resolve();
  function lowerBound(prefix: string) {
    let low = 0, high = prefixes.length;
    while (low < high) {
      const mid = (low + high) >>> 1;
      if (prefixes[mid]!.prefix < prefix) low = mid + 1;
      else high = mid;
    }
    return low;
  }
  function invoke(callback: (() => void) | undefined) { try { callback?.(); } catch { /* Subscribers cannot interrupt the shared reader. */ } }
  function dispatch(row: DecisionRow) {
    const candidates = new Set(all);
    for (const sub of exact.get(row.scope) ?? []) candidates.add(sub);
    for (let length = 0; length <= row.scope.length; length++) {
      const prefix = row.scope.slice(0, length);
      const entry = prefixes[lowerBound(prefix)];
      if (entry?.prefix === prefix) for (const sub of entry.subscribers) candidates.add(sub);
    }
    for (const sub of candidates) if (matches(sub.filter, row)) invoke(() => sub.row(row));
  }
  async function read(signal: AbortSignal) {
    await ready;
    while (!closed && subscribers.size && !signal.aborted) {
      try {
        const before = checked;
        const page = await log.read({ after: before, limit: 256 });
        if (signal.aborted) break;
        if (before !== null && page.checked !== before) {
          const floor = (await log.bounds()).first;
          if (signal.aborted) break;
          if (floor !== null && log.compare(before, floor) < 0) {
            ring.length = 0; head = 0;
            for (const sub of subscribers) invoke(sub.gap);
          }
        }
        for (const row of page.rows) {
          if (ring.length < 4096) ring.push(row);
          else { ring[head] = row; head = (head + 1) % 4096; }
          dispatch(row);
        }
        checked = page.checked ?? checked;
        for (const sub of subscribers) invoke(sub.change);
        if (!page.more) await log.wait(checked, 30_000, signal);
      } catch (error) {
        for (const sub of subscribers) invoke(() => sub.error?.(error));
        await new Promise<void>((resolve) => {
          const done = () => { clearTimeout(timer); signal.removeEventListener('abort', done); resolve(); };
          const timer = setTimeout(done, 100);
          signal.addEventListener('abort', done, { once: true });
          if (signal.aborted) done();
        });
      }
    }
  }
  function start() {
    if (running || closed || !subscribers.size) return;
    reader = new AbortController();
    if (checked === null) ready = log.bounds().then((bounds) => { checked ??= bounds.last; }, () => undefined);
    running = read(reader.signal).finally(() => { running = undefined; start(); });
  }
  return {
    subscribe(filter, row, hooks = {}) {
      if (closed) return () => undefined;
      const sub: Subscriber = { filter, row, ...hooks };
      subscribers.add(sub);
      let entries: Set<Subscriber>;
      let prefix: string | undefined;
      if (filter.scope !== undefined) {
        entries = exact.get(filter.scope) ?? new Set(); exact.set(filter.scope, entries);
      } else if (filter.scopePrefix !== undefined) {
        prefix = filter.scopePrefix;
        const index = lowerBound(prefix);
        if (prefixes[index]?.prefix !== prefix) prefixes.splice(index, 0, { prefix, subscribers: new Set() });
        entries = prefixes[index]!.subscribers;
      } else entries = all;
      entries.add(sub);
      start();
      return () => {
        if (!subscribers.delete(sub)) return;
        entries.delete(sub);
        if (!entries.size && filter.scope !== undefined) exact.delete(filter.scope);
        if (!entries.size && prefix !== undefined) prefixes.splice(lowerBound(prefix), 1);
        if (!subscribers.size) reader?.abort();
      };
    },
    checked: () => checked,
    get ready() { return ready; },
    cached(after) {
      if (after === null || !ring.length || checked === null) return null;
      const first = ring[head];
      if (!first || log.compare(after, first.position) < 0 || log.compare(after, checked) > 0) return null;
      const rows: DecisionRow[] = [];
      for (let i = 0; i < ring.length; i++) {
        const row = ring[(head + i) % ring.length]!;
        if (log.compare(row.position, after) > 0) rows.push(row);
        if (rows.length === 256) return { rows, checked: row.position, more: log.compare(row.position, checked) < 0 };
      }
      return { rows, checked, more: false };
    },
    get size() { return subscribers.size; },
    close() { closed = true; reader?.abort(); subscribers.clear(); exact.clear(); all.clear(); prefixes.length = 0; ring.length = 0; },
  };
}
