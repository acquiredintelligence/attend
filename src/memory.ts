/**
 * In-memory adapters: the reference implementation of both ports, the browser's store,
 * and what the conformance suites are first run against.
 */
import type { Event, EventLog, LogEntry, ScopeRecord, StateStore } from './types.js';

export function memoryStore(): StateStore {
  const rows = new Map<string, ScopeRecord>();
  return {
    async get(scope) {
      const row = rows.get(scope);
      return row ? structuredClone(row) : null;
    },
    async put(record, expectedVersion) {
      const current = rows.get(record.scope)?.version ?? 0;
      if (current !== expectedVersion) return 'conflict';
      rows.set(record.scope, structuredClone(record));
      return 'ok';
    },
    async due(now, limit) {
      const due: { scope: string; dueAt: number }[] = [];
      for (const [scope, row] of rows) if (row.dueAt !== null && row.dueAt <= now) due.push({ scope, dueAt: row.dueAt });
      due.sort((a, b) => a.dueAt - b.dueAt || (a.scope < b.scope ? -1 : a.scope > b.scope ? 1 : 0));
      return due.slice(0, limit).map((d) => d.scope);
    },
    async remove(scope) {
      rows.delete(scope);
    },
  };
}

export function memoryLog(options?: { retain?: number }): EventLog {
  const retain = options?.retain ?? Number.POSITIVE_INFINITY;
  const partitions = new Map<string, { next: number; entries: LogEntry[]; waiters: Set<() => void> }>();
  const partition = (scope: string) => {
    let p = partitions.get(scope);
    if (!p) {
      p = { next: 1, entries: [], waiters: new Set() };
      partitions.set(scope, p);
    }
    return p;
  };
  const page = (scope: string, after: string | null, limit: number): LogEntry[] => {
    const entries = partition(scope).entries;
    const from = after === null ? 0 : entries.findIndex((e) => e.position > after);
    return from === -1 ? [] : entries.slice(from, from + limit).map((e) => ({ position: e.position, event: structuredClone(e.event) }));
  };
  return {
    async append(scope: string, events: Event[]) {
      const p = partition(scope);
      for (const event of events) {
        p.entries.push({ position: String(p.next).padStart(20, '0'), event: structuredClone(event) });
        p.next += 1;
      }
      if (p.entries.length > retain) p.entries.splice(0, p.entries.length - retain);
      for (const wake of p.waiters) wake();
    },
    async read(scope, after, { limit, blockMs }) {
      const first = page(scope, after, limit);
      if (first.length > 0 || blockMs === undefined || blockMs <= 0) return first;
      const p = partition(scope);
      await new Promise<void>((resolve) => {
        const done = () => {
          p.waiters.delete(done);
          clearTimeout(timer);
          resolve();
        };
        const timer = setTimeout(done, blockMs);
        p.waiters.add(done);
      });
      return page(scope, after, limit);
    },
  };
}
