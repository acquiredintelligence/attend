import type { LoggedInput } from '../core/types.js';
import type { CommitOutcome, DecisionRow, DueKey, ScopeStore, Snapshot } from './types.js';

export interface MemoryStoreOptions {
  decisions?: { maxRows?: number; maxAgeMs?: number };
}

function lowerBound(values: readonly number[], target: number, start = 0): number {
  let end = values.length;
  while (start < end) {
    const middle = Math.floor((start + end) / 2);
    if (values[middle]! < target) start = middle + 1;
    else end = middle;
  }
  return start;
}

function sequence(position: string): number {
  const value = Number(position);
  if (!Number.isSafeInteger(value) || value < 0 || String(value) !== position) {
    throw new TypeError(`attend: invalid position "${position}"`);
  }
  return value;
}

/** Snapshots and inputs held by reference, a due index bucketed by second, a bounded decision ring and a random epoch. */
export function memoryStore(options?: MemoryStoreOptions): ScopeStore {
  const scopes = new Map<string, { version: number; snapshot: Snapshot | null; tail: LoggedInput[]; due: number | null }>();
  const buckets = new Map<number, Set<string>>();
  const keys: number[] = [];
  const epoch = crypto.randomUUID();
  let ring: (DecisionRow | undefined)[] = [];
  let head = 0;
  let last = 0;
  const byScope = new Map<string, { seqs: number[]; head: number }>();
  const byKind = { decision: new Map<string, DecisionRow>(), deadLetter: new Map<string, DecisionRow>() };
  const waiters = new Set<() => void>();

  function removeDue(scope: string, due: number | null): void {
    if (due === null) return;
    const key = Math.floor(due / 1000);
    const bucket = buckets.get(key)!;
    bucket.delete(scope);
    if (bucket.size === 0) {
      buckets.delete(key);
      keys.splice(lowerBound(keys, key), 1);
    }
  }

  function dropOldest(): void {
    const row = ring[head]!;
    ring[head++] = undefined;
    byKind[row.kind].delete(row.id);
    const scoped = byScope.get(row.scope)!;
    scoped.head += 1;
    if (scoped.head === scoped.seqs.length) byScope.delete(row.scope);
    else if (scoped.head >= 16 && scoped.head * 2 >= scoped.seqs.length) {
      scoped.seqs = scoped.seqs.slice(scoped.head);
      scoped.head = 0;
    }
    if (head >= 1024 && head * 2 >= ring.length) {
      ring = ring.slice(head);
      head = 0;
    }
  }

  function retain(maxRows?: number, age?: number): void {
    if (maxRows !== undefined) {
      while (ring.length - head > Math.max(maxRows, 1)) dropOldest();
    }
    if (age !== undefined) {
      const cutoff = Date.now() - age;
      while (ring.length - head > 1 && ring[head]!.at < cutoff) dropOldest();
    }
  }

  return {
    durable: false,
    async load(scope) {
      const stored = scopes.get(scope);
      return stored ? { version: stored.version, snapshot: stored.snapshot, tail: stored.tail.slice() } : null;
    },
    async commit(writes) {
      if (writes.length > 1) {
        const seen = new Set<string>();
        for (const write of writes) {
          if (seen.has(write.scope)) throw new TypeError(`attend: a commit names scope "${write.scope}" twice`);
          seen.add(write.scope);
        }
      }
      const outcomes: CommitOutcome[] = [];
      const before = last;
      for (const write of writes) {
        let stored = scopes.get(write.scope);
        if ((stored?.version ?? 0) !== write.expectedVersion) {
          outcomes.push('conflict');
          continue;
        }
        if (!stored) {
          stored = { version: 0, snapshot: null, tail: [], due: null };
          scopes.set(write.scope, stored);
        }
        if (write.snapshot) {
          stored.snapshot = write.snapshot;
          stored.tail = [];
        } else {
          for (const input of write.inputs) stored.tail.push(input);
        }
        stored.version = write.expectedVersion + write.inputs.length;
        if (write.due !== undefined) {
          removeDue(write.scope, stored.due);
          stored.due = write.due;
          if (write.due !== null) {
            const key = Math.floor(write.due / 1000);
            let bucket = buckets.get(key);
            if (!bucket) {
              bucket = new Set();
              buckets.set(key, bucket);
              keys.splice(lowerBound(keys, key), 0, key);
            }
            bucket.add(write.scope);
          }
        }
        for (const append of write.appends) {
          if (byKind[append.kind].has(append.id)) continue;
          const seq = ++last;
          const row = { ...append, position: String(seq) };
          ring.push(row);
          byKind[row.kind].set(row.id, row);
          let scoped = byScope.get(row.scope);
          if (!scoped) {
            scoped = { seqs: [], head: 0 };
            byScope.set(row.scope, scoped);
          }
          scoped.seqs.push(seq);
        }
        outcomes.push('ok');
      }
      if (last > before) {
        retain(options?.decisions?.maxRows ?? 10_000, options?.decisions?.maxAgeMs);
        for (const wake of waiters) wake();
      }
      return outcomes;
    },
    async due(now, limit, after) {
      const result: DueKey[] = [];
      if (limit <= 0) return result;
      const end = Math.floor(now / 1000);
      const start = after ? lowerBound(keys, Math.floor(after.due / 1000)) : 0;
      for (let i = start; i < keys.length && keys[i]! <= end; i += 1) {
        const matches: DueKey[] = [];
        for (const scope of buckets.get(keys[i]!)!) {
          const due = scopes.get(scope)!.due!;
          if (due <= now && (!after || due > after.due || (due === after.due && scope > after.scope))) {
            matches.push({ due, scope });
          }
        }
        matches.sort((a, b) => a.due - b.due || (a.scope < b.scope ? -1 : a.scope > b.scope ? 1 : 0));
        for (const match of matches) {
          result.push(match);
          if (result.length >= limit) return result;
        }
      }
      return result;
    },
    async remove(scope) {
      const stored = scopes.get(scope);
      if (stored) removeDue(scope, stored.due);
      scopes.delete(scope);
    },
    log: {
      async epoch() { return epoch; },
      async bounds() { return { first: ring[head]?.position ?? null, last: last === 0 ? null : String(last) }; },
      async read(query) {
        const after = query.after === null ? 0 : sequence(query.after);
        const first = last - (ring.length - head) + 1;
        const scoped = query.scope === undefined ? null : byScope.get(query.scope) ?? { seqs: [], head: 0 };
        let index = scoped ? lowerBound(scoped.seqs, after + 1, scoped.head) : head + Math.max(0, after + 1 - first);
        const end = scoped ? scoped.seqs.length : ring.length;
        const rows: DecisionRow[] = [];
        const scanLimit = query.scanLimit ?? 4 * query.limit;
        let examined = 0;
        let checked = query.after;
        for (; index < end; index += 1) {
          if (rows.length >= query.limit || examined >= scanLimit) return { rows, checked, more: true };
          const row = ring[scoped ? head + (scoped.seqs[index]! - first) : index]!;
          examined += 1;
          checked = row.position;
          if (query.scopePrefix !== undefined && !row.scope.startsWith(query.scopePrefix)) continue;
          if (query.kinds && !query.kinds.includes(row.kind)) continue;
          if (query.routes && (row.route === null || !query.routes.includes(row.route))) continue;
          if (query.sinceAt !== undefined && row.at < query.sinceAt) continue;
          rows.push(row);
        }
        return { rows, checked: last === 0 && query.after === null ? null : String(Math.max(last, after)), more: false };
      },
      async find(kind, id) { return byKind[kind].get(id) ?? null; },
      async wait(after, blockMs, signal) {
        const seq = after === null ? 0 : sequence(after);
        if (signal.aborted || last > seq) return;
        return new Promise<void>((resolve) => {
          const finish = () => {
            clearTimeout(timer);
            signal.removeEventListener('abort', finish);
            waiters.delete(wake);
            resolve();
          };
          const wake = () => { if (last > seq) finish(); };
          const timer = setTimeout(finish, blockMs);
          signal.addEventListener('abort', finish, { once: true });
          waiters.add(wake);
        });
      },
      async trim(retention) { retain(retention.maxRows, retention.olderThanMs); },
      compare(a, b) { return sequence(a) - sequence(b); },
    },
  };
}

function deepFreeze<T>(value: T): T {
  if (typeof value !== 'object' || value === null) return value;
  Object.freeze(value);
  for (const inner of Object.values(value)) deepFreeze(inner);
  return value;
}

/** For tests: deep-freezes every snapshot, logged input and decision append at commit, and everything a read returns, so code that mutates committed or returned data throws. */
export function freezingStore(inner: ScopeStore): ScopeStore {
  return {
    durable: inner.durable,
    log: {
      epoch: () => inner.log.epoch(),
      bounds: () => inner.log.bounds().then(deepFreeze),
      read: (query) => inner.log.read(query).then(deepFreeze),
      find: (kind, id) => inner.log.find(kind, id).then(deepFreeze),
      wait: (after, blockMs, signal) => inner.log.wait(after, blockMs, signal),
      trim: (retention) => inner.log.trim(retention),
      compare: (a, b) => inner.log.compare(a, b),
    },
    load: (scope) => inner.load(scope).then(deepFreeze),
    due: (now, limit, after) => inner.due(now, limit, after).then(deepFreeze),
    remove: (scope) => inner.remove(scope),
    commit(writes) {
      for (const write of writes) {
        deepFreeze(write.snapshot);
        for (const input of write.inputs) deepFreeze(input);
        for (const append of write.appends) deepFreeze(append);
      }
      return inner.commit(writes);
    },
    ...(inner.close ? { close: inner.close.bind(inner) } : {}),
  };
}
