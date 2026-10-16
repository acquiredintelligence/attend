import type { IdRing, KnownIds, ScopeState } from './types.js';

type SeenState = Pick<ScopeState, 'seen' | 'known'>;

/** The snapshot's seen ring and refcounts across seen, open and held ids. O(S + W). */
export function seenFrom(seen: readonly string[], open: readonly { id: string }[], held: readonly { id: string }[]): SeenState {
  const known: KnownIds = new Map();
  for (const id of seen) known.set(id, (known.get(id) ?? 0) + 1);
  for (const entry of open) known.set(entry.id, (known.get(entry.id) ?? 0) + 1);
  for (const entry of held) known.set(entry.id, (known.get(entry.id) ?? 0) + 1);
  return { seen: { ids: [...seen], head: 0, size: seen.length, capacity: seen.length }, known };
}

/** The ring's ids, oldest first, as a fresh array for `ScopeRecord.seen`. */
export function seenIds(ring: IdRing): string[] {
  const ids: string[] = [];
  for (let i = 0; i < ring.size; i += 1) ids.push(ring.ids[(ring.head + i) % ring.capacity]!);
  return ids;
}

/**
 * Admits `id` for a batch. Returns false when it is already known, so the caller counts a
 * duplicate. Otherwise counts it for the ring and for the open entry the caller appends, and
 * stages the ring write. Eviction waits for `seenTrim`.
 */
export function seenAdmit(state: SeenState, id: string): boolean {
  if (state.known.has(id)) return false;
  const ring = state.seen;
  if (ring.size === ring.capacity) {
    const capacity = Math.max(8, ring.capacity * 2);
    const ids: string[] = new Array<string>(capacity).fill('');
    for (let i = 0; i < ring.size; i += 1) ids[i] = ring.ids[(ring.head + i) % ring.capacity]!;
    ring.ids = ids;
    ring.capacity = capacity;
    ring.head = 0;
  }
  state.known.set(id, 2);
  ring.ids[(ring.head + ring.size) % ring.capacity] = id;
  ring.size += 1;
  return true;
}

/** After the batch: evicts the oldest ids down to `seenCount`, decrementing `known` and deleting ids at 0. */
export function seenTrim(state: SeenState, seenCount: number): void {
  const ring = state.seen;
  const limit = Math.max(0, seenCount);
  while (ring.size > limit) {
    seenRelease(state, ring.ids[ring.head]!);
    ring.ids[ring.head] = '';
    ring.head = (ring.head + 1) % ring.capacity;
    ring.size -= 1;
  }
}

/** An id left the open or held window: decrement its count and delete it at zero. */
export function seenRelease(state: SeenState, id: string): void {
  const count = state.known.get(id)!;
  if (count === 1) state.known.delete(id);
  else state.known.set(id, count - 1);
}
