import type { Buffer, Entry, Event, Kind, OpenWindow, SettlePolicy, SettleReason, TriggerSummary } from './types.js';
import { fnvStart, foldId, utf8Length } from './ids.js';

/** Closing, continuous and passive entries are acts. */
export function isAct(entry: { kind: Kind }): boolean {
  return entry.kind === 'closing' || entry.kind === 'continuous' || entry.kind === 'passive';
}

function isSheddable(entry: { kind: Kind }): boolean {
  return entry.kind === 'consequence' || entry.kind === 'context';
}

/** The UTF-8 bytes of name and text (or JSON data), plus 40 bytes of framing. */
export function estimateRenderedBytes(event: Event): number {
  return utf8Length(event.name) + utf8Length(event.text !== undefined ? event.text : JSON.stringify(event.data)) + 40;
}

function emptySummary(): TriggerSummary {
  return {
    minFlushAt: Infinity, lastClosingAt: NaN, lastStreamAt: NaN, lastPassiveAt: NaN,
    firstActAt: NaN, firstActIndex: -1, prefixMinSheddableAt: Infinity, sheddable: 0, lastSeenAt: NaN,
  };
}

function summarizeEntry(summary: TriggerSummary, entry: Entry, index: number): void {
  if (entry.kind === 'flush') summary.minFlushAt = Math.min(summary.minFlushAt, entry.seenAt);
  if (entry.kind === 'closing') summary.lastClosingAt = entry.seenAt;
  if (entry.kind === 'continuous') summary.lastStreamAt = entry.seenAt;
  if (entry.kind === 'passive') summary.lastPassiveAt = entry.seenAt;
  if (summary.firstActIndex === -1 && isAct(entry)) {
    summary.firstActIndex = index;
    summary.firstActAt = entry.seenAt;
  }
  if (isSheddable(entry)) {
    summary.sheddable += 1;
    if (summary.firstActIndex === -1) summary.prefixMinSheddableAt = Math.min(summary.prefixMinSheddableAt, entry.seenAt);
  }
  summary.lastSeenAt = entry.seenAt;
}

function summarize(entries: readonly Entry[]): TriggerSummary {
  const summary = emptySummary();
  for (let i = 0; i < entries.length; i += 1) summarizeEntry(summary, entries[i]!, i);
  return summary;
}

/** An empty open window whose hash starts with `len:scope`. */
export function openWindow(scope: string): OpenWindow {
  return {
    entries: [], sizes: [], bytes: 0, dropped: 0, gap: false, forcedAt: null, endedAt: null,
    summary: emptySummary(), hash: fnvStart(scope), hashValid: true, monotone: true,
  };
}

/** Copies a snapshot buffer into new arrays, sharing its immutable entries. O(W). */
export function windowFrom(scope: string, buffer: Buffer): OpenWindow {
  const open = openWindow(scope);
  for (const entry of buffer.entries) windowAppend(open, entry, estimateRenderedBytes(entry));
  open.dropped = buffer.dropped;
  open.gap = buffer.gap;
  open.forcedAt = buffer.forcedAt;
  open.endedAt = buffer.endedAt ?? null;
  return open;
}

/** Appends an entry and updates sizes, summary, monotonicity and any valid hash in O(1). */
export function windowAppend(open: OpenWindow, entry: Entry, bytes: number): void {
  if (entry.seenAt < open.summary.lastSeenAt) open.monotone = false;
  const index = open.entries.length;
  open.entries.push(entry);
  open.sizes.push(bytes);
  open.bytes += bytes;
  summarizeEntry(open.summary, entry, index);
  if (open.hashValid) foldId(open.hash, entry.id);
}

function prefixMinimum(open: OpenWindow): number {
  const end = open.summary.firstActIndex === -1 ? open.entries.length : open.summary.firstActIndex;
  let min = Infinity;
  for (let i = 0; i < end; i += 1) {
    const entry = open.entries[i]!;
    if (isSheddable(entry)) {
      if (open.monotone) return entry.seenAt;
      min = Math.min(min, entry.seenAt);
    }
  }
  return min;
}

function removedFrom(open: OpenWindow, count: number, bytes: number, prefixCount: number): void {
  open.dropped += count;
  open.bytes -= bytes;
  open.summary.sheddable -= count;
  if (open.summary.firstActIndex !== -1) open.summary.firstActIndex -= prefixCount;
  if (prefixCount > 0) open.summary.prefixMinSheddableAt = prefixMinimum(open);
  open.summary.lastSeenAt = open.entries[open.entries.length - 1]?.seenAt ?? NaN;
  open.hashValid = false;
}

/**
 * Removes the `count` slots at `at` from both arrays. When fewer slots precede the gap than follow
 * it, they move up and the front is shifted off, which V8 trims in O(1); `splice` would move the tail.
 */
function closeGap(entries: Entry[], sizes: number[] | null, at: number, count: number): void {
  if (at > entries.length - at - count) {
    entries.splice(at, count);
    sizes?.splice(at, count);
    return;
  }
  for (let i = at - 1; i >= 0; i -= 1) {
    entries[i + count] = entries[i]!;
    if (sizes !== null) sizes[i + count] = sizes[i]!;
  }
  for (let i = 0; i < count; i += 1) {
    entries.shift();
    sizes?.shift();
  }
}

/** Removes stale sheddable entries before the first act, returning them in list order. */
export function windowPrune(open: OpenWindow, maxWindowMs: number, now: number): Entry[] | null {
  const { entries, sizes, summary } = open;
  if (summary.prefixMinSheddableAt + maxWindowMs > now) return null;
  const end = summary.firstActIndex === -1 ? entries.length : summary.firstActIndex;
  let gone: Entry[] | null = null;
  let bytes = 0;
  let write = 0;
  let read = 0;
  for (; read < end; read += 1) {
    const entry = entries[read]!;
    const stale = entry.seenAt + maxWindowMs <= now;
    if (!stale && open.monotone) break;
    if (stale && isSheddable(entry)) {
      if (gone === null) gone = [entry];
      else gone.push(entry);
      bytes += sizes[read]!;
      continue;
    }
    if (gone !== null) {
      entries[write] = entry;
      sizes[write] = sizes[read]!;
    }
    write += 1;
  }
  if (gone === null) return null;
  closeGap(entries, sizes, write, read - write);
  removedFrom(open, gone.length, bytes, gone.length);
  return gone;
}

/** Removes the first `entries.length - maxEvents` sheddable entries in one pass, then closes the gap. */
function shed(entries: Entry[], maxEvents: number, open: OpenWindow | null): Entry[] | null {
  const excess = entries.length - maxEvents;
  if (excess <= 0) return null;
  const sizes = open === null ? null : open.sizes;
  const head = open === null || open.summary.firstActIndex === -1 ? entries.length : open.summary.firstActIndex;
  let gone: Entry[] | null = null;
  let bytes = 0;
  let beforeAct = 0;
  let write = 0;
  let read = 0;
  for (; read < entries.length && (gone === null || gone.length < excess); read += 1) {
    const entry = entries[read]!;
    if (isSheddable(entry)) {
      if (gone === null) gone = [entry];
      else gone.push(entry);
      if (sizes !== null) bytes += sizes[read]!;
      if (read < head) beforeAct += 1;
      continue;
    }
    if (gone !== null) {
      entries[write] = entry;
      if (sizes !== null) sizes[write] = sizes[read]!;
    }
    write += 1;
  }
  if (gone === null) return null;
  closeGap(entries, sizes, write, read - write);
  if (open !== null) removedFrom(open, gone.length, bytes, beforeAct);
  return gone;
}

/** Removes the oldest sheddable entries until the window fits, in place. */
export function windowShed(open: OpenWindow, maxEvents: number): Entry[] | null {
  if (open.entries.length <= maxEvents || open.summary.sheddable === 0) return null;
  return shed(open.entries, maxEvents, open);
}

/** Sheds a plain list in place for a merged window, returning removals in order. */
export function shedEntries(entries: Entry[], maxEvents: number): Entry[] | null {
  return shed(entries, maxEvents, null);
}

/** The open window's earliest deadline and its reason, in O(1) from the summary. */
export function windowDeadline(policy: SettlePolicy, open: OpenWindow): { at: number; reason: SettleReason } | null {
  const s = open.summary;
  let at = Infinity;
  let reason: SettleReason | null = null;
  if (open.forcedAt !== null) {
    at = open.forcedAt;
    reason = 'forced';
  }
  if (open.endedAt !== null && (reason === null || open.endedAt < at)) {
    at = open.endedAt;
    reason = 'closing';
  }
  if (open.entries.length > 0 && open.entries.length > policy.maxEvents && (reason === null || s.lastSeenAt < at)) {
    at = s.lastSeenAt;
    reason = 'overflow';
  }
  if (s.minFlushAt !== Infinity && (reason === null || s.minFlushAt < at)) {
    at = s.minFlushAt;
    reason = 'flush';
  }
  const closing = s.lastClosingAt + policy.closingGraceMs;
  if (!Number.isNaN(s.lastClosingAt) && (reason === null || closing < at)) {
    at = closing;
    reason = 'closing';
  }
  const stream = s.lastStreamAt + policy.streamGapMs;
  if (!Number.isNaN(s.lastStreamAt) && (reason === null || stream < at)) {
    at = stream;
    reason = 'stream_quiet';
  }
  const passive = s.lastPassiveAt + policy.passiveQuietMs;
  if (!Number.isNaN(s.lastPassiveAt) && (reason === null || passive < at)) {
    at = passive;
    reason = 'passive_quiet';
  }
  const age = s.firstActAt + policy.maxWindowMs;
  if (!Number.isNaN(s.firstActAt) && (reason === null || age < at)) {
    at = age;
    reason = 'age';
  }
  if (policy.maxWindowBytes !== undefined && open.entries.length > 0 && open.bytes >= policy.maxWindowBytes
    && (reason === null || s.lastSeenAt < at)) {
    at = s.lastSeenAt;
    reason = 'size';
  }
  return reason === null ? null : { at, reason };
}

/** Summarizes a snapshot once and computes its deadline without hashing or mutating it. */
export function deadline(policy: SettlePolicy, buffer: Buffer): { at: number; reason: SettleReason } | null {
  let bytes = 0;
  if (policy.maxWindowBytes !== undefined) for (const entry of buffer.entries) bytes += estimateRenderedBytes(entry);
  return windowDeadline(policy, {
    entries: buffer.entries, sizes: [], bytes, dropped: buffer.dropped, gap: buffer.gap,
    forcedAt: buffer.forcedAt, endedAt: buffer.endedAt ?? null, summary: summarize(buffer.entries),
    hash: { hi: 0, lo: 0 }, hashValid: false, monotone: false,
  });
}

/** Checks I3, I4, I5 and monotonicity, naming the broken invariant in any error. */
export function assertWindow(scope: string, open: OpenWindow): void {
  const summary = summarize(open.entries);
  for (const key of Object.keys(summary)) {
    if (!Object.is(Reflect.get(summary, key), Reflect.get(open.summary, key))) throw new Error(`I3: ${key}`);
  }
  if (open.sizes.length !== open.entries.length || open.sizes.reduce((sum, size) => sum + size, 0) !== open.bytes) {
    throw new Error('I4: sizes and bytes');
  }
  if (open.hashValid) {
    const hash = fnvStart(scope);
    for (const entry of open.entries) foldId(hash, entry.id);
    if (hash.hi !== open.hash.hi || hash.lo !== open.hash.lo) throw new Error('I5: hash');
  }
  if (open.monotone) {
    for (let i = 1; i < open.entries.length; i += 1) {
      if (open.entries[i]!.seenAt < open.entries[i - 1]!.seenAt) throw new Error('monotone: seenAt decreased');
    }
  }
}
