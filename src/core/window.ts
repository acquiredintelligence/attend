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

function summarize(entries: readonly Entry[], start = 0): TriggerSummary {
  const summary = emptySummary();
  for (let i = start; i < entries.length; i += 1) summarizeEntry(summary, entries[i]!, i - start);
  return summary;
}

/** An empty open window whose hash starts with `len:scope`. */
export function openWindow(scope: string): OpenWindow {
  return {
    entries: [], start: 0, sizes: [], bytes: 0, dropped: 0, gap: false, forcedAt: null, endedAt: null,
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

/** The live entry count, in O(1). */
export function windowSize(open: OpenWindow): number {
  return open.entries.length - open.start;
}

/** A fresh array of the live entries, in O(W). */
export function windowEntries(open: OpenWindow): Entry[] {
  return open.entries.slice(open.start);
}

/** Moves live slots to the front and returns the owned entries array, in O(live). */
export function windowCompact(open: OpenWindow): Entry[] {
  if (open.start === 0) return open.entries;
  const length = windowSize(open);
  for (let i = 0; i < length; i += 1) {
    open.entries[i] = open.entries[open.start + i]!;
    open.sizes[i] = open.sizes[open.start + i]!;
  }
  open.entries.length = length;
  open.sizes.length = length;
  open.start = 0;
  return open.entries;
}

/** Appends an entry and updates sizes, summary, monotonicity and any valid hash in O(1). */
export function windowAppend(open: OpenWindow, entry: Entry, bytes: number): void {
  if (entry.seenAt < open.summary.lastSeenAt) open.monotone = false;
  const index = windowSize(open);
  open.entries.push(entry);
  open.sizes.push(bytes);
  open.bytes += bytes;
  summarizeEntry(open.summary, entry, index);
  if (open.hashValid) foldId(open.hash, entry.id);
}

function prefixMinimum(open: OpenWindow): number {
  const end = open.summary.firstActIndex === -1 ? windowSize(open) : open.summary.firstActIndex;
  let min = Infinity;
  for (let i = 0; i < end; i += 1) {
    const entry = open.entries[open.start + i]!;
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
  open.summary.lastSeenAt = windowSize(open) === 0 ? NaN : open.entries[open.entries.length - 1]!.seenAt;
  open.hashValid = false;
}

/**
 * Removes the `count` slots at `at`. A plain list splices. A window moves the smaller side: it
 * splices the tail, or moves the survivors before the gap up and advances `start`, because V8's
 * `shift` copies the whole array at 100 elements or fewer.
 */
function closeGap(entries: Entry[], open: OpenWindow | null, at: number, count: number): void {
  if (open === null) {
    entries.splice(at, count);
    return;
  }
  if (at - open.start > entries.length - at - count) {
    entries.splice(at, count);
    open.sizes.splice(at, count);
  } else {
    for (let i = at - 1; i >= open.start; i -= 1) {
      entries[i + count] = entries[i]!;
      open.sizes[i + count] = open.sizes[i]!;
    }
    open.start += count;
  }
  // Waiting until the dead prefix passes half the backing array lets V8 shrink both arrays to the live count, then regrow them, every cycle.
  if (2 * open.start > windowSize(open)) windowCompact(open);
}

/** Removes stale sheddable entries before the first act, returning them in list order. */
export function windowPrune(open: OpenWindow, maxWindowMs: number, now: number): Entry[] | null {
  const { entries, start, sizes, summary } = open;
  if (summary.prefixMinSheddableAt + maxWindowMs > now) return null;
  const end = summary.firstActIndex === -1 ? windowSize(open) : summary.firstActIndex;
  let gone: Entry[] | null = null;
  let bytes = 0;
  let write = 0;
  let read = 0;
  for (; read < end; read += 1) {
    const entry = entries[start + read]!;
    const stale = entry.seenAt + maxWindowMs <= now;
    if (!stale && open.monotone) break;
    if (stale && isSheddable(entry)) {
      if (gone === null) gone = [entry];
      else gone.push(entry);
      bytes += sizes[start + read]!;
      continue;
    }
    if (gone !== null) {
      entries[start + write] = entry;
      sizes[start + write] = sizes[start + read]!;
    }
    write += 1;
  }
  if (gone === null) return null;
  closeGap(entries, open, start + write, read - write);
  removedFrom(open, gone.length, bytes, gone.length);
  return gone;
}

/** Removes the first `live - maxEvents` sheddable entries in one pass, then closes the gap. */
function shed(entries: Entry[], maxEvents: number, open: OpenWindow | null): Entry[] | null {
  const start = open?.start ?? 0;
  const length = entries.length - start;
  const excess = length - maxEvents;
  if (excess <= 0) return null;
  const sizes = open === null ? null : open.sizes;
  const head = open === null || open.summary.firstActIndex === -1 ? length : open.summary.firstActIndex;
  let gone: Entry[] | null = null;
  let bytes = 0;
  let beforeAct = 0;
  let write = 0;
  let read = 0;
  for (; read < length && (gone === null || gone.length < excess); read += 1) {
    const entry = entries[start + read]!;
    if (isSheddable(entry)) {
      if (gone === null) gone = [entry];
      else gone.push(entry);
      if (sizes !== null) bytes += sizes[start + read]!;
      if (read < head) beforeAct += 1;
      continue;
    }
    if (gone !== null) {
      entries[start + write] = entry;
      if (sizes !== null) sizes[start + write] = sizes[start + read]!;
    }
    write += 1;
  }
  if (gone === null) return null;
  closeGap(entries, open, start + write, read - write);
  if (open !== null) removedFrom(open, gone.length, bytes, beforeAct);
  return gone;
}

/** Removes the oldest sheddable entries until the window fits, in place. */
export function windowShed(open: OpenWindow, maxEvents: number): Entry[] | null {
  if (windowSize(open) <= maxEvents || open.summary.sheddable === 0) return null;
  return shed(open.entries, maxEvents, open);
}

/** Sheds a plain list in place for a merged window, returning removals in order. */
export function shedEntries(entries: Entry[], maxEvents: number): Entry[] | null {
  return shed(entries, maxEvents, null);
}

/** The open window's earliest deadline and its reason, in O(1) from the summary. */
export function windowDeadline(policy: SettlePolicy, open: OpenWindow): { at: number; reason: SettleReason } | null {
  const s = open.summary;
  const size = windowSize(open);
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
  if (size > 0 && size > policy.maxEvents && (reason === null || s.lastSeenAt < at)) {
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
  if (policy.maxWindowBytes !== undefined && size > 0 && open.bytes >= policy.maxWindowBytes
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
    entries: buffer.entries, start: 0, sizes: [], bytes, dropped: buffer.dropped, gap: buffer.gap,
    forcedAt: buffer.forcedAt, endedAt: buffer.endedAt ?? null, summary: summarize(buffer.entries),
    hash: { hi: 0, lo: 0 }, hashValid: false, monotone: false,
  });
}

/** Checks I3, I4, I5 and monotonicity, naming the broken invariant in any error. */
export function assertWindow(scope: string, open: OpenWindow): void {
  if (!Number.isInteger(open.start) || open.start < 0 || 2 * open.start > windowSize(open)) throw new Error('I4: start');
  const summary = summarize(open.entries, open.start);
  for (const key of Object.keys(summary)) {
    if (!Object.is(Reflect.get(summary, key), Reflect.get(open.summary, key))) throw new Error(`I3: ${key}`);
  }
  let bytes = 0;
  for (let i = open.start; i < open.sizes.length; i += 1) bytes += open.sizes[i]!;
  if (open.sizes.length !== open.entries.length || bytes !== open.bytes) {
    throw new Error('I4: sizes and bytes');
  }
  if (open.hashValid) {
    const hash = fnvStart(scope);
    for (let i = open.start; i < open.entries.length; i += 1) foldId(hash, open.entries[i]!.id);
    if (hash.hi !== open.hash.hi || hash.lo !== open.hash.lo) throw new Error('I5: hash');
  }
  if (open.monotone) {
    for (let i = open.start + 1; i < open.entries.length; i += 1) {
      if (open.entries[i]!.seenAt < open.entries[i - 1]!.seenAt) throw new Error('monotone: seenAt decreased');
    }
  }
}
