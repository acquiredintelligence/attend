import type { DecisionLog, Position } from '../store/types.js';
import { eventsError } from './events.js';
import type { CursorParts } from './types.js';

/** Encodes a store position without interpreting it. */
export function encodeCursor(parts: CursorParts): string {
  if (!parts.epoch || parts.epoch.includes('.')) throw new TypeError('Invalid cursor epoch');
  return `v1.${parts.epoch}.${parts.position}`;
}

/** Null for a malformed cursor or one with another version prefix. */
export function decodeCursor(cursor: string): CursorParts | null {
  if (!cursor.startsWith('v1.')) return null;
  const dot = cursor.indexOf('.', 3);
  return dot > 3 ? { epoch: cursor.slice(3, dot), position: cursor.slice(dot + 1) } : null;
}

/** Where to start reading, and whether to say truncated. */
export async function resolveStart(log: DecisionLog, cursor: string | null | undefined, maxAgeMs: number | undefined, now: number): Promise<{ epoch: string; after: Position | null; truncated: boolean; fromNow: boolean }> {
  const epoch = await log.epoch();
  if (cursor == null) return { epoch, after: (await log.bounds()).last, truncated: false, fromNow: true };
  const invalid = () => eventsError('InvalidParams', 'Invalid cursor', { field: 'cursor', reason: 'malformed' });
  const parts = decodeCursor(cursor);
  if (!parts) throw invalid();
  let after: Position | null = parts.position || null;
  let truncated = parts.epoch !== epoch;
  if (truncated) after = null;
  else if (after !== null) {
    try { log.compare(after, after); } catch { throw invalid(); }
    const first = (await log.bounds()).first;
    if (first !== null && log.compare(after, first) < 0) {
      after = null;
      truncated = true;
    }
  }
  if (maxAgeMs !== undefined && !truncated) {
    const row = (await log.read({ after, limit: 1 })).rows[0];
    if (row && row.at < now - maxAgeMs) truncated = true;
  }
  return { epoch, after, truncated, fromNow: false };
}
