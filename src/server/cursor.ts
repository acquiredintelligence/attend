import type { DecisionLog, DecisionPage, Position } from '../store/types.js';
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

/** Where a read starts: the head for a null cursor, the floor for another epoch or an empty position, else the cursor's own position. */
export async function resolveStart(log: DecisionLog, epoch: string, cursor: string | null | undefined): Promise<{ after: Position | null; truncated: boolean; fromNow: boolean }> {
  if (cursor == null) return { after: (await log.bounds()).last, truncated: false, fromNow: true };
  const invalid = () => eventsError('InvalidParams', 'Invalid cursor', { field: 'cursor', reason: 'malformed' });
  const parts = decodeCursor(cursor);
  if (!parts) throw invalid();
  if (parts.epoch !== epoch) return { after: null, truncated: true, fromNow: false };
  if (parts.position === '') return { after: null, truncated: false, fromNow: false };
  try { log.compare(parts.position, parts.position); } catch { throw invalid(); }
  return { after: parts.position, truncated: false, fromNow: false };
}

/** Whether rows past `after` were gone when `page` was read from it: below the retention floor, or decided before `sinceAt`. A page that found nothing past `after` needs no store call, since retention never drops the head. */
export async function lostBefore(log: DecisionLog, after: Position | null, page: DecisionPage, sinceAt: number | undefined): Promise<boolean> {
  if (page.checked === null || (after !== null && log.compare(page.checked, after) === 0)) return false;
  if (after !== null) {
    const first = (await log.bounds()).first;
    if (first !== null && log.compare(after, first) < 0) return true;
  }
  if (sinceAt !== undefined) {
    const row = (await log.read({ after, limit: 1 })).rows[0];
    return row !== undefined && row.at < sinceAt;
  }
  return false;
}
