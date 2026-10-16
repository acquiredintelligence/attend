import type { DecisionLog, Position } from '../store/types.js';
import type { CursorParts } from './types.js';

export function encodeCursor(parts: CursorParts): string {
  throw new Error('attend: encodeCursor is not implemented');
}

/** Null for a malformed cursor or one with another version prefix. */
export function decodeCursor(cursor: string): CursorParts | null {
  throw new Error('attend: decodeCursor is not implemented');
}

/** Where to start reading, and whether to say truncated. */
export function resolveStart(log: DecisionLog, cursor: string | null | undefined, maxAgeMs: number | undefined, now: number): Promise<{ after: Position | null; truncated: boolean; fromNow: boolean }> {
  throw new Error('attend: resolveStart is not implemented');
}
