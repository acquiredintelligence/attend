/**
 * Checks shared by the storage adapters for what crosses their boundary: the table prefix
 * going in, and rows, replies and stored JSON coming back. Every error names the adapter.
 */
import type { Event, ScopeRecord } from '../types.js';

const decoder = new TextDecoder();

export function boundary(adapter: string) {
  const error = (message: string) => new Error(`${adapter}: ${message}`);

  /** Drivers hand text back as a string or, like node-redis with Buffer replies, as bytes. */
  const text = (value: unknown): string => {
    if (typeof value === 'string') return value;
    if (value instanceof Uint8Array) return decoder.decode(value);
    throw error(`expected text, got ${typeof value}`);
  };

  /** `pg` hands json columns back parsed; the other drivers hand back text or bytes. */
  const json = (value: unknown): unknown => (typeof value === 'string' || value instanceof Uint8Array ? JSON.parse(text(value)) : value);

  return {
    text,

    /** The prefix is spliced into SQL as part of a table name, so it must be a bare identifier. */
    tablePrefix(value = 'attend_', options?: { lowercase?: boolean }): string {
      const [pattern, rule] = options?.lowercase
        ? [/^[a-z_][a-z0-9_]*$/, 'lowercase letters, digits and underscores']
        : [/^[A-Za-z_][A-Za-z0-9_]*$/, 'letters, digits and underscores'];
      if (!pattern.test(value)) throw error(`prefix must be ${rule}, got "${value}"`);
      return value;
    },

    column(row: unknown, name: string): unknown {
      if (typeof row !== 'object' || row === null) throw error(`expected a row, got ${typeof row}`);
      return Reflect.get(row, name);
    },

    /** Twenty digits, so positions compare as strings the way they compare as numbers. */
    formatPosition(value: unknown): string {
      const digits = typeof value === 'number' || typeof value === 'bigint' ? String(value) : text(value);
      if (!/^\d{1,20}$/.test(digits)) throw error(`expected an integer position, got "${digits}"`);
      return digits.padStart(20, '0');
    },

    /** The digits of a position from `formatPosition`. */
    parsePosition(position: string): string {
      if (!/^\d{1,20}$/.test(position)) throw error(`not a position from this log: "${position}"`);
      return position;
    },

    /** The adapter wrote this JSON from a `ScopeRecord`; past the identity checks, the shape is its own. */
    parseRecord(value: unknown, scope: string): ScopeRecord {
      const parsed = json(value);
      if (typeof parsed !== 'object' || parsed === null || Reflect.get(parsed, 'scope') !== scope || typeof Reflect.get(parsed, 'version') !== 'number') {
        throw error(`corrupt record for scope "${scope}"`);
      }
      return parsed as ScopeRecord;
    },

    parseEvent(value: unknown): Event {
      const parsed = json(value);
      const field = (name: string): unknown => (typeof parsed === 'object' && parsed !== null ? Reflect.get(parsed, name) : undefined);
      const data = field('data');
      if (typeof field('id') !== 'string' || typeof field('name') !== 'string' || typeof field('at') !== 'number' || typeof data !== 'object' || data === null) {
        throw error('corrupt log entry');
      }
      return parsed as Event;
    },
  };
}
