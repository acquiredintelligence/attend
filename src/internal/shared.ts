import type { Event, JsonObject, JsonValue } from '../types.js';

export function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export function assertNever(value: never): never {
  throw new Error(`attend: unexpected ${JSON.stringify(value)}`);
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function copyJson(value: JsonValue): JsonValue {
  if (typeof value !== 'object' || value === null) return value;
  return Array.isArray(value) ? value.map(copyJson) : copyObject(value);
}

function copyObject(object: JsonObject): JsonObject {
  const out: JsonObject = {};
  for (const key of Object.keys(object)) {
    const value = object[key];
    if (value !== undefined) out[key] = copyJson(value);
  }
  return out;
}

/** An event the caller cannot reach: its data copied as JSON, the way every store would persist it. */
export function copyEvent(event: Event): Event {
  return { ...event, data: copyObject(event.data) };
}
