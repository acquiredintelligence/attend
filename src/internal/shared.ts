import type { Event, JsonObject } from '../types.js';

export function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export function assertNever(value: never): never {
  throw new Error(`attend: unexpected ${JSON.stringify(value)}`);
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** An event the caller cannot reach: its data copied as JSON, the way every store would persist it. */
export function copyEvent(event: Event): Event {
  const json = JSON.stringify(event.data);
  if (json === undefined) throw new TypeError('attend: event data must be a JSON object');
  const data: unknown = JSON.parse(json);
  if (!isRecord(data)) throw new TypeError('attend: event data must be a JSON object');
  return { ...event, data: data as JsonObject };
}
