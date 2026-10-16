import type { JsonObject } from '../core/types.js';
import type { EventsErrorObject } from './types.js';

export const ERROR_CODES = { InvalidParams: -32602, NotFound: -32023, Forbidden: -32024, ResourceExhausted: -32025, Unsupported: -32026, Transient: -32603 } as const;
export type EventsErrorName = keyof typeof ERROR_CODES;

export function eventsError(name: EventsErrorName, message: string, data?: JsonObject): EventsErrorObject {
  throw new Error('attend: eventsError is not implemented');
}
