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

const isRawJSON = (JSON as { isRawJSON?: (value: unknown) => boolean }).isRawJSON;

/** `JSON.parse(JSON.stringify(value))` without building the string. Undefined means JSON would omit it. */
function toJson(value: unknown, key: string | number, ancestors: object[]): JsonValue | undefined {
  if ((typeof value === 'object' && value !== null) || typeof value === 'bigint') {
    const toJSON: unknown = (value as { toJSON?: unknown }).toJSON;
    if (typeof toJSON === 'function') value = toJSON.call(value, String(key));
  }
  if (typeof value === 'object' && value !== null) {
    if (value instanceof Number) value = Number(value);
    else if (value instanceof String) value = String(value);
    else if (value instanceof Boolean || value instanceof BigInt) value = value.valueOf();
  }
  switch (typeof value) {
    case 'string':
    case 'boolean':
      return value;
    case 'number':
      // `+ 0` turns -0 into 0, as JSON does.
      return Number.isFinite(value) ? value + 0 : null;
    case 'bigint':
      throw new TypeError('attend: event data cannot hold a BigInt');
    case 'object': {
      if (value === null) return null;
      const raw: unknown = (value as { rawJSON?: unknown }).rawJSON;
      if (typeof raw === 'string' && isRawJSON?.(value)) return JSON.parse(raw) as JsonValue;
      if (ancestors.includes(value)) throw new TypeError('attend: event data cannot hold a cycle');
      ancestors.push(value);
      let out: JsonValue;
      if (Array.isArray(value)) {
        const array: JsonValue[] = new Array<JsonValue>(value.length);
        for (let i = 0; i < value.length; i += 1) array[i] = toJson(value[i], i, ancestors) ?? null;
        out = array;
      } else {
        const object: JsonObject = {};
        const source = value as Record<string, unknown>;
        for (const name of Object.keys(source)) {
          const copied = toJson(source[name], name, ancestors);
          if (copied === undefined) continue;
          if (name === '__proto__') Object.defineProperty(object, name, { value: copied, enumerable: true, writable: true, configurable: true });
          else object[name] = copied;
        }
        out = object;
      }
      ancestors.pop();
      return out;
    }
    default:
      return undefined;
  }
}

/** An event the caller cannot reach, copied as JSON: what every store would persist. */
export function copyEvent(event: Event): Event {
  const copied = toJson(event, '', []);
  if (!isRecord(copied) || !isRecord(copied.data)) throw new TypeError('attend: event data must be a JSON object');
  return copied as unknown as Event;
}
