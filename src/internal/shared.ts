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
const MAX_DEPTH = 1000;

/**
 * `JSON.parse(JSON.stringify(value))` without building the string; undefined means JSON would omit it.
 * A depth limit stands in for JSON's cycle check, so a cycle and data nested past it both reject.
 */
function toJson(value: unknown, key: string | number, depth: number): JsonValue | undefined {
  if ((typeof value === 'object' && value !== null) || typeof value === 'function' || typeof value === 'bigint') {
    const toJSON: unknown = (value as { toJSON?: unknown }).toJSON;
    if (typeof toJSON === 'function') value = toJSON.call(value, String(key));
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
    case 'object':
      return value === null ? null : copyObject(value, key, depth);
    default:
      return undefined;
  }
}

function copyObject(value: object, key: string | number, depth: number): JsonValue | undefined {
  const prototype: unknown = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== Array.prototype) {
    if (value instanceof Number) return toJson(Number(value), key, depth);
    if (value instanceof String) return String(value);
    if (value instanceof Boolean) {
      const unboxed = booleanData(value);
      if (unboxed !== undefined) return unboxed;
    }
    if (value instanceof BigInt) throw new TypeError('attend: event data cannot hold a BigInt');
    if (prototype === null && isRawJSON?.(value)) return toJson(JSON.parse((value as { rawJSON: string }).rawJSON), key, depth);
  }
  if (depth >= MAX_DEPTH) throw new TypeError(`attend: event data holds a cycle or nests deeper than ${MAX_DEPTH}`);
  if (Array.isArray(value)) {
    const length = value.length;
    const array: JsonValue[] = new Array<JsonValue>(length);
    for (let i = 0; i < length; i += 1) array[i] = toJson(value[i], i, depth + 1) ?? null;
    return array;
  }
  const object: JsonObject = {};
  const source = value as Record<string, unknown>;
  for (const name of Object.keys(source)) {
    const copied = toJson(source[name], name, depth + 1);
    if (copied === undefined) continue;
    if (name === '__proto__') Object.defineProperty(object, name, { value: copied, enumerable: true, writable: true, configurable: true });
    else object[name] = copied;
  }
  return object;
}

/** The boolean a real Boolean object wraps, read from its slot as JSON does; undefined for an object that only inherits Boolean.prototype. */
const booleanValueOf = Boolean.prototype.valueOf;

function booleanData(value: object): boolean | undefined {
  try {
    const unboxed: unknown = booleanValueOf.call(value);
    return typeof unboxed === 'boolean' ? unboxed : undefined;
  } catch {
    return undefined;
  }
}

function isPlainField(value: unknown): boolean {
  return typeof value === 'string' || typeof value === 'boolean' || value === null || (typeof value === 'number' && Number.isFinite(value) && !Object.is(value, -0));
}

/**
 * An event the caller cannot reach: a plain JSON value, the same on every store. Each property is
 * read once, so a getter or Proxy cannot hand the stores a value the checks never saw.
 */
export function copyEvent(event: Event): Event {
  if (typeof event === 'object' && event !== null && Object.getPrototypeOf(event) === Object.prototype && typeof (event as { toJSON?: unknown }).toJSON !== 'function') {
    const fields: Record<string, unknown> = { ...event };
    if (Object.getOwnPropertySymbols(fields).length === 0 && Object.keys(fields).every((name) => name === 'data' || isPlainField(fields[name]))) {
      const data = toJson(fields.data, 'data', 1);
      if (!isRecord(data)) throw new TypeError('attend: event data must be a JSON object');
      fields.data = data;
      return fields as unknown as Event;
    }
    return checkedEvent(toJson(fields, '', 0));
  }
  return checkedEvent(toJson(event, '', 0));
}

function checkedEvent(copied: JsonValue | undefined): Event {
  if (!isRecord(copied) || !isRecord(copied.data)) throw new TypeError('attend: event data must be a JSON object');
  return copied as unknown as Event;
}
