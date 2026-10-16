import { isDeepStrictEqual } from 'node:util';
import { describe, expect, test } from 'vitest';
import { copyEvent } from '../src/internal/shared.js';
import type { Event } from '../src/types.js';

const event = (data: unknown): Event => ({ id: 'e', at: 0, name: 'n', data: data as Event['data'] });
const viaJson = (data: unknown): unknown => JSON.parse(JSON.stringify(data));

function prototypes(value: unknown, out: unknown[] = []): unknown[] {
  if (typeof value === 'object' && value !== null) {
    out.push(Object.getPrototypeOf(value));
    for (const key of Object.keys(value)) prototypes((value as Record<string, unknown>)[key], out);
  }
  return out;
}

class Point {
  constructor(readonly x: number, readonly y: number) {}
  toJSON(key: string) {
    return { x: this.x, y: this.y, key };
  }
}

const sparse = [1, , 3];
const withGetter = Object.defineProperty({ a: 1 }, 'b', { get: () => 2, enumerable: true });
const hidden = Object.defineProperty({ a: 1 }, 'b', { value: 2, enumerable: false });
const raw = (JSON as { rawJSON?: (text: string) => unknown }).rawJSON;

const cases: [string, unknown][] = [
  ['nested plain data', { a: 1, b: 'x', c: [true, null, { d: 2.5 }] }],
  ['a Date and an invalid Date', { when: new Date(1760570000123), bad: new Date(Number.NaN) }],
  ['a Date in an array', { list: [new Date(0), 1] }],
  ['toJSON on the data itself', { toJSON: () => ({ replaced: true }) }],
  ['toJSON receives its key', { p: new Point(1, 2), list: [new Point(3, 4)] }],
  ['toJSON returning undefined', { gone: { toJSON: () => undefined }, kept: 1 }],
  ['functions and symbols', { f: () => 1, s: Symbol('s'), list: [() => 1, Symbol('t')], ok: 1 }],
  ['undefined in objects and arrays', { u: undefined, list: [undefined, 1] }],
  ['non-finite numbers and -0', { nan: Number.NaN, inf: Number.POSITIVE_INFINITY, neg: Number.NEGATIVE_INFINITY, zero: -0, list: [-0, Number.NaN] }],
  ['boxed primitives', { n: Object(1), s: Object('x'), b: Object(false) }],
  ['an own __proto__ key', JSON.parse('{"__proto__":{"polluted":true},"a":1}')],
  ['integer-like keys order first', { b: 1, 2: 'two', a: 2, 1: 'one' }],
  ['a sparse array', { list: sparse }],
  ['getters and non-enumerables', { withGetter, hidden }],
  ['Map, Set, RegExp and Error become {}', { map: new Map([[1, 2]]), set: new Set([1]), re: /x/, error: new Error('e') }],
  ['a typed array and a Buffer', { typed: new Uint8Array([1, 2]), buffer: Buffer.from('hi') }],
  ['a symbol-keyed property', { [Symbol('k')]: 1, a: 1 }],
  ...(raw ? ([['JSON.rawJSON', { big: raw('12345678901234567890') }]] as [string, unknown][]) : []),
];

describe('copyEvent copies data exactly as JSON.parse(JSON.stringify(data))', () => {
  test.each(cases)('%s', (_name, data) => {
    const copied = copyEvent(event(data)).data;
    const expected = viaJson(data);
    expect(isDeepStrictEqual(copied, expected)).toBe(true);
    expect(JSON.stringify(copied)).toBe(JSON.stringify(expected));
    expect(prototypes(copied)).toEqual(prototypes(expected));
  });

  test('the copy shares no object with the input', () => {
    const inner = { deep: [1, { x: 1 }] };
    const data = { inner };
    const copied = copyEvent(event(data)).data as { inner: { deep: [number, { x: number }] } };
    inner.deep[1] = { x: 2 };
    (copied.inner.deep[1] as { x: number }).x = 3;
    expect(data).toEqual({ inner: { deep: [1, { x: 2 }] } });
    expect(copied).toEqual({ inner: { deep: [1, { x: 3 }] } });
  });

  test.each<[string, unknown, RegExp]>([
    ['a BigInt', { n: 1n }, /BigInt/],
    ['a boxed BigInt', { n: Object(1n) }, /BigInt/],
    ['a cycle', (() => { const a: Record<string, unknown> = {}; a.self = a; return a; })(), /cycle/],
    ['a cycle through an array', (() => { const a: unknown[] = []; a.push({ a }); return { a }; })(), /cycle/],
    ['a toJSON that throws', { bad: { toJSON: () => { throw new Error('nope'); } } }, /nope/],
  ])('rejects %s, as JSON.stringify does', (_name, data, message) => {
    expect(() => JSON.stringify(data)).toThrow();
    expect(() => copyEvent(event(data))).toThrow(message);
  });

  test.each<[string, unknown]>([
    ['an array', [1, 2]],
    ['a string', 'x'],
    ['null', null],
    ['undefined', undefined],
    ['a toJSON returning a number', { toJSON: () => 1 }],
  ])('rejects data that is %s, which is not a JSON object', (_name, data) => {
    expect(() => copyEvent(event(data))).toThrow(/JSON object/);
  });

  test('every field of the event is copied as JSON, not only data', () => {
    const at = Number.NaN;
    const copied = copyEvent({ id: 'e', at, name: 'n', data: { when: new Date(0) }, text: 'x', extra: [undefined] } as unknown as Event);
    expect(copied).toStrictEqual({ id: 'e', at: null, name: 'n', data: { when: '1970-01-01T00:00:00.000Z' }, text: 'x', extra: [null] });
  });

  test.each<[string, unknown]>([
    ['a BigInt in at', { id: 'e', at: 1n, name: 'n', data: {} }],
    ['a cycle in an extra field', (() => { const extra: Record<string, unknown> = {}; extra.self = extra; return { id: 'e', at: 0, name: 'n', data: {}, extra }; })()],
  ])('rejects %s outside data', (_name, value) => {
    expect(() => copyEvent(value as Event)).toThrow(TypeError);
  });

  test('a repeated object that is not a cycle is copied twice', () => {
    const shared = { x: 1 };
    expect(copyEvent(event({ a: shared, b: [shared, shared] })).data).toEqual({ a: { x: 1 }, b: [{ x: 1 }, { x: 1 }] });
  });
});
