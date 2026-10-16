import fc from 'fast-check';
import { describe, expect, test } from 'vitest';
import { fnvHex, fnvStart, foldId, utf8Length, windowId } from '../src/core/ids.js';
import { windowId as oracleWindowId } from './oracle/step.js';

const text = fc.oneof(
  fc.constantFrom('', 'ascii', 'é東京', '🌍🚀', '\ud800', '\udfff', '\ud800a\udfff'),
  fc.array(fc.integer({ min: 0, max: 0xffff }), { maxLength: 40 }).map((units) => String.fromCharCode(...units)),
  fc.string({ unit: 'binary', maxLength: 40 }),
  fc.string({ unit: 'grapheme', maxLength: 20 }),
);
const ids = fc.array(text, { maxLength: 8 });

describe('window ids', () => {
  test('matches the BigInt oracle for UTF-16 strings', () => {
    fc.assert(fc.property(text, ids, (scope, values) => {
      expect(windowId(scope, values)).toBe(oracleWindowId(scope, values));
    }), { numRuns: Number(process.env.ATTEND_FNV_RUNS ?? 20000), seed: 1001 });
  }, 120_000);

  test('matches every prefix when folded incrementally', () => {
    fc.assert(fc.property(text, ids, (scope, values) => {
      const state = fnvStart(scope);
      expect(fnvHex(state)).toBe(oracleWindowId(scope, []));
      for (let i = 0; i < values.length; i += 1) {
        foldId(state, values[i]!);
        expect(fnvHex(state)).toBe(oracleWindowId(scope, values.slice(0, i + 1)));
        expect(state.hi).toBe(state.hi >>> 0);
        expect(state.lo).toBe(state.lo >>> 0);
      }
    }), { numRuns: 2000, seed: 1002 });
  }, 60_000);

  test('preserves pinned window ids', () => {
    expect(windowId('scope', ['a'])).toBe('cf8923dfae84f930');
    expect(windowId('scope', ['e'])).toBe('cf8927dfae84fffc');
    expect(windowId('scope', ['f'])).toBe('cf892adfae850515');
    expect(windowId('scope', ['b', 'd'])).toBe('eaf0348d05594776');
    expect(windowId('scope', ['a', 'b'])).toBe('dc811072354ed32b');
  });

  test('counts the same UTF-8 bytes as TextEncoder', () => {
    const encoder = new TextEncoder();
    fc.assert(fc.property(text, (value) => {
      expect(utf8Length(value)).toBe(encoder.encode(value).length);
    }), { numRuns: 5000, seed: 1003 });
  }, 60_000);

  test('pads both hexadecimal limbs', () => {
    expect(fnvHex({ hi: 0, lo: 1 })).toBe('0000000000000001');
    expect(fnvHex({ hi: 0xffffffff, lo: 0 })).toBe('ffffffff00000000');
  });
});
