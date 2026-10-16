import assert from 'node:assert/strict';
import fc from 'fast-check';
import { describe, expect, test } from 'vitest';
import { seenAdmit, seenFrom, seenIds, seenRelease, seenTrim } from '../src/core/seen.js';
import { initialRecord, step } from './oracle/step.js';
import { DEFAULT_POLICY } from './oracle/types.js';

const universe = Array.from({ length: 12 }, (_, i) => `id-${i}`);
const id = fc.constantFrom(...universe);
const operation = fc.oneof(
  { weight: 5, arbitrary: fc.record({ kind: fc.constant('batch'), ids: fc.array(id, { minLength: 1, maxLength: 10 }) }) },
  { weight: 2, arbitrary: fc.record({ kind: fc.constant('settle') }) },
  { weight: 2, arbitrary: fc.record({ kind: fc.constant('clear') }) },
  { weight: 3, arbitrary: fc.record({ kind: fc.constant('shed'), k: fc.nat({ max: 100 }) }) },
);

function ringSlots(state: ReturnType<typeof seenFrom>): void {
  const ring = state.seen;
  assert.equal(ring.ids.length, ring.capacity);
  for (let i = ring.size; i < ring.capacity; i += 1) {
    assert.equal(ring.ids[(ring.head + i) % ring.capacity], '');
  }
}

describe('seen ring', () => {
  test('matches growing-batch dedup and refcounts through model operations', () => {
    fc.assert(fc.property(fc.integer({ min: 0, max: 8 }), fc.array(operation, { maxLength: 100, size: 'max' }), (seenCount, operations) => {
      const state = seenFrom([], [], []);
      let seen: string[] = [];
      let open: string[] = [];
      let held: string[] = [];
      for (const op of operations) {
        switch (op.kind) {
          case 'batch': {
            const actual: boolean[] = [];
            const expected: boolean[] = [];
            seen = [...seen];
            for (const value of op.ids) {
              const admit = !seen.includes(value) && !open.includes(value) && !held.includes(value);
              expected.push(admit);
              actual.push(seenAdmit(state, value));
              if (admit) {
                seen.push(value);
                open.push(value);
              }
            }
            seen = seenCount <= 0 ? [] : seen.slice(-seenCount);
            seenTrim(state, seenCount);
            assert.deepEqual(actual, expected);
            assert.ok(state.seen.size <= Math.max(0, seenCount));
            break;
          }
          case 'settle': held = [...held, ...open]; open = []; break;
          case 'clear':
            for (const value of held) seenRelease(state, value);
            held = [];
            break;
          case 'shed': {
            const index = op.k % (open.length + held.length);
            const list = index < open.length ? open : held;
            const [removed] = list.splice(index < open.length ? index : index - open.length, 1);
            if (removed !== undefined) seenRelease(state, removed);
            break;
          }
        }
        assert.deepEqual({
          seen: seenIds(state.seen),
          counts: universe.map((value) => [value, state.known.has(value), state.known.get(value) ?? 0]),
        }, {
          seen,
          counts: universe.map((value) => {
            const count = seen.filter((item) => item === value).length + Number(open.includes(value)) + Number(held.includes(value));
            return [value, count > 0, count];
          }),
        });
        ringSlots(state);
      }
    }), { numRuns: 1000, seed: 2001 });
  }, 60_000);

  test('matches seen ids and duplicate traces from oracle event batches', () => {
    const pool = fc.integer({ min: 0, max: 14 }).map((i) => `event-${i}`);
    fc.assert(fc.property(
      fc.integer({ min: 0, max: 10 }),
      fc.array(fc.array(pool, { minLength: 1, maxLength: 10 }), { maxLength: 30, size: 'max' }),
      (seenCount, batches) => {
        const state = seenFrom([], [], []);
        const policy = { ...DEFAULT_POLICY, seenCount, maxEvents: Infinity, maxWindowMs: Infinity };
        let record = initialRecord('scope');
        for (const [now, batch] of batches.entries()) {
          let duplicates = 0;
          for (const value of batch) if (!seenAdmit(state, value)) duplicates += 1;
          seenTrim(state, seenCount);
          const result = step({ policy }, record, {
            kind: 'events', events: batch.map((value) => ({ id: value, name: 'edit', at: now, data: {}, kind: 'context' })),
          }, now);
          const ingested = result.effects.find((effect) => effect.trace.kind === 'ingested')?.trace;
          expect(ingested?.kind === 'ingested' ? ingested.duplicates : undefined).toBe(duplicates);
          expect(seenIds(state.seen)).toEqual(result.record.seen);
          record = result.record;
        }
      },
    ), { numRuns: 1000, seed: 2002 });
  }, 60_000);

  test('defers eviction until the entire batch has deduplicated', () => {
    const state = seenFrom(['a', 'b'], [], []);
    expect(seenAdmit(state, 'c')).toBe(true);
    expect(seenAdmit(state, 'a')).toBe(false);
    seenTrim(state, 2);
    expect(seenIds(state.seen)).toEqual(['b', 'c']);
    expect(Object.fromEntries(state.known)).toEqual({ b: 1, c: 2 });
    expect(state.known.has('a')).toBe(false);
    ringSlots(state);
  });

  test('counts snapshot ring slots, open entries and held entries', () => {
    const seen = Object.freeze(['a', 'b']);
    const state = seenFrom(seen, [{ id: 'c' }], [{ id: 'b' }]);
    expect(Object.fromEntries(state.known)).toEqual({ a: 1, b: 2, c: 1 });
    expect(seenIds(state.seen)).toEqual(['a', 'b']);
    const snapshot = seenIds(state.seen);
    snapshot.push('unrelated');
    expect(seenIds(state.seen)).toEqual(['a', 'b']);
    expect(state.seen.ids).not.toBe(seen);
  });

  test('deduplicates within a batch even when seenCount is zero', () => {
    const state = seenFrom([], [], []);
    expect(seenAdmit(state, 'x')).toBe(true);
    expect(seenAdmit(state, 'x')).toBe(false);
    seenTrim(state, 0);
    expect(seenIds(state.seen)).toEqual([]);
    expect(state.known.get('x')).toBe(1);
    seenRelease(state, 'x');
    expect(state.known.has('x')).toBe(false);
    ringSlots(state);
  });

  test('keeps the last 500 ids with capacity 512 across 10000 batches', () => {
    const state = seenFrom([], [], []);
    for (let i = 0; i < 10_000; i += 1) {
      const value = `id-${i}`;
      expect(seenAdmit(state, value)).toBe(true);
      seenRelease(state, value);
      seenTrim(state, 500);
    }
    expect(seenIds(state.seen)).toEqual(Array.from({ length: 500 }, (_, i) => `id-${9500 + i}`));
    expect(state.seen.capacity).toBe(512);
    expect(state.known.size).toBe(500);
    ringSlots(state);
  });

  test('counts repeated snapshot slots and clears the ring for a negative limit', () => {
    const state = seenFrom(['a', 'a'], [{ id: 'a' }], []);
    expect(state.known.get('a')).toBe(3);
    seenTrim(state, -1);
    expect(seenIds(state.seen)).toEqual([]);
    expect(state.known.get('a')).toBe(1);
    seenRelease(state, 'a');
    expect(state.known.size).toBe(0);
    ringSlots(state);
  });
});
