import assert from 'node:assert/strict';
import fc from 'fast-check';
import { describe, expect, test } from 'vitest';
import { fnvHex } from '../src/core/ids.js';
import type { Buffer, Entry, Kind, SettlePolicy } from '../src/core/types.js';
import {
  assertWindow, deadline, estimateRenderedBytes, isAct, openWindow, shedEntries,
  windowAppend, windowCompact, windowDeadline, windowEntries, windowFrom, windowPrune, windowShed, windowSize,
} from '../src/core/window.js';
import { deadline as oracleDeadline, initialRecord, step, windowId as oracleWindowId } from './oracle/step.js';
import { DEFAULT_POLICY, KINDS } from './oracle/types.js';
import type { Buffer as OracleBuffer } from './oracle/types.js';
import { policyArbitrary } from './support/arbitraries.js';
import { deepFreeze } from './support/deep-freeze.js';

const time = fc.integer({ min: -1000, max: 5000 });
const entryArbitrary: fc.Arbitrary<Entry> = fc.record({
  id: fc.string({ maxLength: 12 }),
  at: time,
  name: fc.constantFrom('edit', 'é', '🌍', ''),
  data: fc.record({ n: fc.integer(), nested: fc.array(fc.constantFrom(true, null, 'é'), { maxLength: 4 }) }),
  text: fc.option(fc.string({ unit: 'grapheme', maxLength: 15 }), { nil: undefined }),
  kind: fc.constantFrom(...KINDS),
  seenAt: time,
}).map(({ text, ...entry }) => text === undefined ? entry : { ...entry, text });
const bufferArbitrary: fc.Arbitrary<Buffer> = fc.record({
  entries: fc.array(entryArbitrary, { maxLength: 30 }),
  dropped: fc.nat({ max: 50 }),
  gap: fc.boolean(),
  forcedAt: fc.option(time, { nil: null }),
});
// Time mostly moves forward, so windows stay monotone for a while before a backward step turns on the full prefix scan.
const dt = fc.oneof(
  { weight: 14, arbitrary: fc.integer({ min: 0, max: 30 }) },
  { weight: 5, arbitrary: fc.integer({ min: 0, max: 500 }) },
  { weight: 2, arbitrary: fc.integer({ min: -100, max: -1 }) },
);
// Mostly sheddable, so prefixes before the first act are long enough to prune and shed.
const sequenceKind = fc.oneof(
  { weight: 6, arbitrary: fc.constantFrom<Kind>('consequence', 'context') },
  { weight: 1, arbitrary: fc.constantFrom<Kind>('flush', 'closing', 'continuous', 'passive') },
);
const operation = fc.oneof(
  { weight: 7, arbitrary: fc.record({ kind: fc.constant('append'), entry: entryArbitrary, entryKind: sequenceKind, dt }) },
  { weight: 2, arbitrary: fc.record({ kind: fc.constant('prune'), ahead: fc.integer({ min: -200, max: 2000 }), maxWindowMs: fc.integer({ min: 0, max: 2000 }) }) },
  { weight: 2, arbitrary: fc.record({ kind: fc.constant('shed'), maxEvents: fc.integer({ min: 0, max: 30 }) }) },
  { weight: 1, arbitrary: fc.record({ kind: fc.constant('force'), at: fc.option(time, { nil: null }) }) },
  { weight: 1, arbitrary: fc.record({ kind: fc.constant('reset') }) },
);

function entry(id: string, kind: Kind = 'context', seenAt = 100): Entry {
  return { id, kind, seenAt, at: 0, name: 'edit', data: {} };
}

function isolate(buffer: OracleBuffer, policy: SettlePolicy, now = 0): { buffer: OracleBuffer; gone: Entry[] | null } {
  const result = step({ policy }, { ...initialRecord('scope'), buffer }, { kind: 'events', events: [] }, now);
  for (const effect of result.effects) {
    if (effect.trace.kind === 'dropped') return { buffer: result.record.buffer, gone: effect.trace.entries };
  }
  return { buffer: result.record.buffer, gone: null };
}

describe('open window', () => {
  test('matches oracle deadlines for random buffers and decreasing timestamps', () => {
    fc.assert(fc.property(bufferArbitrary, policyArbitrary, (buffer, policy) => {
      const before = structuredClone(buffer);
      deepFreeze(buffer);
      expect(deadline(policy, buffer)).toEqual(oracleDeadline(policy, buffer));
      expect(buffer).toEqual(before);
    }), { numRuns: 5000, seed: 3001 });
  }, 60_000);

  test('matches oracle prune and shed through operation sequences', () => {
    const removals = { monotone: 0, scanned: 0, shed: 0, offsetRemovals: 0, compactions: 0 };
    fc.assert(fc.property(
      fc.string({ maxLength: 10 }), policyArbitrary, fc.array(operation, { maxLength: 120, size: 'max' }),
      (scope, policy, operations) => {
        let open = openWindow(scope);
        let model: OracleBuffer = initialRecord(scope).buffer;
        let clock = 1000;
        for (const op of operations) {
          const start = open.start;
          const length = open.entries.length;
          let read = 0;
          let actual: Entry[] | null = null;
          let expected: Entry[] | null = null;
          switch (op.kind) {
            case 'append': {
              clock += op.dt;
              const appended = deepFreeze({ ...op.entry, kind: op.entryKind, seenAt: clock });
              windowAppend(open, appended, estimateRenderedBytes(appended));
              model = { ...model, entries: [...model.entries, appended] };
              break;
            }
            case 'prune': {
              const monotone = open.monotone;
              const end = open.summary.firstActIndex === -1 ? windowSize(open) : open.summary.firstActIndex;
              actual = windowPrune(open, op.maxWindowMs, clock + op.ahead);
              read = monotone ? model.entries.findIndex((e, i) => i < end && e.seenAt + op.maxWindowMs > clock + op.ahead) : -1;
              if (read === -1) read = end;
              const result = isolate(model, { ...DEFAULT_POLICY, maxEvents: Infinity, maxWindowMs: op.maxWindowMs }, clock + op.ahead);
              model = result.buffer;
              expected = result.gone;
              if (actual !== null) removals[monotone ? 'monotone' : 'scanned'] += 1;
              break;
            }
            case 'shed': {
              actual = windowShed(open, op.maxEvents);
              if (actual !== null) read = model.entries.lastIndexOf(actual[actual.length - 1]!) + 1;
              const result = isolate(model, { ...DEFAULT_POLICY, maxEvents: op.maxEvents, maxWindowMs: Infinity });
              model = result.buffer;
              expected = result.gone;
              if (actual !== null) removals.shed += 1;
              break;
            }
            case 'force': open.forcedAt = op.at; model = { ...model, forcedAt: op.at }; break;
            case 'reset': open = openWindow(scope); model = initialRecord(scope).buffer; break;
          }
          if (actual !== null) {
            if (start > 0) removals.offsetRemovals += 1;
            const movedPrefix = read - actual.length <= length - start - read;
            if (open.start === 0 && (start > 0 || movedPrefix)) removals.compactions += 1;
          }
          assertWindow(scope, open);
          assert.deepEqual({ entries: windowEntries(open), dropped: open.dropped, gone: actual, due: windowDeadline(policy, open) }, {
            entries: model.entries, dropped: model.dropped, gone: expected, due: oracleDeadline(policy, model),
          });
          if (open.hashValid) assert.equal(fnvHex(open.hash), oracleWindowId(scope, model.entries.map((value) => value.id)));
        }
        assert.deepEqual(windowEntries(open), model.entries);
      },
    ), { numRuns: 1000, seed: 3002 });
    console.log(JSON.stringify({ removals }));
    expect(Math.min(removals.monotone, removals.scanned, removals.shed)).toBeGreaterThanOrEqual(500);
    expect(removals.offsetRemovals).toBeGreaterThanOrEqual(110);
    expect(removals.compactions).toBeGreaterThanOrEqual(1000);
  }, 60_000);

  test('sheds plain lists like the oracle without touching lists that fit', () => {
    fc.assert(fc.property(fc.array(entryArbitrary, { maxLength: 40 }), fc.integer({ min: 0, max: 30 }), (entries, maxEvents) => {
      const model = isolate({ entries, dropped: 0, gap: false, forcedAt: null }, { ...DEFAULT_POLICY, maxEvents, maxWindowMs: Infinity });
      for (const value of entries) deepFreeze(value);
      if (entries.length <= maxEvents) deepFreeze(entries);
      const gone = shedEntries(entries, maxEvents);
      expect({ entries, gone }).toEqual({ entries: model.buffer.entries, gone: model.gone });
    }), { numRuns: 2000, seed: 3003 });
  }, 60_000);

  test('restores snapshot buffers into independent arrays without mutating them', () => {
    fc.assert(fc.property(bufferArbitrary, fc.option(time, { nil: undefined }), (buffer, endedAt) => {
      if (endedAt !== undefined) buffer.endedAt = endedAt;
      const before = structuredClone(buffer);
      deepFreeze(buffer);
      const open = windowFrom('scope', buffer);
      assertWindow('scope', open);
      expect(open.start).toBe(0);
      expect(open.entries).toEqual(buffer.entries);
      expect(open.entries).not.toBe(buffer.entries);
      for (let i = 0; i < buffer.entries.length; i += 1) expect(open.entries[i]).toBe(buffer.entries[i]);
      expect(open.sizes).toEqual(buffer.entries.map(estimateRenderedBytes));
      expect({ dropped: open.dropped, gap: open.gap, forcedAt: open.forcedAt, endedAt: open.endedAt }).toEqual({
        dropped: buffer.dropped, gap: buffer.gap, forcedAt: buffer.forcedAt, endedAt: buffer.endedAt ?? null,
      });
      expect(buffer).toEqual(before);
    }), { numRuns: 1000, seed: 3004 });
  }, 60_000);

  test('classifies only closing, continuous and passive entries as acts', () => {
    expect(KINDS.map((kind) => isAct({ kind }))).toEqual([true, false, true, true, false, false]);
  });

  test('estimates UTF-8 name and payload bytes with text taking precedence', () => {
    expect(estimateRenderedBytes({ id: 'a', at: 0, name: 'edit', text: 'hello', data: {} })).toBe(49);
    expect(estimateRenderedBytes({ id: 'b', at: 0, name: 'é', data: { a: 1 } })).toBe(49);
    expect(estimateRenderedBytes({ id: 'c', at: 0, name: '🌍', text: '', data: { big: 'x'.repeat(100) } })).toBe(44);
  });

  test('settles an ended window at end time without closing grace', () => {
    const open = openWindow('scope');
    windowAppend(open, entry('a', 'closing', 100), 44);
    open.endedAt = 150;
    expect(windowDeadline(DEFAULT_POLICY, open)).toEqual({ at: 150, reason: 'closing' });
    expect(deadline(DEFAULT_POLICY, { entries: windowEntries(open), dropped: 0, gap: false, forcedAt: null, endedAt: 150 }))
      .toEqual({ at: 150, reason: 'closing' });
  });

  test('adds a size deadline exactly at the byte budget and leaves an absent budget off', () => {
    const open = openWindow('scope');
    windowAppend(open, entry('a', 'context', 100), 99);
    const policy = { ...DEFAULT_POLICY, maxWindowBytes: 100 };
    expect(windowDeadline(policy, open)).toBeNull();
    windowAppend(open, entry('b', 'context', 200), 1);
    expect(windowDeadline(policy, open)).toEqual({ at: 200, reason: 'size' });
    expect(windowDeadline(DEFAULT_POLICY, open)).toEqual(oracleDeadline(DEFAULT_POLICY, { entries: windowEntries(open), dropped: 0, gap: false, forcedAt: null }));
    const buffer = { entries: [entry('c', 'context', 300)], dropped: 0, gap: false, forcedAt: null };
    expect(deadline({ ...DEFAULT_POLICY, maxWindowBytes: 46 }, deepFreeze(buffer))).toEqual({ at: 300, reason: 'size' });
  });

  test('keeps forced before end and overflow before size on ties', () => {
    const open = openWindow('scope');
    open.forcedAt = 100;
    open.endedAt = 100;
    expect(windowDeadline(DEFAULT_POLICY, open)).toEqual({ at: 100, reason: 'forced' });
    open.forcedAt = null;
    open.endedAt = null;
    windowAppend(open, entry('a', 'context', 200), 100);
    expect(windowDeadline({ ...DEFAULT_POLICY, maxEvents: 0, maxWindowBytes: 100 }, open)).toEqual({ at: 200, reason: 'overflow' });
  });

  test('keeps oracle candidate order when all existing triggers tie', () => {
    const open = openWindow('scope');
    for (const kind of KINDS) windowAppend(open, entry(kind, kind, 100), 1);
    const policy = { ...DEFAULT_POLICY, closingGraceMs: 0, streamGapMs: 0, passiveQuietMs: 0, maxWindowMs: 0 };
    expect(windowDeadline(policy, open)).toEqual({ at: 100, reason: 'flush' });
    expect(windowDeadline({ ...policy, maxEvents: 0 }, open)).toEqual({ at: 100, reason: 'overflow' });
  });

  test('starts with the scope hash and folds appended ids', () => {
    const open = openWindow('scope');
    expect(fnvHex(open.hash)).toBe(oracleWindowId('scope', []));
    windowAppend(open, entry('a'), 44);
    expect(fnvHex(open.hash)).toBe('cf8923dfae84f930');
    expect(open.hashValid).toBe(true);
    assertWindow('scope', open);
  });

  test.each(['prune', 'shed'] as const)('invalidates the hash after %s and skips later folds', (operation) => {
    const open = openWindow('scope');
    windowAppend(open, entry('a'), 44);
    const before = fnvHex(open.hash);
    const gone = operation === 'prune' ? windowPrune(open, 0, 100) : windowShed(open, 0);
    expect(gone).toEqual([entry('a')]);
    expect(open.hashValid).toBe(false);
    windowAppend(open, entry('b'), 44);
    expect(fnvHex(open.hash)).toBe(before);
    expect(open.hashValid).toBe(false);
    assertWindow('scope', open);
  });

  test('prunes the whole prefix when timestamps decrease and keeps monotone false', () => {
    const open = openWindow('scope');
    windowAppend(open, entry('a', 'consequence', 500), 45);
    windowAppend(open, entry('b', 'consequence', 100), 46);
    windowAppend(open, entry('c', 'closing', 600), 47);
    expect(open.monotone).toBe(false);
    expect(windowPrune(open, 1000, 1100)).toEqual([entry('b', 'consequence', 100)]);
    expect(windowEntries(open)).toEqual([entry('a', 'consequence', 500), entry('c', 'closing', 600)]);
    expect(open.monotone).toBe(false);
    expect(open.bytes).toBe(92);
    assertWindow('scope', open);
  });

  test('preserves flushes and post-act context while compacting a monotone prefix', () => {
    const open = openWindow('scope');
    const entries = [entry('a', 'context', 10), entry('f', 'flush', 20), entry('b', 'consequence', 30), entry('c', 'closing', 40), entry('d', 'context', 50)];
    for (const value of entries) windowAppend(open, deepFreeze(value), 10);
    expect(windowPrune(open, 10, 100)).toEqual([entries[0], entries[2]]);
    expect(windowEntries(open)).toEqual([entries[1], entries[3], entries[4]]);
    expect(open.summary.firstActIndex).toBe(1);
    expect(open.bytes).toBe(30);
    expect(open.dropped).toBe(2);
    assertWindow('scope', open);
  });

  test('leaves the window untouched when pruning with NaN now or before expiry', () => {
    const open = openWindow('scope');
    windowAppend(open, entry('a'), 44);
    const before = structuredClone(open);
    expect(windowPrune(open, 10, 109)).toBeNull();
    expect(windowPrune(open, 10, NaN)).toBeNull();
    expect(open).toEqual(before);
    expect(windowShed(open, 1)).toBeNull();
    expect(open).toEqual(before);
  });

  test('uses addition at fractional prune boundaries like the oracle', () => {
    const open = openWindow('scope');
    windowAppend(open, entry('a', 'context', 0.1), 44);
    expect(windowPrune(open, 0.2, 0.3)).toBeNull();
    expect(windowPrune(open, 0.2, 0.1 + 0.2)).toEqual([entry('a', 'context', 0.1)]);
    assertWindow('scope', open);
    // 0.1 + 0.7 <= 0.1 + 0.7, but 0.1 > (0.1 + 0.7) - 0.7: only the oracle's addition prunes it.
    const late = openWindow('scope');
    windowAppend(late, entry('b', 'context', 0.1), 44);
    const oracle = isolate({ entries: [entry('b', 'context', 0.1)], dropped: 0, gap: false, forcedAt: null }, { ...DEFAULT_POLICY, maxEvents: Infinity, maxWindowMs: 0.7 }, 0.1 + 0.7);
    expect(oracle.gone).toEqual([entry('b', 'context', 0.1)]);
    expect(windowPrune(late, 0.7, 0.1 + 0.7)).toEqual([entry('b', 'context', 0.1)]);
  });

  test('sheds around acts and flushes and updates the last timestamp', () => {
    const open = openWindow('scope');
    const values = [entry('a', 'context', 10), entry('b', 'closing', 20), entry('c', 'flush', 30), entry('d', 'consequence', 40)];
    for (const value of values) windowAppend(open, value, 10);
    expect(windowShed(open, 0)).toEqual([values[0], values[3]]);
    expect(windowEntries(open)).toEqual([values[1], values[2]]);
    expect(open.summary.firstActIndex).toBe(0);
    expect(open.summary.lastSeenAt).toBe(30);
    expect(open.bytes).toBe(20);
    expect(windowShed(open, 0)).toBeNull();
    assertWindow('scope', open);
  });

  test('advances the prune offset and compacts once the dead prefix passes half the live entries', () => {
    const open = openWindow('scope');
    const values = ['a', 'b', 'c', 'd', 'e', 'f'].map((id, i) => entry(id, 'consequence', 10 + i * 10));
    for (const value of values) windowAppend(open, value, 10);
    const layout = [{ start: 1, length: 6 }, { start: 2, length: 6 }, { start: 0, length: 3 }, { start: 1, length: 3 }, { start: 0, length: 1 }, { start: 0, length: 0 }];
    for (let i = 0; i < values.length; i += 1) {
      assert.deepEqual(windowPrune(open, 10, 20 + i * 10), [values[i]]);
      assert.deepEqual({ start: open.start, length: open.entries.length, entries: windowEntries(open), size: windowSize(open),
        bytes: open.bytes, lastSeenAt: open.summary.lastSeenAt, dropped: open.dropped },
      { ...layout[i], entries: values.slice(i + 1), size: 5 - i, bytes: 10 * (5 - i), lastSeenAt: i === 5 ? NaN : 60, dropped: i + 1 });
      assertWindow('scope', open);
    }
  });

  test('sheds after an act with an offset and compacts the owned arrays in place', () => {
    const open = openWindow('scope');
    const act = entry('act', 'closing', 10);
    const c1 = entry('c1', 'consequence', 20);
    const c2 = entry('c2', 'consequence', 30);
    const c3 = entry('c3', 'consequence', 40);
    const c4 = entry('c4', 'consequence', 50);
    const c5 = entry('c5', 'consequence', 60);
    for (const value of [act, c1, c2, c3]) windowAppend(open, value, 10);
    const entries = open.entries;
    const sizes = open.sizes;
    assert.deepEqual(windowShed(open, 3), [c1]);
    assert.deepEqual({ start: open.start, length: entries.length, live: windowEntries(open), firstActIndex: open.summary.firstActIndex },
      { start: 1, length: 4, live: [act, c2, c3], firstActIndex: 0 });
    assertWindow('scope', open);
    assert.equal(windowCompact(open), entries);
    assert.deepEqual({ start: open.start, entries, sizes }, { start: 0, entries: [act, c2, c3], sizes: [10, 10, 10] });
    assert.equal(windowCompact(open), entries);
    assert.deepEqual({ start: open.start, entries, sizes }, { start: 0, entries: [act, c2, c3], sizes: [10, 10, 10] });
    assertWindow('scope', open);
    windowAppend(open, c4, 10);
    assert.deepEqual(windowShed(open, 3), [c2]);
    assert.deepEqual({ start: open.start, length: entries.length, live: windowEntries(open) }, { start: 1, length: 4, live: [act, c3, c4] });
    assertWindow('scope', open);
    windowAppend(open, c5, 10);
    assert.deepEqual(windowShed(open, 3), [c3]);
    assert.deepEqual({ start: open.start, entries, sizes, firstActIndex: open.summary.firstActIndex, bytes: open.bytes },
      { start: 0, entries: [act, c4, c5], sizes: [10, 10, 10], firstActIndex: 0, bytes: 30 });
    assert.equal(open.entries, entries);
    assertWindow('scope', open);
  });

  test('reports I3 when a trigger summary is corrupted', () => {
    const open = openWindow('scope');
    open.summary.lastClosingAt = 123;
    expect(() => assertWindow('scope', open)).toThrow('I3');
  });

  test('reports I4 when the byte sum or parallel array is corrupted', () => {
    const open = openWindow('scope');
    open.bytes += 1;
    expect(() => assertWindow('scope', open)).toThrow('I4');
    open.bytes = 0;
    open.sizes.push(0);
    expect(() => assertWindow('scope', open)).toThrow('I4');
  });

  test.each([-1, 0.5, 2])('reports I4: start for an invalid offset %s', (start) => {
    const open = openWindow('scope');
    for (const id of ['a', 'b', 'c', 'd']) windowAppend(open, entry(id), 10);
    open.start = start;
    assert.throws(() => assertWindow('scope', open), /^Error: I4: start/);
  });

  test('reports I5 when a valid hash is corrupted', () => {
    const open = openWindow('scope');
    open.hash.lo ^= 1;
    expect(() => assertWindow('scope', open)).toThrow('I5');
  });

  test('reports a false monotone promise', () => {
    const open = openWindow('scope');
    windowAppend(open, entry('a', 'context', 100), 44);
    windowAppend(open, entry('b', 'context', 50), 44);
    open.monotone = true;
    expect(() => assertWindow('scope', open)).toThrow('monotone');
  });
});
