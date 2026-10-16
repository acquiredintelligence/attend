import { DatabaseSync } from 'node:sqlite';
import { expect, test } from 'vitest';
import { encodeCursor, decodeCursor, resolveStart } from '../../src/server/cursor.js';
import { EventsError } from '../../src/server/events.js';
import { memoryStore } from '../../src/store/memory.js';
import { sqliteStore } from '../../src/store/sqlite.js';
import type { DecisionAppend } from '../../src/core/types.js';
import type { ScopeStore } from '../../src/store/types.js';

async function commit(store: ScopeStore, id: string, at = Date.now()) {
  const version = (await store.load('s'))?.version ?? 0;
  const row: DecisionAppend = { kind: 'decision', id, scope: 's', at, route: 'interrupt', body: { id, scope: 's', at, route: 'interrupt', probabilities: { interrupt: 1, queue: 0, note: 0, ask: 0, skip: 0, wait: 0 }, predicates: {}, confidence: null, window: { id, events: [], reason: 'closing', settledAt: at, waits: 0, gap: false, dropped: 0 } } };
  const inputs = [{ input: { kind: 'flush' as const }, now: at }];
  expect(await store.commit([{ scope: 's', expectedVersion: version, inputs, appends: [row], snapshot: null, serialized: { inputs: inputs.map((input) => JSON.stringify(input)), snapshot: null, appends: [JSON.stringify(row.body)] } }])).toEqual(['ok']);
}

for (const position of ['1', '1760000000000-3', '123456.7', '']) test(`cursor round trip: ${position || 'empty'}`, () => {
  expect(encodeCursor({ epoch: 'e', position })).toBe(`v1.e.${position}`);
  expect(decodeCursor(`v1.e.${position}`)).toEqual({ epoch: 'e', position });
});
for (const cursor of ['garbage', 'v1', 'v1.', 'v1.e', 'v1..5', 'v2.e.5']) test(`malformed cursor: ${cursor}`, () => { expect(decodeCursor(cursor)).toBeNull(); });
test('an epoch must be nonempty and contain no dots', () => {
  for (const epoch of ['', 'has.dot']) expect(() => encodeCursor({ epoch, position: '1' })).toThrow(TypeError);
});

test('resolveStart starts at the head, handles empty logs, epochs and retention', async () => {
  const store = memoryStore();
  const epoch = await store.log.epoch();
  expect(await resolveStart(store.log, null, 0, Date.now())).toEqual({ epoch, after: null, truncated: false, fromNow: true });
  await commit(store, 'a');
  await commit(store, 'b');
  const head = (await store.log.bounds()).last!;
  expect(await resolveStart(store.log, undefined, 0, Date.now())).toEqual({ epoch, after: head, truncated: false, fromNow: true });
  expect(await resolveStart(store.log, `v1.${epoch}.${head}`, undefined, Date.now())).toEqual({ epoch, after: head, truncated: false, fromNow: false });
  expect(await resolveStart(store.log, 'v1.other.not-a-position', undefined, Date.now())).toEqual({ epoch, after: null, truncated: true, fromNow: false });
  expect(await resolveStart(store.log, `v1.${epoch}.`, undefined, Date.now())).toEqual({ epoch, after: null, truncated: false, fromNow: false });
  await store.log.trim({ maxRows: 1 });
  expect(await resolveStart(store.log, `v1.${epoch}.1`, undefined, Date.now())).toEqual({ epoch, after: null, truncated: true, fromNow: false });
});

test('resolveStart marks an old next row as truncated, but not a fresh next row', async () => {
  const store = memoryStore();
  const epoch = await store.log.epoch();
  await commit(store, 'old', Date.now() - 3_600_000);
  await commit(store, 'new');
  expect((await resolveStart(store.log, `v1.${epoch}.`, 60_000, Date.now())).truncated).toBe(true);
  expect((await resolveStart(store.log, `v1.${epoch}.1`, 60_000, Date.now())).truncated).toBe(false);
});

for (const position of ['abc', '-1', '1.5']) test(`memory malformed position is InvalidParams: ${position}`, async () => {
  const store = memoryStore();
  const pending = resolveStart(store.log, `v1.${await store.log.epoch()}.${position}`, undefined, Date.now());
  await expect(pending).rejects.toBeInstanceOf(EventsError);
  await expect(pending).rejects.toMatchObject({ code: -32602, data: { field: 'cursor' } });
});

test('SQLite plain compare errors become InvalidParams', async () => {
  const db = new DatabaseSync(':memory:');
  const store = sqliteStore(db);
  try {
    const pending = resolveStart(store.log, `v1.${await store.log.epoch()}.abc`, undefined, Date.now());
    await expect(pending).rejects.toBeInstanceOf(EventsError);
    await expect(pending).rejects.toMatchObject({ code: -32602, data: { field: 'cursor' } });
  } finally { await store.close?.(); db.close(); }
});
