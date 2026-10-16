import { DatabaseSync } from 'node:sqlite';
import { expect, test } from 'vitest';
import { encodeCursor, decodeCursor, resolveStart, lostBefore } from '../../src/server/cursor.js';
import { EventsError } from '../../src/server/events.js';
import { memoryStore } from '../../src/store/memory.js';
import { sqliteStore } from '../../src/store/sqlite.js';
import type { DecisionAppend } from '../../src/core/types.js';
import type { ScopeStore, DecisionLog } from '../../src/store/types.js';

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

test('resolveStart starts at the head or keeps the supplied position without store calls', async () => {
  const store = memoryStore();
  const epoch = await store.log.epoch();
  expect(await resolveStart(store.log, epoch, null)).toEqual({ after: null, truncated: false, fromNow: true });
  await commit(store, 'a');
  await commit(store, 'b');
  expect(await resolveStart(store.log, epoch, undefined)).toEqual({ after: '2', truncated: false, fromNow: true });
  const log: DecisionLog = { ...store.log, async bounds() { throw new Error('Unexpected bounds'); }, async read() { throw new Error('Unexpected read'); }, async epoch() { throw new Error('Unexpected epoch'); } };
  expect(await resolveStart(log, epoch, `v1.${epoch}.2`)).toEqual({ after: '2', truncated: false, fromNow: false });
  expect(await resolveStart(log, epoch, 'v1.other.not-a-position')).toEqual({ after: null, truncated: true, fromNow: false });
  expect(await resolveStart(log, epoch, `v1.${epoch}.`)).toEqual({ after: null, truncated: false, fromNow: false });
  await store.log.trim({ maxRows: 1 });
  expect(await resolveStart(log, epoch, `v1.${epoch}.1`)).toEqual({ after: '1', truncated: false, fromNow: false });
});

for (const cursor of ['garbage', 'v1.', 'v2.e.1']) test(`resolveStart rejects malformed cursor: ${cursor}`, async () => {
  await expect(resolveStart(memoryStore().log, 'e', cursor)).rejects.toMatchObject({ code: -32602, data: { field: 'cursor', reason: 'malformed' } });
});

function counted(log: DecisionLog) {
  const calls = { bounds: 0, read: 0 };
  return { calls, log: { ...log,
    async bounds() { calls.bounds++; return log.bounds(); },
    async read(query) { calls.read++; return log.read(query); },
  } satisfies DecisionLog };
}

test('lostBefore needs no store calls at the head or on an empty log', async () => {
  const store = memoryStore();
  const counter = counted(store.log);
  expect(await lostBefore(counter.log, null, await store.log.read({ after: null, limit: 1 }), 1000)).toBe(false);
  expect(counter.calls).toEqual({ bounds: 0, read: 0 });
  await commit(store, 'a', 1);
  expect(await lostBefore(counter.log, '1', await store.log.read({ after: '1', limit: 1 }), 1000)).toBe(false);
  expect(counter.calls).toEqual({ bounds: 0, read: 0 });
});

test('lostBefore detects a cursor below the retained floor', async () => {
  const store = memoryStore();
  await commit(store, 'a');
  await commit(store, 'b');
  await store.log.trim({ maxRows: 1 });
  const counter = counted(store.log);
  expect(await lostBefore(counter.log, '1', await store.log.read({ after: '1', limit: 1 }), undefined)).toBe(true);
  expect(counter.calls).toEqual({ bounds: 1, read: 0 });
});

for (const after of ['1', '2']) test(`lostBefore only checks bounds without an age floor after ${after}`, async () => {
  const store = memoryStore();
  for (const id of ['a', 'b', 'c']) await commit(store, id);
  const counter = counted(store.log);
  expect(await lostBefore(counter.log, after, await store.log.read({ after, limit: 10 }), undefined)).toBe(false);
  expect(counter.calls).toEqual({ bounds: 1, read: 0 });
});

for (const after of [null, '1']) for (const [at, expected] of [[100, true], [1000, false]] as const) test(`lostBefore checks the next row age after ${after} at ${at}`, async () => {
  const store = memoryStore();
  if (after !== null) await commit(store, 'first', 1);
  await commit(store, 'next', at);
  const counter = counted(store.log);
  expect(await lostBefore(counter.log, after, await store.log.read({ after, limit: 10, sinceAt: 500 }), 500)).toBe(expected);
  expect(counter.calls).toEqual({ bounds: after === null ? 0 : 1, read: 1 });
});

for (const position of ['abc', '-1', '1.5']) test(`memory malformed position is InvalidParams: ${position}`, async () => {
  const store = memoryStore();
  const pending = resolveStart(store.log, await store.log.epoch(), `v1.${await store.log.epoch()}.${position}`);
  await expect(pending).rejects.toBeInstanceOf(EventsError);
  await expect(pending).rejects.toMatchObject({ code: -32602, data: { field: 'cursor' } });
});

test('SQLite plain compare errors become InvalidParams', async () => {
  const db = new DatabaseSync(':memory:');
  const store = sqliteStore(db);
  try {
    const pending = resolveStart(store.log, await store.log.epoch(), `v1.${await store.log.epoch()}.abc`);
    await expect(pending).rejects.toBeInstanceOf(EventsError);
    await expect(pending).rejects.toMatchObject({ code: -32602, data: { field: 'cursor' } });
  } finally { await store.close?.(); db.close(); }
});
