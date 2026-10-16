import type { ConformanceCase } from '../testing.js';
import { assertEqual, assertOk } from '../internal/assert.js';
import type { Decision, DecisionAppend, DeliveredRoute, Entry, LoggedInput, ScopeRecord, SettlePolicy, Window } from '../core/types.js';
import type { DecisionQuery, DecisionRow, Position, ScopeStore, ScopeWrite, Snapshot } from '../store/types.js';

const policy: SettlePolicy = {
  closingGraceMs: 500, streamGapMs: 1000, passiveQuietMs: 4000, waitHorizonMs: 10_000,
  maxWaits: 2, maxEvents: 250, maxWindowMs: 60_000, retryBackoffMs: [1000, 4000, 16_000],
  judgeTimeoutMs: 15_000, deliveryTimeoutMs: 10_000, recentCount: 8, seenCount: 1000,
  maxOutbox: 1000, maxDeadLetters: 1000, deadLetterBackoffMaxMs: 60_000,
};

function logged(id: string, now = 1000): LoggedInput {
  return { input: { kind: 'events', events: [{ id, at: now, name: 'changed', data: { id }, kind: 'closing' }] }, now };
}

function window(id: string): Window {
  return {
    id, events: [{ id: `event:${id}`, at: 1, name: 'changed', data: { nested: [null, true, 'héllo 🌍\u0000'] }, text: 'héllo 🌍\u0000', kind: 'closing', seenAt: 2 }],
    reason: 'closing', waits: 0, dropped: 0, gap: false, settledAt: 3,
  };
}

function decision(id: string, scope: string, route: DeliveredRoute = 'interrupt', at = 1000): Decision {
  return {
    id, scope, route, at, window: window(id), confidence: 0.8, predicates: { relevant: 0.7 },
    probabilities: { interrupt: 0.4, queue: 0.2, note: 0.1, ask: 0.1, skip: 0.1, wait: 0.1 },
  };
}

function append(id: string, scope = 's', route: DeliveredRoute = 'interrupt', at = 1000): DecisionAppend {
  return { kind: 'decision', id, scope, route, at, body: decision(id, scope, route, at) };
}

function letter(id: string, scope = 's', stage: 'judge' | 'sink' = 'judge', at = 1000): DecisionAppend {
  const failed = stage === 'sink' ? decision(id, scope, 'interrupt', at) : null;
  return {
    kind: 'deadLetter', id, scope, route: failed?.route ?? null, at,
    body: { id, scope, stage, window: window(id), decision: failed, message: 'could not deliver', permanent: false, attempts: 3, at },
  };
}

function record(scope: string, version: number): ScopeRecord {
  return {
    scope, version, cursors: {}, seen: [], buffer: { entries: [], dropped: 0, gap: false, forcedAt: null },
    batch: null, outbox: { decisions: [], deadLetters: [] }, recent: [], dueAt: null,
  };
}

function write(scope: string, expectedVersion = 0, options: Partial<Pick<ScopeWrite, 'inputs' | 'appends' | 'snapshot' | 'due'>> = {}): ScopeWrite {
  const result: ScopeWrite = {
    scope, expectedVersion, inputs: [logged(`${scope}:${expectedVersion}`)], appends: [], snapshot: null, ...options,
  };
  result.serialized = {
    inputs: result.inputs.map((input) => JSON.stringify(input)),
    snapshot: result.snapshot === null ? null : JSON.stringify(result.snapshot.record),
    appends: result.appends.map((row) => JSON.stringify(row.body)),
  };
  return result;
}

async function rejects(run: () => Promise<unknown>): Promise<void> {
  try {
    await run();
  } catch {
    return;
  }
  throw new Error('expected rejection');
}

type StoreCase = [string, (store: ScopeStore) => Promise<void>];

/**
 * The store under test, with every value its reads return, and every write it was given, kept beside a
 * structuredClone. After a case closes the store and yields one macrotask, none of them may have changed,
 * so each read shape any case uses also checks aliasing. A store must return plain, cloneable data.
 */
function watched(inner: ScopeStore): { store: ScopeStore; check: () => void } {
  const kept: [unknown, unknown][] = [];
  const keep = async <T>(pending: Promise<T>): Promise<T> => {
    const value = await pending;
    kept.push([value, structuredClone(value)]);
    return value;
  };
  const log = inner.log;
  const store: ScopeStore = {
    durable: inner.durable,
    load: (scope) => keep(inner.load(scope)),
    commit: (writes) => {
      kept.push([writes, structuredClone(writes)]);
      return keep(inner.commit(writes));
    },
    due: (now, limit, after) => keep(inner.due(now, limit, after)),
    remove: (scope) => inner.remove(scope),
    log: {
      epoch: () => log.epoch(),
      bounds: () => keep(log.bounds()),
      read: (query) => keep(log.read(query)),
      find: (kind, id) => keep(log.find(kind, id)),
      wait: (after, blockMs, signal) => log.wait(after, blockMs, signal),
      trim: (retention) => log.trim(retention),
      compare: (a, b) => log.compare(a, b),
    },
    ...(inner.close ? { close: () => inner.close?.() ?? Promise.resolve() } : {}),
  };
  return { store, check: () => kept.forEach(([value, clone], i) => assertEqual(value, clone, `returned value ${i} unchanged by later calls`)) };
}

function casesFor(make: () => Promise<ScopeStore> | ScopeStore, cases: StoreCase[]): ConformanceCase[] {
  return cases.map(([name, run]) => ({
    name,
    async run() {
      const { store, check } = watched(await make());
      let closed = false;
      try {
        await run(store);
        closed = true;
        await store.close?.();
        await new Promise((resolve) => setTimeout(resolve, 0));
        check();
      } finally {
        if (!closed) await store.close?.();
      }
    },
  }));
}

/** Every guarantee in ScopeStore's doc comments, as cases. `make` returns a fresh, empty store per case. */
export function scopeStoreConformance(make: () => Promise<ScopeStore> | ScopeStore): ConformanceCase[] {
  return casesFor(make, [
    ['load of a missing scope is null', async (store) => {
      assertEqual(await store.load('missing'), null, 'load');
    }],
    ['a commit at version 0 creates the scope and load returns its inputs in order', async (store) => {
      const inputs: LoggedInput[] = [logged('a'), logged('b'), logged('c'), { input: { kind: 'flush' }, now: 1001 }];
      assertEqual(await store.commit([write('s', 0, { inputs })]), ['ok'], 'created');
      assertEqual(await store.load('s'), { version: 4, snapshot: null, tail: inputs }, 'ordered inputs');
    }],
    ['the stored version counts committed inputs', async (store) => {
      const first = [logged('a'), logged('b'), logged('c')];
      const second = [logged('d'), logged('e')];
      assertEqual(await store.commit([write('s', 0, { inputs: first })]), ['ok'], 'first commit');
      assertEqual(await store.commit([write('s', 3, { inputs: second })]), ['ok'], 'second commit');
      assertEqual(await store.load('s'), { version: 5, snapshot: null, tail: [...first, ...second] }, 'version');
    }],
    ['a write at a stale version conflicts and changes nothing', async (store) => {
      await store.commit([write('s')]);
      await store.commit([write('s', 1)]);
      const before = await store.load('s');
      assertEqual(await store.commit([write('s', 1, { appends: [append('stale')], due: 1000 })]), ['conflict'], 'stale');
      assertEqual(await store.load('s'), before, 'unchanged');
      assertEqual(await store.log.find('decision', 'stale'), null, 'no stale append');
      assertEqual(await store.due(1000, 10), [], 'no stale due');
    }],
    ['a write at version 0 on an existing scope conflicts', async (store) => {
      await store.commit([write('s')]);
      const before = await store.load('s');
      assertEqual(await store.commit([write('s')]), ['conflict'], 'create twice');
      assertEqual(await store.load('s'), before, 'unchanged');
    }],
    ['N concurrent commits at one version give exactly one ok', async (store) => {
      const first = logged('first');
      await store.commit([write('s', 0, { inputs: [first] })]);
      const inputs = Array.from({ length: 20 }, (_, i) => logged(`contender:${i}`));
      const outcomes = await Promise.all(inputs.map((input) => store.commit([write('s', 1, { inputs: [input] })])));
      assertEqual(outcomes.filter(([result]) => result === 'ok').length, 1, 'exactly one winner');
      const winner = inputs[outcomes.findIndex(([result]) => result === 'ok')];
      assertOk(winner, 'winner exists');
      assertEqual(await store.load('s'), { version: 2, snapshot: null, tail: [first, winner] }, 'winner persisted');
    }],
    ['a conflicting write in a group leaves the other writes committed, with outcomes in order', async (store) => {
      await store.commit([write('a'), write('b')]);
      const before = await store.load('b');
      const a = write('a', 1);
      const c = write('c');
      assertEqual(await store.commit([a, write('b', 0, { appends: [append('conflict', 'b')] }), c]), ['ok', 'conflict', 'ok'], 'outcomes');
      assertEqual((await store.load('a'))?.version, 2, 'a committed');
      assertEqual(await store.load('b'), before, 'b unchanged');
      assertEqual(await store.load('c'), { version: 1, snapshot: null, tail: c.inputs }, 'c committed');
      assertEqual(await store.log.find('decision', 'conflict'), null, 'no conflicting append');
    }],
    ['a group naming one scope twice rejects and applies nothing', async (store) => {
      await rejects(() => store.commit([
        write('a', 0, { appends: [append('a', 'a')], due: 1000 }),
        write('b', 0, { appends: [append('b', 'b')], due: 2000 }), write('b'),
      ]));
      assertEqual(await store.load('a'), null, 'a absent');
      assertEqual(await store.load('b'), null, 'b absent');
      assertEqual(await store.log.read({ after: null, limit: 10 }), { rows: [], checked: null, more: false }, 'no rows');
      assertEqual(await store.due(3000, 10), [], 'no due values');
    }],
    ['a snapshot replaces the stored one and trims the tail', async (store) => {
      await store.commit([write('s', 0, { inputs: [logged('a'), logged('b'), logged('c')] })]);
      const snapshot: Snapshot = { version: 5, record: { ...record('s', 5), seen: ['d', 'e'] }, policy, reducer: 2 };
      await store.commit([write('s', 3, { inputs: [logged('d'), logged('e')], snapshot })]);
      assertEqual(await store.load('s'), { version: 5, snapshot, tail: [] }, 'snapshot replaced');
      const input = logged('f');
      await store.commit([write('s', 5, { inputs: [input] })]);
      assertEqual(await store.load('s'), { version: 6, snapshot, tail: [input] }, 'tail after snapshot');
      const replacement: Snapshot = { version: 7, record: record('s', 7), policy, reducer: 3 };
      await store.commit([write('s', 6, { snapshot: replacement })]);
      assertEqual(await store.load('s'), { version: 7, snapshot: replacement, tail: [] }, 'older snapshot replaced');
    }],
    ['a snapshot and its tail round-trip with unicode, a NUL, a 250-entry window and a 1,000-id ring', async (store) => {
      const entries: Entry[] = Array.from({ length: 250 }, (_, i) => ({ id: `e${i}`, at: i, seenAt: i + 1, name: 'n', text: 'héllo 🌍\u0000', data: { i, nested: [null, { yes: true }] }, kind: 'passive' }));
      const d = decision('delivered', 's');
      const l = letter('failed', 's');
      if (l.kind !== 'deadLetter') throw new Error('expected dead letter');
      const snapshot: Snapshot = {
        version: 1, policy, reducer: 1,
        record: {
          ...record('s', 1), cursors: { 'log:a': '🌍\u0000', 'mcp:b': null }, seen: Array.from({ length: 1000 }, (_, i) => `seen:${i}`),
          buffer: { entries, dropped: 4, gap: true, forcedAt: 12, endedAt: 13 },
          batch: { window: window('held'), phase: { phase: 'judging', attempt: 2, failures: 1, leaseUntil: 5000, offerWait: true } },
          outbox: {
            decisions: [{ item: { id: d.id, kind: 'decision', decision: d }, phase: { phase: 'inFlight', attempt: 1, failures: 0, leaseUntil: 4000 } }],
            deadLetters: [{ item: { id: l.id, kind: 'deadLetter', letter: l.body }, phase: { phase: 'retrying', attempt: 2, failures: 1, at: 3000 } }],
          },
          recent: [{ id: 'prior', route: 'note', at: 1 }], dueAt: 3000,
        },
      };
      await store.commit([write('s', 0, { snapshot })]);
      const tail = [logged('tail:🌍'), logged('tail:\u0000')];
      await store.commit([write('s', 1, { inputs: tail })]);
      assertEqual(await store.load('s'), { version: 3, snapshot, tail }, 'large round trip');
    }],
    ['due returns scopes due by now, ascending by due then scope, paged after a key', async (store) => {
      await store.commit([
        write('a', 0, { due: 3000 }), write('b', 0, { due: 1000 }), write('c', 0, { due: 2000 }),
        write('d', 0, { due: 9000 }), write('e', 0, { due: 1000 }), write('f', 0, { due: null }),
      ]);
      const expected = [{ due: 1000, scope: 'b' }, { due: 1000, scope: 'e' }, { due: 2000, scope: 'c' }, { due: 3000, scope: 'a' }];
      assertEqual(await store.due(3000, 10), expected, 'due order');
      assertEqual(await store.due(3000, 10), expected, 'read does not claim');
      assertEqual(await store.due(3000, 2), expected.slice(0, 2), 'limit');
      assertEqual(await store.due(3000, 10, { due: 1000, scope: 'b' }), expected.slice(1), 'inside tie');
      assertEqual(await store.due(3000, 10, { due: 1000, scope: 'e' }), expected.slice(2), 'after tie');
      assertEqual(await store.due(999, 10), [], 'none yet');
    }],
    ['an omitted due keeps the stored value and a null due clears it', async (store) => {
      await store.commit([write('s', 0, { due: 2000 })]);
      await store.commit([write('s', 1)]);
      assertEqual(await store.due(2000, 10), [{ due: 2000, scope: 's' }], 'omitted keeps');
      await store.commit([write('s', 2, { due: null })]);
      assertEqual(await store.due(2000, 10), [], 'null clears');
    }],
    ['remove forgets the scope, its due value and its version', async (store) => {
      await store.commit([write('s', 0, { due: 1000 })]);
      await store.remove('s');
      assertEqual(await store.load('s'), null, 'removed');
      assertEqual(await store.due(2000, 10), [], 'not due');
      assertEqual(await store.commit([write('s')]), ['ok'], 'recreated');
    }],
    ['scopes that are prefixes of each other or hold punctuation stay separate', async (store) => {
      const scopes = ['a', 'a/b', 'a:b', 'a%b', 'a_b', 'ünï', 'a/b/c'];
      for (const [i, scope] of scopes.entries()) await store.commit([write(scope, 0, { due: (i + 1) * 1000 })]);
      for (const scope of scopes) assertEqual(await store.load(scope), { version: 1, snapshot: null, tail: [logged(`${scope}:0`)] }, scope);
      assertEqual((await store.due(10_000, 10)).map((key) => key.scope), scopes, 'separate due values');
    }],
    ['what a read returned is unchanged by later commits, trims and removes', async (store) => {
      const kept: [unknown, unknown][] = [];
      const keep = async <T>(pending: Promise<T>): Promise<T> => {
        const value = await pending;
        kept.push([value, structuredClone(value)]);
        return value;
      };
      const first: Snapshot = { version: 2, record: record('s', 2), policy, reducer: 1 };
      await store.commit([write('s', 0, { inputs: [logged('a'), logged('b')], snapshot: first, appends: [append('one')], due: 1000 }), write('t', 0), write('u', 0, { due: 1000 })]);
      await keep(store.load('s'));
      await keep(store.load('t'));
      await store.commit([write('s', 2, { appends: [letter('two')] })]);
      await keep(store.load('s'));
      const bounds = await keep(store.log.bounds());
      for (const query of [{}, { scope: 's' }, { scopePrefix: 's' }, { kinds: ['deadLetter' as const] }]) await keep(store.log.read({ after: null, limit: 10, ...query }));
      await keep(store.log.read({ after: bounds.first, limit: 10 }));
      await keep(store.log.read({ after: bounds.last, limit: 10 }));
      await keep(store.log.read({ after: null, limit: 10, routes: ['interrupt'] }));
      await keep(store.log.read({ after: null, limit: 10, sinceAt: 0 }));
      await keep(store.log.read({ after: bounds.first, limit: 10, scope: 's' }));
      const page = await keep(store.log.read({ after: null, limit: 1 }));
      const [firstDue] = await keep(store.due(1000, 1));
      const [secondDue] = await keep(store.due(1000, 1, firstDue));
      await keep(store.due(1000, 1, secondDue));
      await keep(store.log.find('decision', 'one'));
      await keep(store.due(1000, 10));
      assertEqual(await store.commit([write('s', 3, { inputs: [logged('c')], appends: [append('one', 's', 'queue', 2000), letter('two', 's', 'sink', 2000)], due: 1500 }), write('t', 1)]), ['ok', 'ok'], 'duplicate appends commit');
      assertEqual(await store.commit([write('s', 4, { inputs: [logged('d')], appends: [append('three')], due: 2000 })]), ['ok'], 'later commit');
      await keep(store.log.find('decision', 'three'));
      await keep(store.log.read({ after: page.checked, limit: 1 }));
      await keep(store.log.find('deadLetter', 'two'));
      await keep(store.log.bounds());
      await keep(store.due(2000, 10));
      await store.log.trim({ maxRows: 1 });
      assertEqual(await store.commit([write('s', 5, { snapshot: { version: 6, record: record('s', 6), policy, reducer: 1 }, appends: [append('four'), letter('five'), append('one', 's', 'note', 3000)], due: null })]), ['ok'], 'snapshot commit');
      await keep(store.log.read({ after: null, limit: 10 }));
      await keep(store.log.find('decision', 'one'));
      await keep(store.log.bounds());
      await keep(store.due(3000, 10));
      await keep(store.load('s'));
      await store.log.trim({ olderThanMs: 1 });
      await store.remove('s');
      assertEqual(await store.commit([write('s', 0, { snapshot: { version: 1, record: record('s', 1), policy, reducer: 2 }, appends: [append('six')] })]), ['ok'], 're-created');
      await store.remove('t');
      for (const [value, clone] of kept) assertEqual(value, clone, 'returned values unchanged');
    }],
  ]);
}

async function seed(store: ScopeStore, rows: DecisionAppend[]): Promise<void> {
  for (const row of rows) {
    const version = (await store.load(row.scope))?.version ?? 0;
    assertEqual(await store.commit([write(row.scope, version, { appends: [row] })]), ['ok'], 'seed');
  }
}

function ids(rows: readonly DecisionRow[]): string[] {
  return rows.map((row) => row.id);
}

function position(rows: readonly DecisionRow[], index: number): Position {
  const row = rows[index];
  if (!row) throw new Error(`missing row ${index}`);
  return row.position;
}

async function within(pending: Promise<void>, ms: number): Promise<void> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([pending, new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error(`wait exceeded ${ms}ms`)), ms); })]);
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Every guarantee in DecisionLog's doc comments. With `concurrent`, which returns two stores over
 * one database, it also checks that a reader never skips a row while two writers commit at once,
 * and that durable stores share an epoch.
 */
export function decisionLogConformance(make: () => Promise<ScopeStore> | ScopeStore, options?: { concurrent?: () => Promise<[ScopeStore, ScopeStore]> }): ConformanceCase[] {
  const cases: StoreCase[] = [
    ['an empty log has no bounds and reads nothing', async (store) => {
      assertEqual(await store.log.bounds(), { first: null, last: null }, 'empty bounds');
      assertEqual(await store.log.read({ after: null, limit: 10 }), { rows: [], checked: null, more: false }, 'empty page');
    }],
    ['rows appear only when their write commits', async (store) => {
      await store.commit([write('s', 0, { appends: [append('committed')] })]);
      assertEqual(ids((await store.log.read({ after: null, limit: 10 })).rows), ['committed'], 'committed row');
      assertEqual(await store.commit([write('s', 0, { appends: [append('conflict')] })]), ['conflict'], 'conflict');
      await rejects(() => store.commit([write('a', 0, { appends: [append('rejected', 'a')] }), write('a')]));
      assertEqual(await store.log.find('decision', 'conflict'), null, 'no conflict row');
      assertEqual(await store.log.find('decision', 'rejected'), null, 'no rejected row');
      assertEqual(ids((await store.log.read({ after: null, limit: 10 })).rows), ['committed'], 'only committed');
    }],
    ['rows read back in commit order with strictly increasing positions', async (store) => {
      const appends = [append('one', 'a'), append('two', 'b'), letter('three', 'b'), append('four', 'a')];
      await store.commit([write('a', 0, { appends: appends.slice(0, 1) })]);
      await store.commit([write('b', 0, { appends: appends.slice(1, 3) })]);
      await store.commit([write('a', 1, { appends: appends.slice(3) })]);
      const rows = (await store.log.read({ after: null, limit: 10 })).rows;
      assertEqual(rows.map(({ position: _, ...row }) => row), appends, 'append and commit order');
      for (let i = 1; i < rows.length; i += 1) assertOk(store.log.compare(position(rows, i - 1), position(rows, i)) < 0, 'strictly increasing');
      assertEqual(await store.log.bounds(), { first: position(rows, 0), last: position(rows, 3) }, 'bounds');
    }],
    ['read returns rows strictly after a position and pages by limit', async (store) => {
      await seed(store, Array.from({ length: 5 }, (_, i) => append(`${i + 1}`)));
      const all = (await store.log.read({ after: null, limit: 10 })).rows;
      const page = await store.log.read({ after: position(all, 0), limit: 2 });
      assertEqual(page, { rows: all.slice(1, 3), checked: position(all, 2), more: true }, 'middle page');
      assertEqual(await store.log.read({ after: page.checked, limit: 2 }), { rows: all.slice(3), checked: position(all, 4), more: false }, 'last page');
      assertEqual(await store.log.read({ after: position(all, 4), limit: 2 }), { rows: [], checked: position(all, 4), more: false }, 'head never goes backwards');
    }],
    ['checked reaches the head when a read is exhausted and the last row examined when it stops early', async (store) => {
      await seed(store, [...Array.from({ length: 6 }, (_, i) => letter(`letter:${i}`)), append('decision')]);
      const all = (await store.log.read({ after: null, limit: 10 })).rows;
      assertEqual(await store.log.read({ after: null, routes: ['queue'], limit: 10 }), { rows: [], checked: position(all, 6), more: false }, 'exhausted without matches');
      const first = await store.log.read({ after: null, kinds: ['decision'], limit: 1, scanLimit: 4 });
      assertEqual(first, { rows: [], checked: position(all, 3), more: true }, 'scan limit');
      assertEqual(await store.log.read({ after: first.checked, kinds: ['decision'], limit: 2, scanLimit: 4 }), { rows: all.slice(6), checked: position(all, 6), more: false }, 'continued scan');
    }],
    ['a scoped read examines only that scope', async (store) => {
      await seed(store, [...Array.from({ length: 50 }, (_, i) => append(`before:${i}`, 'b')), append('a', 'a'), ...Array.from({ length: 50 }, (_, i) => append(`after:${i}`, 'b'))]);
      const row = await store.log.find('decision', 'a');
      const head = (await store.log.bounds()).last;
      assertEqual(await store.log.read({ after: null, scope: 'a', limit: 2, scanLimit: 4 }), { rows: [row], checked: head, more: false }, 'scope index');
    }],
    ['a prefix read matches only scopes that start with the prefix', async (store) => {
      const scopes = ['p/x', 'p/y', 'px', 'p%', 'p_', 'q'];
      await seed(store, scopes.map((scope) => append(scope, scope)));
      for (const [prefix, expected] of [['p/', ['p/x', 'p/y']], ['p%', ['p%']], ['p_', ['p_']], ['', scopes]] satisfies [string, string[]][]) {
        assertEqual(ids((await store.log.read({ after: null, scopePrefix: prefix, limit: 10 })).rows), expected, `prefix ${prefix}`);
      }
    }],
    ['kinds, routes and sinceAt filter rows', async (store) => {
      await seed(store, [append('interrupt', 's', 'interrupt', 1000), append('queue', 's', 'queue', 2000), append('note', 's', 'note', 3000), letter('judge', 's', 'judge', 4000), letter('sink', 's', 'sink', 5000)]);
      const query = async (filter: Partial<DecisionQuery>) => ids((await store.log.read({ after: null, limit: 10, ...filter })).rows);
      assertEqual(await query({ kinds: ['decision'] }), ['interrupt', 'queue', 'note'], 'decisions');
      assertEqual(await query({ kinds: ['deadLetter'] }), ['judge', 'sink'], 'letters');
      assertEqual(await query({ routes: ['interrupt'] }), ['interrupt', 'sink'], 'null route excluded');
      assertEqual(await query({ routes: ['queue', 'note'] }), ['queue', 'note'], 'route list');
      assertEqual(await query({ sinceAt: 3000 }), ['note', 'judge', 'sink'], 'inclusive since');
      assertEqual(await query({ kinds: ['deadLetter'], routes: ['interrupt'], sinceAt: 5000 }), ['sink'], 'combined');
    }],
    ['find returns a row by kind and id', async (store) => {
      const rows = [append('same'), letter('same', 's', 'sink')];
      await seed(store, rows);
      const all = (await store.log.read({ after: null, limit: 10 })).rows;
      assertEqual(await store.log.find('decision', 'same'), all[0], 'decision');
      assertEqual(await store.log.find('deadLetter', 'same'), all[1], 'dead letter');
      assertEqual(await store.log.find('decision', 'unknown'), null, 'missing');
    }],
    ['an append whose kind and id are already logged is skipped and its write still commits', async (store) => {
      await seed(store, [append('same')]);
      const before = await store.log.read({ after: null, limit: 10 });
      assertEqual(await store.commit([write('s', 1, { appends: [append('same', 's', 'queue', 2000)] })]), ['ok'], 'duplicate commits');
      assertEqual((await store.load('s'))?.version, 2, 'version advanced');
      assertEqual(await store.log.read({ after: null, limit: 10 }), before, 'first row retained');
    }],
    ['wait resolves on an append, at the timeout and on abort', async (store) => {
      await seed(store, [append('first')]);
      const first = (await store.log.bounds()).last;
      await store.log.read({ after: null, limit: 10 });
      await store.log.find('decision', 'first');
      const abort = new AbortController();
      try {
        const pending = store.log.wait(first, 5000, abort.signal);
        const start = Date.now();
        await seed(store, [append('second')]);
        await within(pending, 3900);
        assertOk(Date.now() - start < 4000, 'append wakes waiter');
        const head = (await store.log.bounds()).last;
        const timeoutAt = Date.now();
        await within(store.log.wait(head, 30, abort.signal), 1900);
        assertOk(Date.now() - timeoutAt >= 20, 'wait did not return immediately');
        assertOk(Date.now() - timeoutAt < 2000, 'timeout resolves');
        const cancelled = new AbortController();
        const waiting = store.log.wait(head, 60_000, cancelled.signal);
        cancelled.abort();
        await within(waiting, 900);
        await within(store.log.wait(first, 60_000, abort.signal), 900);
      } finally {
        abort.abort();
      }
    }],
    ['trim keeps the newest rows by count or age and never drops the head', async (store) => {
      const old = Date.now() - 10 * 86_400_000;
      await seed(store, Array.from({ length: 5 }, (_, i) => append(`old:${i}`, 's', 'interrupt', old + i)));
      const all = (await store.log.read({ after: null, limit: 10 })).rows;
      await store.log.trim({ maxRows: 2 });
      assertEqual((await store.log.read({ after: null, limit: 10 })).rows, all.slice(3), 'newest two');
      assertEqual(await store.log.bounds(), { first: position(all, 3), last: position(all, 4) }, 'trimmed bounds');
      await store.log.trim({ maxRows: 0 });
      assertEqual((await store.log.read({ after: null, limit: 10 })).rows, all.slice(4), 'head survives zero');
      await seed(store, [append('old:5', 's', 'interrupt', old + 5), append('now', 's', 'interrupt', Date.now())]);
      await store.log.trim({ olderThanMs: 86_400_000 });
      assertEqual(ids((await store.log.read({ after: null, limit: 10 })).rows), ['now'], 'age');
      await store.log.trim({ olderThanMs: 0, maxRows: 0 });
      assertEqual(ids((await store.log.read({ after: null, limit: 10 })).rows), ['now'], 'head always survives');
    }],
    ['the epoch is stable on one instance and new for each non-durable instance', async (store) => {
      const epoch = await store.log.epoch();
      assertOk(typeof epoch === 'string' && epoch.length > 0, 'nonempty epoch');
      assertEqual(await store.log.epoch(), epoch, 'stable');
      if (!store.durable) {
        const other = await make();
        try {
          assertOk(await other.log.epoch() !== epoch, 'distinct incarnations');
        } finally {
          await other.close?.();
        }
      }
    }],
  ];
  const result = casesFor(make, cases);
  const concurrent = options?.concurrent;
  if (concurrent) {
    const together = (name: string, run: (a: ScopeStore, b: ScopeStore) => Promise<void>): ConformanceCase => ({
      name,
      async run() {
        const [rawA, rawB] = await concurrent();
        const [a, b] = [watched(rawA), watched(rawB)];
        let closed = false;
        try {
          await run(a.store, b.store);
          closed = true;
          await Promise.all([a.store.close?.(), b.store.close?.()]);
          await new Promise((resolve) => setTimeout(resolve, 0));
          a.check();
          b.check();
        } finally {
          if (!closed) await Promise.all([a.store.close?.(), b.store.close?.()]);
        }
      },
    });
    result.push(together('two stores on one database share a durable epoch', async (a, b) => {
      assertOk(a.durable && b.durable, 'both durable');
      const epoch = await a.log.epoch();
      assertOk(epoch.length > 0, 'nonempty epoch');
      assertEqual(await b.log.epoch(), epoch, 'shared epoch');
    }));
    result.push(together('a reader never skips a row while two writers commit concurrently', async (a, b) => {
      const pending = [a, b].flatMap((store, writer) => Array.from({ length: 100 }, (_, i) => {
        const scope = `writer:${writer}:${i}`;
        return store.commit([write(scope, 0, { appends: [append(scope, scope)] })]);
      }));
      const commits = Promise.allSettled(pending);
      const seen = new Set<string>();
      let after: Position | null = null;
      let previous: Position | null = null;
      const end = Date.now() + 15_000;
      try {
        while (seen.size < 200 && Date.now() < end) {
          const page = await a.log.read({ after, limit: 7 });
          for (const row of page.rows) {
            assertOk(!seen.has(row.id), 'no duplicate rows');
            if (previous !== null) assertOk(a.log.compare(previous, row.position) < 0, 'positions strictly increase');
            previous = row.position;
            seen.add(row.id);
          }
          if (after !== null && page.checked !== null) assertOk(a.log.compare(after, page.checked) <= 0, 'cursor never retreats');
          after = page.checked;
          if (!page.more) await new Promise<void>((resolve) => setTimeout(resolve, 1));
        }
        const expected = [0, 1].flatMap((writer) => Array.from({ length: 100 }, (_, i) => `writer:${writer}:${i}`));
        assertEqual([...seen].sort(), expected.sort(), 'every committed id exactly once');
      } finally {
        const outcomes = await commits;
        for (const outcome of outcomes) {
          if (outcome.status === 'rejected') throw outcome.reason;
          assertEqual(outcome.value, ['ok'], 'concurrent write committed');
        }
      }
    }));
  }
  return result;
}
