/**
 * `attend/testing`: a fake clock, a scripted judge, a recording sink, the port conformance
 * suites, the crash harness and the crash suite. Runner-agnostic: the suites return named
 * async cases that any test runner can register.
 */
import type { Clock, Decision, Event, EventLog, Judge, JudgeInput, Judgment, ScopeRecord, Sink, StateStore } from './types.js';
import { initialRecord } from './step.js';
import { PermanentError } from './errors.js';

// ─── clock ───────────────────────────────────────────────────────────────────

export interface FakeClock extends Clock {
  /** Move time forward, firing every timer due on the way in order and letting each one's work drain. */
  advance(ms: number): Promise<void>;
  /** Jump to an instant, firing timers the same way. */
  set(at: number): Promise<void>;
  /** The earliest armed timer, or null. */
  nextAt(): number | null;
}

/** One macrotask: every microtask chain that does not wait on real I/O has drained after it. */
export function drain(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

export function fakeClock(start = 0): FakeClock {
  let now = start;
  let seq = 0;
  const timers: { at: number; seq: number; fn: () => void }[] = [];
  const earliest = () => timers.reduce<(typeof timers)[number] | null>((best, t) => (best === null || t.at < best.at || (t.at === best.at && t.seq < best.seq) ? t : best), null);
  return {
    now: () => now,
    at(at, fn) {
      const timer = { at, seq: seq++, fn };
      timers.push(timer);
      return () => {
        const i = timers.indexOf(timer);
        if (i !== -1) timers.splice(i, 1);
      };
    },
    nextAt: () => earliest()?.at ?? null,
    async set(at) {
      for (let next = earliest(); next !== null && next.at <= at; next = earliest()) {
        now = Math.max(now, next.at);
        timers.splice(timers.indexOf(next), 1);
        next.fn();
        await drain();
      }
      now = Math.max(now, at);
      await drain();
    },
    advance(ms) {
      return this.set(now + ms);
    },
  };
}

// ─── judge and sink doubles ──────────────────────────────────────────────────

export type ScriptedAnswer = Judgment | { fail: string; permanent?: boolean } | { hang: true };

export interface ScriptedJudge extends Judge {
  calls: JudgeInput[];
}

/** Answers in order from a script; the last entry repeats. Records every input. */
export function scriptedJudge(script: ScriptedAnswer[]): ScriptedJudge {
  const calls: JudgeInput[] = [];
  const judge = async (input: JudgeInput, signal: AbortSignal): Promise<Judgment> => {
    const answer = script[Math.min(calls.length, script.length - 1)];
    calls.push(input);
    if (answer === undefined) throw new Error('scriptedJudge: empty script');
    if ('hang' in answer) {
      return new Promise((_, reject) => signal.addEventListener('abort', () => reject(new Error('aborted')), { once: true }));
    }
    if ('fail' in answer) throw answer.permanent ? new PermanentError(answer.fail) : new Error(answer.fail);
    return answer;
  };
  return Object.assign(judge, { calls });
}

export interface RecordingSink extends Sink {
  delivered: Decision[];
  /** Attempts per decision id, including the failed ones. */
  attempts: Map<string, number>;
  /** Make the next `n` deliveries throw. */
  failNext(n: number, permanent?: boolean): void;
}

export function recordingSink(): RecordingSink {
  const delivered: Decision[] = [];
  const attempts = new Map<string, number>();
  let failing = 0;
  let permanent = false;
  return {
    delivered,
    attempts,
    failNext(n, p = false) {
      failing = n;
      permanent = p;
    },
    async deliver(decision) {
      attempts.set(decision.id, (attempts.get(decision.id) ?? 0) + 1);
      if (failing > 0) {
        failing -= 1;
        throw permanent ? new PermanentError('sink refused') : new Error('sink failed');
      }
      delivered.push(decision);
    },
  };
}

// ─── conformance ─────────────────────────────────────────────────────────────

export interface ConformanceCase {
  name: string;
  run(): Promise<void>;
}

class ConformanceError extends Error {}

function canonical(value: unknown): string {
  return JSON.stringify(value, (_key, v: unknown) =>
    typeof v === 'object' && v !== null && !Array.isArray(v) ? Object.fromEntries(Object.entries(v).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))) : v,
  );
}

export function assertEqual(actual: unknown, expected: unknown, message: string): void {
  const a = canonical(actual);
  const e = canonical(expected);
  if (a !== e) throw new ConformanceError(`${message}\n  expected: ${e}\n  actual:   ${a}`);
}

export function assertOk(value: unknown, message: string): void {
  if (!value) throw new ConformanceError(message);
}

const record = (scope: string, version: number, dueAt: number | null): ScopeRecord => ({ ...initialRecord(scope), version, dueAt });

/**
 * Every guarantee in `StateStore`'s doc comments, as cases. `make` returns a fresh, empty
 * store per case. Run them with your runner:
 *   for (const c of storeConformance(() => memoryStore())) test(c.name, c.run)
 */
export function storeConformance(make: () => Promise<StateStore> | StateStore): ConformanceCase[] {
  const cases: [string, (store: StateStore) => Promise<void>][] = [
    [
      'get of a missing scope is null',
      async (store) => {
        assertEqual(await store.get('missing'), null, 'get');
      },
    ],
    [
      'put at version 0 creates and get returns a deep-equal record',
      async (store) => {
        const r: ScopeRecord = {
          ...record('s', 1, 42),
          cursor: 'c1',
          seen: ['a', 'b'],
          buffer: { entries: [{ id: 'a', at: 1, name: 'n', data: { nested: { list: [1, 'two', null, true] }, text: 'héllo 🌍' }, kind: 'closing', seenAt: 5 }], dropped: 1, gap: true, forcedAt: null },
          recent: [{ id: 'x', route: 'note', at: 3 }],
        };
        assertEqual(await store.put(r, 0), 'ok', 'put');
        assertEqual(await store.get('s'), r, 'round trip');
      },
    ],
    [
      'put at a stale version conflicts and leaves the record unchanged',
      async (store) => {
        await store.put(record('s', 1, 10), 0);
        await store.put(record('s', 2, 20), 1);
        assertEqual(await store.put(record('s', 2, 99), 1), 'conflict', 'stale put');
        assertEqual(await store.get('s'), record('s', 2, 20), 'unchanged');
      },
    ],
    [
      'put at version 0 on an existing scope conflicts',
      async (store) => {
        await store.put(record('s', 1, null), 0);
        assertEqual(await store.put(record('s', 1, null), 0), 'conflict', 'create twice');
      },
    ],
    [
      'N concurrent puts at one version give exactly one ok',
      async (store) => {
        await store.put(record('s', 1, null), 0);
        const results = await Promise.all(Array.from({ length: 20 }, (_, i) => store.put(record('s', 2, i), 1)));
        assertEqual(results.filter((r) => r === 'ok').length, 1, 'exactly one winner');
        const stored = await store.get('s');
        assertEqual(stored?.version, 2, 'winner persisted');
      },
    ],
    [
      'due returns only dueAt <= now, ascending, honouring limit',
      async (store) => {
        await store.put(record('late', 1, 300), 0);
        await store.put(record('early', 1, 100), 0);
        await store.put(record('mid', 1, 200), 0);
        await store.put(record('future', 1, 1000), 0);
        assertEqual(await store.due(300, 10), ['early', 'mid', 'late'], 'ascending');
        assertEqual(await store.due(300, 2), ['early', 'mid'], 'limit');
        assertEqual(await store.due(99, 10), [], 'none due');
      },
    ],
    [
      'due excludes null dueAt and a put that clears dueAt removes the scope',
      async (store) => {
        await store.put(record('idle', 1, null), 0);
        await store.put(record('busy', 1, 5), 0);
        assertEqual(await store.due(10, 10), ['busy'], 'null excluded');
        await store.put(record('busy', 2, null), 1);
        assertEqual(await store.due(10, 10), [], 'cleared');
      },
    ],
    [
      'remove then get is null and put at version 0 succeeds again',
      async (store) => {
        await store.put(record('s', 1, 1), 0);
        await store.remove('s');
        assertEqual(await store.get('s'), null, 'removed');
        assertEqual(await store.due(10, 10), [], 'not due');
        assertEqual(await store.put(record('s', 1, null), 0), 'ok', 'recreated');
      },
    ],
    [
      'scopes that are prefixes of each other or hold punctuation stay separate',
      async (store) => {
        const scopes = ['a', 'a/b', 'a:b', 'a%b', 'ünï', 'a/b/c'];
        for (const [i, scope] of scopes.entries()) await store.put(record(scope, 1, i), 0);
        for (const [i, scope] of scopes.entries()) assertEqual((await store.get(scope))?.dueAt, i, scope);
      },
    ],
    [
      'a record at maxEvents round-trips',
      async (store) => {
        const entries = Array.from({ length: 250 }, (_, i) => ({ id: `e${i}`, at: i, name: 'n', data: { i }, kind: 'passive' as const, seenAt: i }));
        const r: ScopeRecord = { ...record('s', 1, 1), buffer: { entries, dropped: 0, gap: false, forcedAt: null } };
        await store.put(r, 0);
        assertEqual(await store.get('s'), r, 'large round trip');
      },
    ],
  ];
  return cases.map(([name, run]) => ({ name, run: async () => run(await make()) }));
}

const event = (id: string): Event => ({ id, at: 0, name: 'n', data: { id } });

/** Every guarantee in `EventLog`'s doc comments. `canBlock` says whether `blockMs` waits. */
export function logConformance(make: () => Promise<EventLog> | EventLog, options?: { canBlock?: boolean }): ConformanceCase[] {
  const canBlock = options?.canBlock ?? true;
  const cases: [string, (log: EventLog) => Promise<void>][] = [
    [
      'read(null) returns from the oldest retained entry in order',
      async (log) => {
        await log.append('s', [event('a'), event('b')]);
        await log.append('s', [event('c')]);
        assertEqual((await log.read('s', null, { limit: 10 })).map((e) => e.event.id), ['a', 'b', 'c'], 'order');
      },
    ],
    [
      'positions strictly increase and read(after) returns strictly after',
      async (log) => {
        await log.append('s', [event('a'), event('b'), event('c')]);
        const all = await log.read('s', null, { limit: 10 });
        for (let i = 1; i < all.length; i += 1) assertOk((all[i]?.position ?? '') > (all[i - 1]?.position ?? ''), 'increasing');
        const after = await log.read('s', all[0]?.position ?? null, { limit: 10 });
        assertEqual(after.map((e) => e.event.id), ['b', 'c'], 'strictly after');
        assertEqual(await log.read('s', all[2]?.position ?? null, { limit: 10 }), [], 'nothing after the last');
      },
    ],
    [
      'limit is honoured and the next read continues',
      async (log) => {
        await log.append('s', [event('a'), event('b'), event('c')]);
        const first = await log.read('s', null, { limit: 2 });
        assertEqual(first.map((e) => e.event.id), ['a', 'b'], 'first page');
        const second = await log.read('s', first[1]?.position ?? null, { limit: 2 });
        assertEqual(second.map((e) => e.event.id), ['c'], 'second page');
      },
    ],
    [
      'scopes are separate',
      async (log) => {
        await log.append('s', [event('a')]);
        await log.append('t', [event('a')]);
        assertEqual((await log.read('s', null, { limit: 10 })).length, 1, 'one per scope');
      },
    ],
    [
      'two producers appending concurrently lose nothing',
      async (log) => {
        await Promise.all([
          log.append('s', Array.from({ length: 50 }, (_, i) => event(`p1-${i}`))),
          log.append('s', Array.from({ length: 50 }, (_, i) => event(`p2-${i}`))),
        ]);
        const ids = (await log.read('s', null, { limit: 1000 })).map((e) => e.event.id);
        assertEqual(new Set(ids).size, 100, 'all present');
      },
    ],
    [
      'events round-trip deep-equal',
      async (log) => {
        const e: Event = { id: 'a', at: 7, name: 'x.y', data: { n: 1, s: 'ünï 🌍', l: [null, { k: false }] }, text: 'one line' };
        await log.append('s', [e]);
        assertEqual((await log.read('s', null, { limit: 1 }))[0]?.event, e, 'round trip');
      },
    ],
    canBlock
      ? [
          'read with blockMs returns early on append and empty at the timeout',
          async (log) => {
            const started = Date.now();
            const pending = log.read('s', null, { limit: 10, blockMs: 5000 });
            await log.append('s', [event('a')]);
            assertEqual((await pending).map((e) => e.event.id), ['a'], 'woken by append');
            assertOk(Date.now() - started < 4000, 'returned before the timeout');
            const last = (await log.read('s', null, { limit: 10 }))[0]?.position ?? null;
            assertEqual(await log.read('s', last, { limit: 10, blockMs: 20 }), [], 'empty at the timeout');
          },
        ]
      : [
          'read with blockMs returns at once',
          async (log) => {
            const started = Date.now();
            assertEqual(await log.read('s', null, { limit: 10, blockMs: 5000 }), [], 'empty');
            assertOk(Date.now() - started < 1000, 'did not wait');
          },
        ],
  ];
  return cases.map(([name, run]) => ({ name, run: async () => run(await make()) }));
}
