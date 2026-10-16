/**
 * `attend/testing`: a fake clock, a scripted judge, a recording sink, the port conformance
 * suites, the crash harness and the crash suite. Runner-agnostic: the suites return named
 * async cases that any test runner can register.
 */
import type { Clock, DeadLetter, Decision, Event, EventLog, Judge, JudgeInput, Judgment, Kinded, ScopeRecord, SettlePolicy, Sink, StateStore, Trace } from './types.js';
import { hashIds, initialRecord } from './step.js';
import { PermanentError } from './errors.js';
import { createAttend, type Attend, type AttendConfig } from './attend.js';
import { memoryStore } from './memory.js';

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
  return new Promise((resolve) => (typeof setImmediate === 'function' ? setImmediate(resolve) : setTimeout(resolve, 0)));
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

// ─── crash harness ───────────────────────────────────────────────────────────

/** Thrown by a crashed instance's store, and by the harness once a scheduled crash struck. */
export class CrashError extends Error {
  constructor() {
    super('attend: crashed');
    this.name = 'CrashError';
  }
}

/** Where a scheduled crash strikes relative to the nth commit: before it (nothing written) or after it (written, effects never started). */
export type CrashPoint = 'before' | 'after';

export type HarnessConfig = Omit<AttendConfig, 'store' | 'clock' | 'deadLetter'> & {
  /** Optional here: the harness records every letter after the handler, if any, accepted it. */
  deadLetter?: AttendConfig['deadLetter'];
};

export interface HarnessOptions {
  /** Default 'scope'. */
  scope?: string;
  /** Default `memoryStore()`. */
  store?: StateStore;
  /** The clock's start. Default 0. */
  start?: number;
  /** Default 1000. */
  sweepMs?: number;
}

export interface Harness {
  readonly clock: FakeClock;
  readonly store: StateStore;
  readonly scope: string;
  /** The current instance; a new one after `restart()`. */
  readonly attend: Attend;
  readonly deadLetters: DeadLetter[];
  readonly traces: Trace[];
  /** Commits so far, across instances. `crashAt` counts the same way. */
  readonly puts: number;
  /** True from a crash until `restart()`. */
  readonly crashed: boolean;
  /** Push into the scope and wait until the runner has nothing left to do. Throws `CrashError` if a crash struck. */
  push(events: Event[]): Promise<void>;
  /** Advance the clock, firing timers one at a time and draining after each. Throws `CrashError` at the instant a crash struck. */
  advance(ms: number): Promise<void>;
  flush(): Promise<void>;
  /** Resolve once no store call is in flight. Work that waits on real time is invisible to it. */
  idle(): Promise<void>;
  /** Kill the runner now. The store keeps whatever was committed. */
  crash(): void;
  /** Schedule a crash at the nth commit from now, counting `puts` so far. */
  crashAt(put: number, point: CrashPoint): void;
  /** Start a fresh runner over the same store, as a restarted process would, and let it recover. */
  restart(): Promise<void>;
  /** Stop the runner gracefully. */
  stop(): Promise<void>;
  inspect(): Promise<ScopeRecord | null>;
}

/**
 * Runs `createAttend` on one scope with a fake clock over a store it can crash at any
 * commit. Judges, sinks and context must resolve without real time, or `idle()` cannot
 * see them.
 */
export function harness(config: HarnessConfig, options?: HarnessOptions): Harness {
  const scope = options?.scope ?? 'scope';
  const inner = options?.store ?? memoryStore();
  const clock = fakeClock(options?.start ?? 0);
  const sweepMs = options?.sweepMs ?? 1000;
  const deadLetters: DeadLetter[] = [];
  const traces: Trace[] = [];
  let puts = 0;
  let pending = 0;
  let schedule: { put: number; point: CrashPoint } | null = null;
  let crashed = false;

  interface Instance {
    attend: Attend;
    controller: AbortController;
    done: Promise<void>;
    /** Shared with the instance's store guard: once set, every store call it makes throws. */
    state: { dead: boolean };
  }

  function die(instance: Instance): void {
    crashed = true;
    instance.state.dead = true;
    instance.controller.abort();
  }

  function start(): Instance {
    const controller = new AbortController();
    const state = { dead: false };
    const guard = async <T>(work: () => Promise<T>): Promise<T> => {
      if (state.dead) throw new CrashError();
      pending += 1;
      try {
        return await work();
      } finally {
        pending -= 1;
      }
    };
    const strike = (n: number, point: CrashPoint) => {
      if (schedule === null || schedule.put !== n || schedule.point !== point) return;
      schedule = null;
      crashed = true;
      state.dead = true;
      controller.abort();
      throw new CrashError();
    };
    const store: StateStore = {
      get: (s) => guard(() => inner.get(s)),
      due: (now, limit) => guard(() => inner.due(now, limit)),
      remove: (s) => guard(() => inner.remove(s)),
      put: (record, expected) =>
        guard(async () => {
          puts += 1;
          const n = puts;
          strike(n, 'before');
          const result = await inner.put(record, expected);
          strike(n, 'after');
          return result;
        }),
    };
    const attend = createAttend({
      ...config,
      store,
      clock,
      deadLetter: async (letter, signal) => {
        await config.deadLetter?.(letter, signal);
        deadLetters.push(letter);
      },
      observe: (t) => {
        traces.push(t);
        config.observe?.(t);
      },
    });
    return { attend, controller, done: attend.run({ signal: controller.signal, sweepMs }), state };
  }

  let current = start();

  async function idle(): Promise<void> {
    for (;;) {
      await drain();
      if (pending === 0) return;
    }
  }

  function check(): void {
    if (crashed) throw new CrashError();
  }

  return {
    clock,
    store: inner,
    scope,
    deadLetters,
    traces,
    get attend() {
      return current.attend;
    },
    get puts() {
      return puts;
    },
    get crashed() {
      return crashed;
    },
    async push(events) {
      await current.attend.push(scope, events);
      await idle();
      check();
    },
    async flush() {
      await current.attend.flush(scope);
      await idle();
      check();
    },
    async advance(ms) {
      const target = clock.now() + ms;
      await idle();
      check();
      for (let next = clock.nextAt(); next !== null && next <= target; next = clock.nextAt()) {
        await clock.set(next);
        await idle();
        check();
      }
      await clock.set(target);
      await idle();
      check();
    },
    idle,
    crash() {
      die(current);
    },
    crashAt(put, point) {
      schedule = { put: puts + put, point };
    },
    async restart() {
      die(current);
      await current.done.catch(() => undefined);
      crashed = false;
      current = start();
      await idle();
    },
    async stop() {
      current.controller.abort();
      await current.done;
    },
    inspect: () => inner.get(scope),
  };
}

// ─── crash suite ─────────────────────────────────────────────────────────────

const sessionKind = (e: Event): Kinded => {
  switch (e.name) {
    case 'close':
      return { kind: 'closing' };
    case 'say':
      return { kind: 'flush' };
    case 'look':
      return { kind: 'passive' };
    default:
      return { kind: 'consequence' };
  }
};

/** Short leases and backoffs, so a stranded call recovers well inside the gaps the session leaves between acts. */
const SESSION_POLICY: Partial<SettlePolicy> = { judgeTimeoutMs: 1000, deliveryTimeoutMs: 2000, retryBackoffMs: [1000, 1000, 1000] };

/**
 * A scripted session over a store: a closing act delivered, a passive act the judge waits
 * on and then merges with a closing act whose delivery exhausts the sink, a flush whose
 * first judge call fails, an act the judge refuses for good, and an act the judge skips.
 * Judge and sink decide from event ids, so a re-judged window gets the same answer. Each
 * act is left to resolve under the worst stranded lease before the next one settles, so
 * the windows, and so the ids, are the same in every run.
 */
async function session(store: StateStore, crash: { put: number; point: CrashPoint } | null): Promise<{ decisions: string[]; letters: string[]; puts: number }> {
  const delivered = new Set<string>();
  const flaked = new Set<string>();
  const judge: Judge = async (input) => {
    const ids = input.window.events.map((e) => e.id);
    const has = (id: string) => ids.includes(id);
    if (has('f')) throw new PermanentError('refused');
    if (has('e') && !flaked.has(input.window.id)) {
      flaked.add(input.window.id);
      throw new Error('flaky');
    }
    if (has('b') && !has('d') && input.routes.some((r) => r.route === 'wait')) return { route: 'wait' };
    if (has('d')) return { route: 'interrupt' };
    if (has('e')) return { route: 'note' };
    if (has('g')) return { route: 'skip' };
    return { route: 'queue' };
  };
  const sink: Sink = {
    async deliver(decision) {
      if (decision.route === 'interrupt') throw new Error('sink down');
      delivered.add(decision.id);
    },
  };
  const h = harness({ judge, sink, kind: sessionKind, settle: SESSION_POLICY }, { store, sweepMs: 10_000 });
  if (crash) h.crashAt(crash.put, crash.point);
  const survive = async (op: () => Promise<void>) => {
    try {
      await op();
    } catch (error) {
      if (!(error instanceof CrashError)) throw error;
      await h.restart();
      await op();
    }
  };
  const push = (id: string, name: string) => survive(() => h.push([{ id, at: 0, name, data: {}, text: `${name} ${id}` }]));
  const advance = (ms: number) => {
    const target = h.clock.now() + ms;
    return survive(() => h.advance(target - h.clock.now()));
  };
  await push('a', 'close');
  await advance(600);
  await advance(8000);
  await push('b', 'look');
  await advance(4100);
  await push('d', 'close');
  await advance(600);
  await advance(25_000);
  await push('e', 'say');
  await advance(1500);
  await advance(8000);
  await push('f', 'close');
  await advance(600);
  await advance(8000);
  await push('g', 'close');
  await advance(600);
  await advance(60_000);
  await h.stop();
  return { decisions: [...delivered].sort(), letters: [...new Set(h.deadLetters.map((l) => l.id))].sort(), puts: h.puts };
}

/**
 * Whole-runtime guarantee over a real adapter: the scripted session runs once cleanly, then
 * once per commit boundary with a crash injected there, restarting over the same store.
 * Every run must produce the same set of delivered decision ids and dead-letter ids.
 */
export function crashSuite(make: () => Promise<StateStore> | StateStore): ConformanceCase[] {
  const survives = (point: CrashPoint) => async () => {
    const clean = await session(await make(), null);
    for (let put = 1; put <= clean.puts; put += 1) {
      const crashed = await session(await make(), { put, point });
      assertEqual(crashed.decisions, clean.decisions, `decisions after a crash ${point} commit ${put}`);
      assertEqual(crashed.letters, clean.letters, `dead letters after a crash ${point} commit ${put}`);
    }
  };
  return [
    {
      name: 'the scripted session delivers two decisions and two dead letters',
      async run() {
        const clean = await session(await make(), null);
        assertEqual(clean.decisions, [hashIds(['a']), hashIds(['e'])].sort(), 'decisions');
        assertEqual(clean.letters, [hashIds(['b', 'd']), hashIds(['f'])].sort(), 'dead letters');
      },
    },
    { name: 'the same ids survive a crash before every commit', run: survives('before') },
    { name: 'the same ids survive a crash after every commit', run: survives('after') },
  ];
}
