/**
 * `attend/testing`: a fake clock, a scripted judge, a recording sink, the port conformance
 * suites, the crash harness and the crash suite. Runner-agnostic: the suites return named
 * async cases that any test runner can register.
 */
import type { Clock, DeadLetter, Decision, Event, EventLog, Judge, JudgeInput, Judgment, Kind, ScopeRecord, SettlePolicy, Sink, ScopeStore, Trace } from './types.js';
import { PermanentError } from './errors.js';
import { createAttend, type Attend, type AttendConfig } from './attend.js';
import { memoryStore } from './memory.js';
import { assertEqual, assertOk } from './internal/assert.js';
import { drain } from './internal/clock.js';
import { drainCopies } from './shell/copy.js';
import { readRecord } from './shell/read.js';

export { drain } from './internal/clock.js';

// ─── clock ───────────────────────────────────────────────────────────────────

export interface FakeClock extends Clock {
  /** Move time forward, firing every timer due on the way in order and letting each one's work drain. */
  advance(ms: number): Promise<void>;
  /** Jump to an instant, firing timers the same way. */
  set(at: number): Promise<void>;
  /** The earliest armed timer, or null. */
  nextAt(): number | null;
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
        await drainCopies();
        await drain();
        await drainCopies();
        await drain();
      }
      now = Math.max(now, at);
      await drainCopies();
      await drain();
      await drainCopies();
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
export { decisionLogConformance, scopeStoreConformance } from './testing/conformance.js';
export { eventsWireConformance, type WireTarget } from './testing/wire.js';
export { suggestThreshold } from './core/threshold.js';
export type { SuggestThresholdOptions, ThresholdCandidate, ThresholdSuggestion } from './core/threshold.js';

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
  store?: ScopeStore;
  /** The clock's start. Default 0. */
  start?: number;
  /** Default 1000. */
  sweepMs?: number;
}

export interface Harness {
  readonly clock: FakeClock;
  readonly store: ScopeStore;
  readonly scope: string;
  /** The current instance; a new one after `restart()`. */
  readonly attend: Attend;
  readonly deadLetters: DeadLetter[];
  readonly traces: Trace[];
  /** Commits so far, across instances. `crashAt` counts the same way. */
  readonly commits: number;
  /** True from a crash until `restart()`. */
  readonly crashed: boolean;
  /** Push into the scope and wait until the runner has nothing left to do. Throws `CrashError` if a crash struck. */
  push(events: Event[]): Promise<void>;
  /** Advance the clock, firing timers one at a time and draining after each. Throws `CrashError` at the instant a crash struck. */
  advance(ms: number): Promise<void>;
  /** Flush the scope and let the runner's tick settle it. */
  flush(): Promise<void>;
  /** Resolve once no store call is in flight. Work that waits on real time is invisible to it. */
  idle(): Promise<void>;
  /** Kill the runner now. The store keeps whatever was committed. */
  crash(): void;
  /** Schedule a crash at the nth commit from now, counting `commits` so far. */
  crashAt(commit: number, point: CrashPoint): void;
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
  let commits = 0;
  let pending = 0;
  let schedule: { commit: number; point: CrashPoint } | null = null;
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
      if (schedule === null || schedule.commit !== n || schedule.point !== point) return;
      schedule = null;
      crashed = true;
      state.dead = true;
      controller.abort();
      throw new CrashError();
    };
    const store: ScopeStore = {
      durable: inner.durable,
      load: (scope) => guard(() => inner.load(scope)),
      due: (now, limit, after) => guard(() => inner.due(now, limit, after)),
      remove: (scope) => guard(() => inner.remove(scope)),
      log: {
        epoch: () => guard(() => inner.log.epoch()),
        bounds: () => guard(() => inner.log.bounds()),
        read: (query) => guard(() => inner.log.read(query)),
        find: (kind, id) => guard(() => inner.log.find(kind, id)),
        wait: (after, ms, signal) => guard(() => inner.log.wait(after, ms, signal)),
        trim: (retention) => guard(() => inner.log.trim(retention)),
        compare: (a, b) => inner.log.compare(a, b),
      },
      commit: (writes) => guard(async () => {
        commits += 1;
        const n = commits;
        strike(n, 'before');
        const result = await inner.commit(writes);
        strike(n, 'after');
        return result;
      }),
    };
    const attend = createAttend({
      ...config,
      store,
      clock,
      deadLetter: async (letter, signal) => {
        if (typeof config.deadLetter === 'function') await config.deadLetter(letter, signal);
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
      await drainCopies();
      await drain();
      if (pending === 0) return;
    }
  }

  function check(): void {
    if (crashed) throw new CrashError();
  }

  async function advance(ms: number): Promise<void> {
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
    get commits() {
      return commits;
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
      await advance(0);
    },
    advance,
    idle,
    crash() {
      die(current);
    },
    crashAt(commit, point) {
      schedule = { commit: commits + commit, point };
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
    inspect: () => readRecord(inner, scope, { decisions: config.sink === 'log' ? 'log' : 'lane', deadLetters: config.deadLetter === 'log' ? 'log' : 'lane' }),
  };
}

// ─── crash suite ─────────────────────────────────────────────────────────────

const sessionKind = (e: Event): Kind => {
  switch (e.name) {
    case 'close':
      return 'closing';
    case 'say':
      return 'flush';
    case 'look':
      return 'passive';
    default:
      return 'consequence';
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
async function session(store: ScopeStore, crash: { commit: number; point: CrashPoint } | null): Promise<{ decisions: string[]; letters: string[]; commits: number }> {
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
  if (crash) h.crashAt(crash.commit, crash.point);
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
  await store.close?.();
  return { decisions: [...delivered].sort(), letters: [...new Set(h.deadLetters.map((l) => l.id))].sort(), commits: h.commits };
}

/** The window ids the session produces in scope `scope`: `a` and `e` are delivered, `b,d` and `f` are dead letters. */
const SESSION_DECISIONS = ['cf8923dfae84f930', 'cf8927dfae84fffc'];
const SESSION_LETTERS = ['cf892adfae850515', 'eaf0348d05594776'];

/**
 * Whole-runtime guarantee over a real adapter: the scripted session runs once cleanly, then
 * once per commit boundary with a crash injected there, restarting over the same store.
 * Every run must produce the same set of delivered decision ids and dead-letter ids.
 */
export function crashSuite(make: () => Promise<ScopeStore> | ScopeStore): ConformanceCase[] {
  const survives = (point: CrashPoint) => async () => {
    const clean = await session(await make(), null);
    for (let commit = 1; commit <= clean.commits; commit += 1) {
      const crashed = await session(await make(), { commit, point });
      assertEqual(crashed.decisions, clean.decisions, `decisions after a crash ${point} commit ${commit}`);
      assertEqual(crashed.letters, clean.letters, `dead letters after a crash ${point} commit ${commit}`);
    }
  };
  return [
    {
      name: 'the scripted session delivers two decisions and two dead letters',
      async run() {
        const clean = await session(await make(), null);
        assertEqual(clean.decisions, SESSION_DECISIONS, 'decisions');
        assertEqual(clean.letters, SESSION_LETTERS, 'dead letters');
      },
    },
    { name: 'the same ids survive a crash before every commit', run: survives('before') },
    { name: 'the same ids survive a crash after every commit', run: survives('after') },
  ];
}

interface MultiGroup {
  scopes: string[];
  outcomes: readonly import('./store/types.js').CommitOutcome[];
  snapshot: boolean;
  effect: boolean;
}

async function multiSession(store: ScopeStore, crash: { commit: number; point: CrashPoint } | null) {
  const clock = fakeClock();
  const groups: MultiGroup[] = [];
  const delivered = new Set<string>();
  const letters = new Set<string>();
  const flaked = new Set<string>();
  const scopes = ['m/x', 'm/y', 'm/z'];
  let commits = 0;
  let pending = 0;
  let victim: number | null = null;
  const strikeState: { struck: { snapshot: boolean; effect: boolean } | null } = { struck: null };
  const instances: { attend: Attend; controller: AbortController; done: Promise<void>; dead: boolean }[] = [];
  const judge: Judge = async (input) => {
    const has = (id: string) => input.window.events.some((event) => event.id === `${input.scope}/${id}`);
    if (has('f')) throw new PermanentError('refused');
    if (has('e') && !flaked.has(input.window.id)) {
      flaked.add(input.window.id);
      throw new Error('flaky');
    }
    if (has('b') && !has('d') && input.routes.some((route) => route.route === 'wait')) return { route: 'wait' };
    return { route: has('d') ? 'interrupt' : has('e') ? 'note' : has('g') ? 'skip' : 'queue' };
  };

  function start(index: number) {
    const controller = new AbortController();
    const state = { dead: false };
    const guard = async <T>(work: () => Promise<T>): Promise<T> => {
      if (state.dead) throw new CrashError();
      pending += 1;
      try { return await work(); }
      finally { pending -= 1; }
    };
    const view: ScopeStore = {
      ...store,
      load: (scope) => guard(() => store.load(scope)),
      due: (now, limit, after) => guard(() => store.due(now, limit, after)),
      remove: (scope) => guard(() => store.remove(scope)),
      log: {
        epoch: () => guard(() => store.log.epoch()),
        bounds: () => guard(() => store.log.bounds()),
        read: (query) => guard(() => store.log.read(query)),
        find: (kind, id) => guard(() => store.log.find(kind, id)),
        wait: (after, ms, signal) => guard(() => store.log.wait(after, ms, signal)),
        trim: (retention) => guard(() => store.log.trim(retention)),
        compare: (a, b) => store.log.compare(a, b),
      },
      commit: (writes) => guard(async () => {
        const n = ++commits;
        const snapshot = writes.some((write) => write.snapshot !== null);
        const effect = writes.some((write) => write.inputs.some(({ input }) => input.kind === 'tick' || input.kind === 'judged' || input.kind === 'judgeFailed'));
        const strike = (point: CrashPoint) => {
          if (strikeState.struck !== null || crash?.commit !== n || crash.point !== point) return;
          strikeState.struck = { snapshot, effect };
          victim = index;
          state.dead = true;
          controller.abort();
          throw new CrashError();
        };
        strike('before');
        const outcomes = await store.commit(writes);
        groups.push({ scopes: writes.map((write) => write.scope), outcomes, snapshot, effect });
        strike('after');
        return outcomes;
      }),
    };
    const attend = createAttend({ store: view, clock, kind: sessionKind, judge, settle: SESSION_POLICY,
      sink: { async deliver(decision) { if (decision.route === 'interrupt') throw new Error('sink down'); delivered.add(decision.id); } },
      deadLetter: (letter) => { letters.add(letter.id); } });
    return { attend, controller, get dead() { return state.dead; }, done: attend.run({ signal: controller.signal, sweepMs: 10000 }) };
  }

  instances.push(start(0), start(1));
  const idle = async () => {
    do { await drainCopies(); await drain(); } while (pending > 0);
  };
  const check = () => { if (victim !== null) throw new CrashError(); };
  const survive = async (work: () => Promise<void>) => {
    try { await work(); await idle(); check(); }
    catch (error) {
      if (!(error instanceof CrashError) || victim === null) throw error;
      const index = victim;
      await instances[index]!.done;
      victim = null;
      instances[index] = start(index);
      await idle();
      await work();
      await idle();
      check();
    }
  };
  const push = (index: number, id: string, name: string, targets = scopes) => survive(async () => {
    await Promise.all(targets.map((scope) => instances[index]!.attend.push(scope, [{ id: `${scope}/${id}`, at: 0, name, data: {}, text: `${name} ${id}` }])));
  });
  const advance = (ms: number) => {
    const target = clock.now() + ms;
    return survive(async () => {
      for (let next = clock.nextAt(); next !== null && next <= target; next = clock.nextAt()) {
        await clock.set(next);
        await idle();
        check();
      }
      await clock.set(target);
    });
  };
  try {
    await idle();
    await push(0, 'a', 'close');
    await advance(600);
    await advance(8000);
    await push(1, 'c', 'out', ['m/x']);
    await push(0, 'h', 'close');
    await advance(600);
    await advance(8000);
    await push(1, 'b', 'look');
    await advance(4100);
    await push(0, 'd', 'close');
    await advance(600);
    await advance(25000);
    await push(1, 'e', 'say');
    await advance(1500);
    await advance(8000);
    await push(0, 'f', 'close');
    await advance(600);
    await advance(8000);
    await push(1, 'g', 'close');
    await advance(600);
    await advance(60000);
    return { decisions: [...delivered].sort(), letters: [...letters].sort(), groups, commits, struck: strikeState.struck };
  } finally {
    for (const instance of instances) instance.controller.abort();
    await Promise.all(instances.map((instance) => instance.done));
    await store.close?.();
  }
}

/** Cross-scope recovery, including mixed conflicts, snapshots and effect-start boundaries. */
export function multiScopeCrashSuite(make: () => Promise<ScopeStore> | ScopeStore): ConformanceCase[] {
  const verify = (result: Awaited<ReturnType<typeof multiSession>>) => {
    assertEqual(result.decisions, MULTI_DECISIONS, 'multi-scope decisions');
    assertEqual(result.letters, MULTI_LETTERS, 'multi-scope dead letters');
  };
  const survives = (point: CrashPoint) => async () => {
    const clean = await multiSession(await make(), null);
    verify(clean);
    let snapshots = 0;
    let effects = 0;
    for (let commit = 1; commit <= clean.commits; commit += 1) {
      const result = await multiSession(await make(), { commit, point });
      verify(result);
      if (result.struck?.snapshot) snapshots += 1;
      if (result.struck?.effect) effects += 1;
    }
    assertOk(snapshots > 0, 'crashed during a snapshot');
    assertOk(effects > 0, 'crashed between commit and effect start');
  };
  return [
    { name: 'three scopes group commits and isolate a conflict beside successful writes', async run() {
      const result = await multiSession(await make(), null);
      verify(result);
      assertOk(result.groups.some((group) => group.scopes.length === 3), 'a group of three');
      assertOk(result.groups.some((group) => group.outcomes.includes('conflict') && group.outcomes.includes('ok')), 'mixed outcomes');
      assertOk(result.groups.some((group) => group.snapshot), 'a snapshot commit');
    } },
    { name: 'three scopes keep their ids after a crash before every group', run: survives('before') },
    { name: 'three scopes keep their ids after a crash after every group', run: survives('after') },
  ];
}

const MULTI_DECISIONS = ['4b0d164deed6032f', '57926a9b506cd3a1', '57926e9b506cda6d', '6be88691d9775406', '6be88f91d9776351', '6be89391d9776a1d', 'd29b92ea485105d3', 'd29b96ea48510c9f', 'd29b9dea48511884'];
const MULTI_LETTERS = ['51a3be624f9cf887', '57926b9b506cd554', '6be89091d9776504', 'ba2ddaa47740a48a', 'c43155c9c7ae9dd8', 'd29b93ea48510786'];
