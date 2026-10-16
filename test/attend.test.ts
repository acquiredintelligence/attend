import { describe, expect, test } from 'vitest';
import { ConflictError, createAttend, defaultNotice, logSource, OutboxFullError, PermanentError, routes, type AttendConfig } from '../src/attend.js';
import { memoryLog, memoryStore } from '../src/memory.js';
import { drain, fakeClock, recordingSink, scriptedJudge, type ScriptedAnswer } from '../src/testing.js';
import type { DeadLetter, Decision, Event, EventLog, Judge, JudgeInput, Judgment, Kind, SettlePolicy, Sink, Source, SourceBatch, StateStore, Trace } from '../src/types.js';

const kind = (e: Event): Kind => {
  switch (e.name) {
    case 'close':
      return 'closing';
    case 'say':
      return 'flush';
    case 'look':
      return 'passive';
    case 'out':
      return 'consequence';
    default:
      return 'context';
  }
};

const ev = (id: string, name = 'close'): Event => ({ id, at: 0, name, data: {}, text: `${name} ${id}` });
const ID_A = '3ecbd4f2ef91c78f';
const ID_B = '3ecbd5f2ef91c942';
const ID_AB = 'f9df8ba2df127ca6';
const ID_AC = 'f9df8ca2df127e59';
const ID_BC = 'aace44ab3ec7c91e';
const ID_ABC = '1a353cc02182e85a';
const NO_WAIT = ['interrupt', 'queue', 'note', 'ask', 'skip'];

/** Takes `wait` whenever it is offered and `interrupt` otherwise, so a window that may wait is held to the horizon. */
const eager = (input: JudgeInput): Judgment => ({ route: input.routes.some((r) => r.route === 'wait') ? 'wait' : 'interrupt' });

/** An eager judge whose first call stays open until `release`. Records every input. */
function heldJudge() {
  const calls: JudgeInput[] = [];
  let release: (judgment: Judgment) => void = () => undefined;
  const first = new Promise<Judgment>((resolve) => {
    release = resolve;
  });
  const judge: Judge = (input) => {
    calls.push(input);
    return calls.length === 1 ? first : Promise.resolve(eager(input));
  };
  return { judge, calls, release: (judgment: Judgment) => release(judgment) };
}

function setup(script: ScriptedAnswer[], extra?: { settle?: Partial<SettlePolicy>; judge?: Judge; store?: StateStore; config?: Partial<AttendConfig>; run?: boolean }) {
  const clock = fakeClock(1000);
  const store = extra?.store ?? memoryStore();
  const judge = scriptedJudge(script);
  const sink = recordingSink();
  const deadLetters: DeadLetter[] = [];
  const traces: Trace[] = [];
  const attend = createAttend({
    store,
    judge: extra?.judge ?? judge,
    sink,
    kind,
    deadLetter: async (letter) => {
      deadLetters.push(letter);
    },
    observe: (t) => traces.push(t),
    clock,
    ...(extra?.settle ? { settle: extra.settle } : {}),
    ...extra?.config,
  });
  const controller = new AbortController();
  const done = extra?.run === false ? Promise.resolve() : attend.run({ signal: controller.signal, sweepMs: 1000 });
  const stop = async () => {
    controller.abort();
    await done;
  };
  return { clock, store, judge, sink, deadLetters, traces, attend, stop };
}

/** A source over a list of events, resumable from its cursor (the count already emitted). Records what it was subscribed with. */
function listSource(key: string, events: Event[], options?: { failFirstWith?: Error }): Source & { subscribedFrom: (string | null)[] } {
  const subscribedFrom: (string | null)[] = [];
  let pendingFailure = options?.failFirstWith;
  return {
    key,
    subscribedFrom,
    async subscribe(cursor, emit, signal) {
      subscribedFrom.push(cursor);
      if (pendingFailure) {
        const error = pendingFailure;
        pendingFailure = undefined;
        throw error;
      }
      for (let i = cursor === null ? 0 : Number(cursor); i < events.length && !signal.aborted; i += 1) {
        const event = events[i];
        if (event) await emit({ events: [event], cursor: String(i + 1) });
      }
      if (signal.aborted) return;
      await new Promise<void>((resolve) => signal.addEventListener('abort', () => resolve(), { once: true }));
    },
  };
}

const realTick = () => new Promise((r) => setTimeout(r, 10));

/** A store whose puts wait while held, the way a slow database leaves a write in flight. */
function heldStore(): { store: StateStore; hold(): void; release(): void } {
  const inner = memoryStore();
  let gate: Promise<void> | null = null;
  let open: () => void = () => undefined;
  return {
    store: {
      get: (scope) => inner.get(scope),
      async put(record, expectedVersion) {
        if (gate) await gate;
        return inner.put(record, expectedVersion);
      },
      due: (now, limit) => inner.due(now, limit),
      remove: (scope) => inner.remove(scope),
    },
    hold() {
      gate = new Promise((resolve) => {
        open = () => resolve();
      });
    },
    release() {
      gate = null;
      open();
    },
  };
}

const pendingOrResolved = (p: Promise<unknown>) => Promise.race([p.then(() => 'resolved'), drain().then(() => 'pending')]);

describe('createAttend', () => {
  test('a closing act settles after the grace, is judged with the configured context, and is delivered', async () => {
    const h = setup([{ route: 'queue', probabilities: { queue: 0.8, note: 0.2 }, predicates: { asked: 0.1 } }], {
      config: {
        guidance: 'an agent that watches a repo',
        criteria: { interrupt: 'cannot wait' },
        predicates: { asked: 'Did the user ask for this?' },
        context: () => ({ facts: { turn: 'idle' }, predicates: { wants: 'Does the user want help?' } }),
      },
    });
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(499);
    expect(h.judge.calls).toHaveLength(0);
    await h.clock.advance(1);
    expect(h.judge.calls).toEqual([
      {
        scope: 's',
        window: { id: ID_A, events: [{ ...ev('a'), kind: 'closing', seenAt: 1000 }], reason: 'closing', waits: 0, dropped: 0, gap: false, settledAt: 1500 },
        routes: [{ route: 'interrupt', criterion: 'cannot wait' }, { route: 'queue' }, { route: 'note' }, { route: 'ask' }, { route: 'skip' }, { route: 'wait' }],
        guidance: 'an agent that watches a repo',
        predicates: [
          { name: 'asked', question: 'Did the user ask for this?' },
          { name: 'wants', question: 'Does the user want help?' },
        ],
        facts: { turn: 'idle' },
        recent: [],
        now: 1500,
      },
    ]);
    expect(h.sink.delivered).toEqual([
      {
        id: ID_A,
        scope: 's',
        route: 'queue',
        probabilities: { interrupt: 0, queue: 0.8, note: 0.2, ask: 0, skip: 0, wait: 0 },
        predicates: { asked: 0.1 },
        window: { id: ID_A, events: [{ ...ev('a'), kind: 'closing', seenAt: 1000 }], reason: 'closing', waits: 0, dropped: 0, gap: false, settledAt: 1500 },
        at: 1500,
      },
    ]);
    const record = await h.attend.inspect('s');
    expect(record).toMatchObject({ version: 4, batch: null, outbox: { decisions: [], deadLetters: [] }, recent: [{ id: ID_A, route: 'queue', at: 1500 }], dueAt: null });
    await h.stop();
  });

  test('a flush settles at once with wait withheld', async () => {
    const h = setup([{ route: 'note' }]);
    await h.attend.push('s', [ev('a', 'say')]);
    await h.clock.advance(0);
    expect(h.judge.calls[0]?.routes.map((r) => r.route)).toEqual(['interrupt', 'queue', 'note', 'ask', 'skip']);
    expect(h.sink.delivered.map((d) => d.route)).toEqual(['note']);
    await h.stop();
  });

  test('a direct address that lands while the judge is busy is judged as flush with no wait, although an act deadline passed first', async () => {
    const held = heldJudge();
    const h = setup([], { judge: held.judge });
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(500);
    await h.clock.advance(100);
    await h.attend.push('s', [ev('b')]);
    await h.clock.advance(600);
    await h.attend.push('s', [ev('c', 'say')]);
    held.release({ route: 'queue' });
    await h.clock.advance(0);
    expect(held.calls[1]).toMatchObject({ window: { id: ID_BC, reason: 'flush', events: [{ id: 'b' }, { id: 'c' }] } });
    expect(held.calls[1]?.routes.map((r) => r.route)).toEqual(NO_WAIT);
    expect(h.sink.delivered.map((d) => [d.id, d.route, d.at])).toEqual([
      [ID_A, 'queue', 2200],
      [ID_BC, 'interrupt', 2200],
    ]);
    await h.stop();
  });

  test('attend.flush() while the judge is busy settles the act whose deadline passed as forced with no wait', async () => {
    const held = heldJudge();
    const h = setup([], { judge: held.judge });
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(500);
    await h.clock.advance(100);
    await h.attend.push('s', [ev('b')]);
    await h.clock.advance(600);
    await h.attend.flush('s');
    held.release({ route: 'queue' });
    await h.clock.advance(0);
    expect(held.calls[1]).toMatchObject({ window: { id: ID_B, reason: 'forced', events: [{ id: 'b' }] } });
    expect(held.calls[1]?.routes.map((r) => r.route)).toEqual(NO_WAIT);
    expect(h.sink.delivered.map((d) => [d.id, d.route, d.at])).toEqual([
      [ID_A, 'queue', 2200],
      [ID_B, 'interrupt', 2200],
    ]);
    await h.stop();
  });

  test('attend.flush() while the judge holds the only window forbids its wait: a wait verdict is judged again at once as forced', async () => {
    const held = heldJudge();
    const h = setup([], { judge: held.judge });
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(500);
    expect(held.calls[0]?.routes.some((r) => r.route === 'wait')).toBe(true);
    await h.attend.flush('s');
    held.release({ route: 'wait' });
    await h.clock.advance(0);
    expect(held.calls[1]).toMatchObject({ window: { id: ID_A, reason: 'forced', waits: 1, events: [{ id: 'a' }] } });
    expect(held.calls[1]?.routes.map((r) => r.route)).toEqual(NO_WAIT);
    expect(h.sink.delivered.map((d) => [d.id, d.route, d.at])).toEqual([[ID_A, 'interrupt', 1500]]);
    expect(await h.attend.inspect('s')).toMatchObject({ batch: null, buffer: { forcedAt: null }, dueAt: null });
    await h.stop();
  });

  test('a direct address merged into a waiting window is judged at once with no wait', async () => {
    const held = heldJudge();
    const h = setup([], { judge: held.judge });
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(500);
    await h.clock.advance(100);
    await h.attend.push('s', [ev('b')]);
    await h.clock.advance(600);
    await h.attend.push('s', [ev('c', 'say')]);
    held.release({ route: 'wait' });
    await h.clock.advance(0);
    expect(held.calls[1]).toMatchObject({ window: { id: ID_ABC, reason: 'flush', waits: 1, events: [{ id: 'a' }, { id: 'b' }, { id: 'c' }] } });
    expect(held.calls[1]?.routes.map((r) => r.route)).toEqual(NO_WAIT);
    expect(h.sink.delivered.map((d) => [d.id, d.route, d.at])).toEqual([[ID_ABC, 'interrupt', 2200]]);
    await h.stop();
  });

  test('an act whose deadline passed between sweeps and a flush from another replica are judged together with no wait', async () => {
    const store = memoryStore();
    const clock = fakeClock(1000);
    const producer = setup([], { store, run: false, config: { clock } });
    const runner = setup([], { store, judge: async (input) => eager(input), config: { clock } });
    await producer.attend.push('s', [ev('a')]);
    await clock.advance(700);
    await producer.attend.push('s', [ev('c', 'say')]);
    await clock.advance(300);
    expect(runner.judge.calls).toEqual([]);
    expect(runner.sink.delivered.map((d) => [d.id, d.window.reason, d.route, d.at])).toEqual([[ID_AC, 'flush', 'interrupt', 2000]]);
    await runner.stop();
  });

  test('a wait carries the window into the next settle', async () => {
    const h = setup([{ route: 'wait' }, { route: 'queue' }]);
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(500);
    expect(h.judge.calls).toHaveLength(1);
    expect(await h.attend.inspect('s')).toMatchObject({ batch: { window: { waits: 1 }, phase: { phase: 'waiting', until: 11_500 } } });
    await h.attend.push('s', [ev('b')]);
    await h.clock.advance(500);
    expect(h.judge.calls[1]?.window).toMatchObject({ id: ID_AB, waits: 1 });
    expect(h.judge.calls[1]?.routes.some((r) => r.route === 'wait')).toBe(true);
    expect(h.sink.delivered.map((d) => d.id)).toEqual([ID_AB]);
    await h.stop();
  });

  test('the horizon judges a waiting window alone with wait withheld', async () => {
    const h = setup([{ route: 'wait' }, { route: 'ask' }]);
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(500);
    await h.clock.advance(9999);
    expect(h.judge.calls).toHaveLength(1);
    await h.clock.advance(1);
    expect(h.judge.calls[1]?.routes.map((r) => r.route)).toEqual(['interrupt', 'queue', 'note', 'ask', 'skip']);
    expect(h.sink.delivered.map((d) => [d.id, d.route])).toEqual([[ID_A, 'ask']]);
    await h.stop();
  });

  test('the horizon judges the held window together with a consequence that arrived while waiting', async () => {
    const h = setup([{ route: 'wait' }, { route: 'queue' }]);
    await h.attend.push('s', [ev('a', 'look')]);
    await h.clock.advance(4000);
    expect(h.judge.calls.map((c) => c.window.events.map((e) => e.id))).toEqual([['a']]);
    await h.clock.advance(2000);
    await h.attend.push('s', [ev('c1', 'out')]);
    await h.clock.advance(8000);
    expect(h.judge.calls.map((c) => c.window.events.map((e) => e.id))).toEqual([['a'], ['a', 'c1']]);
    expect(h.judge.calls[1]?.routes.map((r) => r.route)).toEqual(['interrupt', 'queue', 'note', 'ask', 'skip']);
    expect(h.sink.delivered.map((d) => d.window.events.map((e) => e.id))).toEqual([['a', 'c1']]);
    expect((await h.attend.inspect('s'))?.buffer.entries).toEqual([]);
    await h.stop();
  });

  test('a judge call that outlives a wait cannot answer for the call after the horizon', async () => {
    const pending: ((judgment: { route: 'interrupt' | 'queue' }) => void)[] = [];
    const hold = () => new Promise<{ route: 'interrupt' | 'queue' }>((resolve) => pending.push(resolve));
    const answers: (() => Promise<{ route: 'interrupt' | 'wait' | 'queue' }>)[] = [hold, () => Promise.resolve({ route: 'wait' }), hold];
    const judge: Judge = () => answers.shift()?.() ?? Promise.resolve({ route: 'skip' });
    const h = setup([], { judge });
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(500);
    await h.clock.advance(30_000);
    await h.clock.advance(1000);
    expect(await h.attend.inspect('s')).toMatchObject({ batch: { phase: { phase: 'waiting', attempt: 2 } } });
    await h.clock.advance(10_000);
    expect(await h.attend.inspect('s')).toMatchObject({ batch: { phase: { phase: 'judging', attempt: 3 } } });
    pending[0]?.({ route: 'interrupt' });
    await h.clock.advance(0);
    expect(h.traces).toContainEqual({ kind: 'stale', scope: 's', input: 'judged', why: `no live judge attempt 1 for batch ${ID_A}` });
    expect(h.sink.delivered).toEqual([]);
    pending[1]?.({ route: 'queue' });
    await h.clock.advance(0);
    expect(h.sink.delivered.map((d) => d.route)).toEqual(['queue']);
    await h.stop();
  });

  test('a retryable judge failure backs off and retries', async () => {
    const h = setup([{ fail: 'busy' }, { route: 'note' }]);
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(500);
    expect(h.traces).toContainEqual({ kind: 'retrying', scope: 's', stage: 'judge', id: ID_A, attempt: 1, at: 2500, message: 'busy' });
    await h.clock.advance(999);
    expect(h.judge.calls).toHaveLength(1);
    await h.clock.advance(1);
    expect(h.judge.calls).toHaveLength(2);
    expect(h.sink.delivered.map((d) => d.route)).toEqual(['note']);
    await h.stop();
  });

  test('a judge that always throws dead-letters after the retry budget, never silently', async () => {
    const h = setup([{ fail: 'down' }]);
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(500 + 1000 + 4000 + 16_000);
    expect(h.judge.calls).toHaveLength(4);
    expect(h.deadLetters).toHaveLength(1);
    expect(h.deadLetters[0]).toMatchObject({ id: ID_A, stage: 'judge', decision: null, message: 'down', permanent: false, attempts: 4 });
    expect(h.sink.delivered).toEqual([]);
    expect(await h.attend.inspect('s')).toMatchObject({ batch: null, outbox: { decisions: [], deadLetters: [] } });
    await h.stop();
  });

  test('a flush during a judge outage keeps the retry budget, so the outage still ends in a dead letter', async () => {
    const h = setup([{ fail: 'down' }]);
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(500);
    await h.attend.flush('s');
    await h.clock.advance(0);
    expect(h.judge.calls).toHaveLength(2);
    await h.clock.advance(4000 + 16_000);
    expect(h.judge.calls).toHaveLength(4);
    expect(h.deadLetters.map((l) => [l.id, l.attempts])).toEqual([[ID_A, 4]]);
    await h.stop();
  });

  test('a permanent judge error dead-letters at once', async () => {
    const h = setup([{ fail: 'refused', permanent: true }]);
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(500);
    expect(h.judge.calls).toHaveLength(1);
    expect(h.deadLetters[0]).toMatchObject({ stage: 'judge', permanent: true, attempts: 1 });
    await h.stop();
  });

  test('a malformed judgment is a retryable failure', async () => {
    const answers = ['{"nonsense":true}', '{"route":"queue"}'];
    const judge: Judge = async () => JSON.parse(answers.shift() ?? '{"route":"skip"}');
    const h = setup([], { judge });
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(500);
    expect(h.traces).toContainEqual(expect.objectContaining({ kind: 'retrying', stage: 'judge', message: 'malformed judgment' }));
    await h.clock.advance(1000);
    expect(h.sink.delivered.map((d) => d.route)).toEqual(['queue']);
    await h.stop();
  });

  test('a judge that never answers is aborted at the timeout and re-judged after the backoff', async () => {
    const h = setup([{ hang: true }, { route: 'queue' }]);
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(500);
    expect(await h.attend.inspect('s')).toMatchObject({ batch: { phase: { phase: 'judging', leaseUntil: 1500 + 30_000 } }, dueAt: 31_500 });
    await h.clock.advance(15_000);
    expect(h.traces).toContainEqual(expect.objectContaining({ kind: 'retrying', stage: 'judge', attempt: 1, message: 'aborted' }));
    await h.clock.advance(1000);
    expect(h.sink.delivered.map((d) => d.route)).toEqual(['queue']);
    await h.stop();
  });

  test('a judge that ignores its signal is retried when the lease expires', async () => {
    let calls = 0;
    const judge: Judge = () => {
      calls += 1;
      return calls === 1 ? new Promise(() => undefined) : Promise.resolve({ route: 'queue' });
    };
    const h = setup([], { judge });
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(500);
    await h.clock.advance(29_999);
    expect(calls).toBe(1);
    await h.clock.advance(1);
    expect(h.traces).toContainEqual(expect.objectContaining({ kind: 'retrying', stage: 'judge', message: 'judge lease expired' }));
    await h.clock.advance(1000);
    expect(h.sink.delivered.map((d) => d.route)).toEqual(['queue']);
    await h.stop();
  });

  test('a sink that fails once is retried after the backoff', async () => {
    const h = setup([{ route: 'queue' }]);
    h.sink.failNext(1);
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(500);
    expect(h.sink.delivered).toEqual([]);
    await h.clock.advance(1000);
    expect(h.sink.delivered.map((d) => d.id)).toEqual([ID_A]);
    expect(h.sink.attempts.get(ID_A)).toBe(2);
    await h.stop();
  });

  test('a sink that keeps failing dead-letters the decision', async () => {
    const h = setup([{ route: 'interrupt' }]);
    h.sink.failNext(10);
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(500 + 1000 + 4000 + 16_000);
    expect(h.sink.attempts.get(ID_A)).toBe(4);
    expect(h.deadLetters[0]).toMatchObject({ id: ID_A, stage: 'sink', decision: { route: 'interrupt' }, attempts: 4 });
    expect(await h.attend.inspect('s')).toMatchObject({ outbox: { decisions: [], deadLetters: [] } });
    await h.stop();
  });

  test('a dead-letter handler that fails is retried and the letter is never dropped', async () => {
    const letters: DeadLetter[] = [];
    let refusals = 2;
    const deadLetter = async (letter: DeadLetter) => {
      if (refusals > 0) {
        refusals -= 1;
        throw new Error('handler down');
      }
      letters.push(letter);
    };
    const h = setup([{ fail: 'no', permanent: true }], { config: { deadLetter } });
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(500);
    expect(letters).toEqual([]);
    expect(h.traces).toContainEqual(expect.objectContaining({ kind: 'retrying', stage: 'deadLetter', attempt: 1, at: 2500 }));
    await h.clock.advance(1000 + 4000);
    expect(letters).toHaveLength(1);
    expect(await h.attend.inspect('s')).toMatchObject({ outbox: { decisions: [], deadLetters: [] } });
    await h.stop();
  });

  test('a dead-letter handler that always throws never holds back a later decision', async () => {
    const deadLetter = async () => {
      throw new Error('handler down');
    };
    const h = setup([{ fail: 'no', permanent: true }, { route: 'queue' }], { config: { deadLetter } });
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(500);
    await h.attend.push('s', [ev('b')]);
    await h.clock.advance(500);
    expect(h.sink.delivered.map((d) => d.id)).toEqual([ID_B]);
    expect((await h.attend.inspect('s'))?.outbox).toEqual({
      decisions: [],
      deadLetters: [
        {
          item: {
            id: ID_A,
            kind: 'deadLetter',
            letter: {
              id: ID_A,
              scope: 's',
              stage: 'judge',
              window: { id: ID_A, events: [{ ...ev('a'), kind: 'closing', seenAt: 1000 }], reason: 'closing', waits: 0, dropped: 0, gap: false, settledAt: 1500 },
              decision: null,
              message: 'no',
              permanent: true,
              attempts: 1,
              at: 1500,
            },
          },
          phase: { phase: 'retrying', attempt: 1, failures: 1, at: 2500 },
        },
      ],
    });
    await h.stop();
  });

  test('a dead-letter lane at maxDeadLetters rejects intake, while decisions still flow below their own cap', async () => {
    const deadLetter = async () => {
      throw new Error('handler down');
    };
    const h = setup([{ fail: 'no', permanent: true }, { route: 'queue' }], { config: { deadLetter }, settle: { maxDeadLetters: 1 } });
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(500);
    await expect(h.attend.push('s', [ev('b')])).rejects.toMatchObject({ name: 'OutboxFullError', lane: 'deadLetters', size: 1 });
    expect(h.sink.delivered).toEqual([]);
    const roomy = setup([{ fail: 'no', permanent: true }, { route: 'queue' }], { config: { deadLetter }, settle: { maxDeadLetters: 2 } });
    await roomy.attend.push('s', [ev('a')]);
    await roomy.clock.advance(500);
    await roomy.attend.push('s', [ev('b')]);
    await roomy.clock.advance(500);
    expect(roomy.sink.delivered.map((d) => d.id)).toEqual([ID_B]);
    await h.stop();
    await roomy.stop();
  });

  test('a dead letter whose handler keeps throwing retries with doubling backoff up to the cap', async () => {
    let calls = 0;
    const deadLetter = async () => {
      calls += 1;
      throw new Error('handler down');
    };
    const h = setup([{ fail: 'no', permanent: true }], { config: { deadLetter } });
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(500 + 173_000);
    expect(calls).toBe(7);
    expect(h.traces.flatMap((t) => (t.kind === 'retrying' && t.stage === 'deadLetter' ? [t.at] : []))).toEqual([2500, 6500, 22_500, 54_500, 114_500, 174_500, 234_500]);
    expect((await h.attend.inspect('s'))?.outbox.deadLetters).toMatchObject([{ item: { id: ID_A }, phase: { phase: 'retrying', attempt: 7, failures: 7, at: 234_500 } }]);
    await h.stop();
  });

  test('push rejects with OutboxFullError at maxOutbox and attach resumes after backoff', async () => {
    const h = setup([{ route: 'queue' }], { settle: { maxOutbox: 1 } });
    h.sink.failNext(1);
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(500);
    await expect(h.attend.push('s', [ev('b')])).rejects.toBeInstanceOf(OutboxFullError);
    const log = memoryLog();
    await log.append('s', [ev('c')]);
    const attached = h.attend.attach('s', logSource(log, 's', { blockMs: 0, pollMs: 5 }));
    await realTick();
    expect(h.traces).toContainEqual({
      kind: 'retrying',
      scope: 's',
      stage: 'source',
      id: 'log:s',
      attempt: 1,
      at: 2500,
      message: 'attend: outbox for scope "s" holds 1 undelivered decisions',
    });
    await h.clock.advance(1000);
    await realTick();
    expect(h.sink.delivered.map((d) => d.id)).toEqual([ID_A]);
    expect((await h.attend.inspect('s'))?.buffer.entries.map((e) => e.id)).toEqual(['c']);
    await h.stop();
    await attached;
  });

  test('attach backs off through the retry rungs on any source error and stops on a PermanentError', async () => {
    const h = setup([{ route: 'skip' }]);
    const flaky = listSource('flaky', [ev('a', 'out')], { failFirstWith: new Error('socket reset') });
    const attached = h.attend.attach('s', flaky);
    await realTick();
    expect(h.traces).toContainEqual({ kind: 'retrying', scope: 's', stage: 'source', id: 'flaky', attempt: 1, at: 2000, message: 'socket reset' });
    expect(flaky.subscribedFrom).toEqual([null]);
    await h.clock.advance(1000);
    await realTick();
    expect(flaky.subscribedFrom).toEqual([null, null]);
    expect((await h.attend.inspect('s'))?.cursors).toEqual({ flaky: '1' });
    const fatal = listSource('fatal', [], { failFirstWith: new PermanentError('revoked') });
    await expect(h.attend.attach('s', fatal)).rejects.toThrow('revoked');
    await h.stop();
    await attached;
  });

  test('two sources on one scope keep separate cursors and both resume after a restart', async () => {
    const store = memoryStore();
    const log = memoryLog();
    await log.append('s', [ev('a', 'out'), ev('b', 'out')]);
    const other = listSource('other', [ev('x', 'out'), ev('y', 'out')]);
    const first = setup([{ route: 'skip' }], { store });
    const stopFirst = new AbortController();
    const attached = [
      first.attend.attach('s', logSource(log, 's', { blockMs: 0, pollMs: 5 }), { signal: stopFirst.signal }),
      first.attend.attach('s', other, { signal: stopFirst.signal }),
    ];
    await realTick();
    expect((await store.get('s'))?.cursors).toEqual({ 'log:s': '00000000000000000002', other: '2' });
    expect((await store.get('s'))?.buffer.entries.map((e) => e.id).sort()).toEqual(['a', 'b', 'x', 'y']);
    stopFirst.abort();
    await Promise.all(attached);
    await first.stop();

    await log.append('s', [ev('c', 'out')]);
    const resumed = listSource('other', [ev('x', 'out'), ev('y', 'out'), ev('z', 'out')]);
    const second = setup([{ route: 'skip' }], { store });
    const stopSecond = new AbortController();
    const reattached = [
      second.attend.attach('s', logSource(log, 's', { blockMs: 0, pollMs: 5 }), { signal: stopSecond.signal }),
      second.attend.attach('s', resumed, { signal: stopSecond.signal }),
    ];
    await realTick();
    expect(resumed.subscribedFrom).toEqual(['2']);
    expect((await store.get('s'))?.cursors).toEqual({ 'log:s': '00000000000000000003', other: '3' });
    expect(second.traces.filter((t) => t.kind === 'ingested').map((t) => [t.count, t.duplicates])).toEqual([
      [1, 0],
      [1, 0],
    ]);
    expect((await store.get('s'))?.buffer.entries.map((e) => e.id).sort()).toEqual(['a', 'b', 'c', 'x', 'y', 'z']);
    stopSecond.abort();
    await Promise.all(reattached);
    await second.stop();
  });

  test('a second attach of the same source key on a scope throws', async () => {
    const h = setup([]);
    const signal = new AbortController();
    const attached = h.attend.attach('s', listSource('k', []), { signal: signal.signal });
    await expect(h.attend.attach('s', listSource('k', []))).rejects.toThrow('attend: source "k" is already attached to scope "s"');
    await expect(h.attend.attach('t', listSource('k', []), { signal: AbortSignal.abort() })).resolves.toBeUndefined();
    signal.abort();
    await attached;
    await h.stop();
  });

  test('two writers on one scope: the loser reloads and both inputs land', async () => {
    const store = memoryStore();
    const a = setup([{ route: 'skip' }], { store });
    const b = setup([{ route: 'skip' }], { store });
    await Promise.all([a.attend.push('s', [ev('a')]), b.attend.push('s', [ev('b')])]);
    expect((await store.get('s'))?.seen.sort()).toEqual(['a', 'b']);
    expect([...a.traces, ...b.traces]).toContainEqual({ kind: 'conflict', scope: 's', version: 0 });
    await a.stop();
    await b.stop();
  });

  test('a writer that never wins backs off between tries and gives up after eight conflicts', async () => {
    const inner = memoryStore();
    let puts = 0;
    const store: StateStore = {
      ...inner,
      async put() {
        puts += 1;
        return 'conflict';
      },
    };
    const h = setup([], { store });
    const started = Date.now();
    await expect(h.attend.push('s', [ev('a')])).rejects.toBeInstanceOf(ConflictError);
    expect(Date.now() - started).toBeGreaterThanOrEqual(50);
    expect(puts).toBe(8);
    expect(h.traces.filter((t) => t.kind === 'conflict')).toHaveLength(8);
    expect(h.traces).toContainEqual({ kind: 'abandoned', scope: 's', input: 'events' });
    await h.stop();
  });

  test('a flush settles whatever is open', async () => {
    const h = setup([{ route: 'note' }]);
    await h.attend.push('s', [ev('x', 'out')]);
    await h.clock.advance(5000);
    expect(h.judge.calls).toHaveLength(0);
    await h.attend.flush('s');
    await h.clock.advance(0);
    expect(h.judge.calls[0]?.window).toMatchObject({ reason: 'forced', events: [{ id: 'x' }] });
    await h.stop();
  });

  test('a flush from a producer without a runner is persisted and settled by a runner elsewhere', async () => {
    const store = memoryStore();
    const producer = setup([], { store, run: false });
    await producer.attend.push('s', [ev('x', 'out')]);
    await producer.attend.flush('s');
    expect(producer.judge.calls).toEqual([]);
    expect(await store.get('s')).toMatchObject({ buffer: { entries: [{ id: 'x' }], forcedAt: 1000 }, batch: null, dueAt: 1000 });
    const runner = setup([{ route: 'note' }], { store });
    await runner.clock.advance(1000);
    expect(runner.judge.calls[0]?.window).toMatchObject({ reason: 'forced', events: [{ id: 'x' }] });
    expect(runner.sink.delivered.map((d) => d.route)).toEqual(['note']);
    await runner.stop();
  });

  test('logSource feeds a scope from the log and resumes from the persisted cursor', async () => {
    const store = memoryStore();
    const log = memoryLog();
    await log.append('s', [ev('a'), ev('b')]);
    const first = setup([{ route: 'queue' }], { store });
    const stopFirst = new AbortController();
    const attached = first.attend.attach('s', logSource(log, 's', { blockMs: 20, pollMs: 0 }), { signal: stopFirst.signal });
    await new Promise((r) => setTimeout(r, 30));
    await first.clock.advance(500);
    expect(first.sink.delivered.map((d) => d.id)).toEqual([ID_AB]);
    expect((await store.get('s'))?.cursors).toEqual({ 'log:s': '00000000000000000002' });
    stopFirst.abort();
    await attached;
    await first.stop();

    await log.append('s', [ev('c')]);
    const second = setup([{ route: 'queue' }], { store });
    const stopSecond = new AbortController();
    const reattached = second.attend.attach('s', logSource(log, 's', { blockMs: 20, pollMs: 0 }), { signal: stopSecond.signal });
    await new Promise((r) => setTimeout(r, 30));
    await second.clock.advance(500);
    expect(second.judge.calls[0]?.window.events.map((e) => e.id)).toEqual(['c']);
    stopSecond.abort();
    await reattached;
    await second.stop();
  });

  test('logSource paces empty polls by pollMs on the injected clock', async () => {
    const clock = fakeClock(0);
    const log = memoryLog();
    let reads = 0;
    const counted: EventLog = {
      append: (scope, events) => log.append(scope, events),
      read: (scope, after, options) => {
        reads += 1;
        return log.read(scope, after, options);
      },
    };
    const batches: SourceBatch[] = [];
    const controller = new AbortController();
    const done = logSource(counted, 's', { blockMs: 0, pollMs: 5000, clock }).subscribe(
      null,
      async (batch) => {
        batches.push(batch);
      },
      controller.signal,
    );
    await clock.advance(0);
    expect(reads).toBe(1);
    await clock.advance(4999);
    expect(reads).toBe(1);
    await clock.advance(1);
    expect(reads).toBe(2);
    await log.append('s', [ev('a')]);
    await clock.advance(5000);
    expect(batches).toEqual([{ events: [ev('a')], cursor: '00000000000000000001' }]);
    expect(reads).toBe(4);
    controller.abort();
    await done;
  });

  test('run resolves on abort and refuses to run twice', async () => {
    const h = setup([]);
    await expect(h.attend.run({ signal: new AbortController().signal })).rejects.toThrow('already running');
    await h.stop();
  });

  test('an aborted run waits for the judge in flight, persists its verdict, starts no delivery, and the next runner delivers after the lease', async () => {
    const store = memoryStore();
    const held = heldJudge();
    const first = setup([], { store, judge: held.judge });
    await first.attend.push('s', [ev('a')]);
    await first.clock.advance(500);
    const stopping = first.stop();
    expect(await Promise.race([stopping.then(() => 'resolved'), drain().then(() => 'pending')])).toBe('pending');
    held.release({ route: 'queue' });
    await stopping;
    expect(first.sink.delivered).toEqual([]);
    expect(await store.get('s')).toMatchObject({
      batch: null,
      outbox: { decisions: [{ item: { id: ID_A, kind: 'decision' }, phase: { phase: 'inFlight', attempt: 1, leaseUntil: 11_500 } }], deadLetters: [] },
      dueAt: 11_500,
    });
    const second = setup([], { store, config: { clock: first.clock } });
    await first.clock.advance(10_000);
    expect(second.traces).toContainEqual(expect.objectContaining({ kind: 'retrying', stage: 'sink', id: ID_A, message: 'delivery lease expired' }));
    await first.clock.advance(1000);
    expect(second.sink.delivered.map((d) => [d.id, d.route])).toEqual([[ID_A, 'queue']]);
    expect(first.sink.delivered).toEqual([]);
    await second.stop();
  });

  test('an aborted run waits for a tick whose write is in flight, so the store sees it before run resolves', async () => {
    const slow = heldStore();
    const stuck: Sink = { deliver: () => new Promise(() => undefined) };
    const h = setup([{ route: 'queue' }], { store: slow.store, config: { sink: stuck } });
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(500);
    expect(await slow.store.get('s')).toMatchObject({ outbox: { decisions: [{ item: { id: ID_A }, phase: { phase: 'inFlight', attempt: 1, leaseUntil: 11_500 } }] } });
    slow.hold();
    await h.clock.advance(10_000);
    const stopping = h.stop();
    expect(await pendingOrResolved(stopping)).toBe('pending');
    expect(await slow.store.get('s')).toMatchObject({ outbox: { decisions: [{ phase: { phase: 'inFlight' } }] } });
    slow.release();
    await stopping;
    expect(await slow.store.get('s')).toMatchObject({
      outbox: { decisions: [{ item: { id: ID_A }, phase: { phase: 'retrying', attempt: 1, failures: 1, at: 12_500 } }], deadLetters: [] },
      dueAt: 12_500,
    });
  });

  test("an aborted run waits for a source's write in flight, so the store sees it before run resolves", async () => {
    const slow = heldStore();
    const h = setup([], { store: slow.store });
    slow.hold();
    const attached = h.attend.attach('s', listSource('k', [ev('x', 'out')]));
    await drain();
    expect(await slow.store.get('s')).toBeNull();
    const stopping = h.stop();
    expect(await pendingOrResolved(stopping)).toBe('pending');
    slow.release();
    await stopping;
    expect(await slow.store.get('s')).toMatchObject({ version: 1, cursors: { k: '1' }, buffer: { entries: [{ id: 'x', kind: 'consequence' }] } });
    await attached;
  });

  test('an aborted run does not wait past the lease for a judge that never settles', async () => {
    const h = setup([], { judge: () => new Promise(() => undefined) });
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(500);
    const stopping = h.stop();
    expect(await Promise.race([stopping.then(() => 'resolved'), drain().then(() => 'pending')])).toBe('pending');
    await h.clock.advance(30_000);
    expect(await Promise.race([stopping.then(() => 'resolved'), drain().then(() => 'pending')])).toBe('resolved');
  });

});

describe('routes and defaultNotice', () => {
  const decision = (route: Decision['route']): Decision => ({
    id: ID_A,
    scope: 's',
    route,
    probabilities: { interrupt: 0, queue: 0, note: 0, ask: 0, skip: 0, wait: 0 },
    predicates: {},
    window: {
      id: ID_A,
      events: [
        { ...ev('a'), kind: 'closing', seenAt: 1 },
        { id: 'b', at: 0, name: 'agent.said', data: { words: 2 }, kind: 'context', seenAt: 2 },
        { id: 'c', at: 0, name: 'build.done', data: { ok: true }, kind: 'consequence', seenAt: 3 },
      ],
      reason: 'closing',
      waits: 0,
      dropped: 2,
      gap: true,
      settledAt: 5,
    },
    at: 5,
  });

  test('routes dispatches each delivered route to its handler', async () => {
    const seen: string[] = [];
    const sink = routes({
      interrupt: (d) => {
        seen.push(`interrupt:${d.id}`);
      },
      queue: (d) => {
        seen.push(`queue:${d.id}`);
      },
      note: async (d) => {
        seen.push(`note:${d.id}`);
      },
      ask: (d) => {
        seen.push(`ask:${d.id}`);
      },
    });
    const signal = new AbortController().signal;
    for (const route of ['interrupt', 'queue', 'note', 'ask'] as const) await sink.deliver(decision(route), signal);
    expect(seen).toEqual([`interrupt:${ID_A}`, `queue:${ID_A}`, `note:${ID_A}`, `ask:${ID_A}`]);
  });

  test('defaultNotice lists the events, what was left out, and one line on the route', () => {
    expect(defaultNotice(decision('queue'))).toBe(
      ['Some events before these were lost.', 'close a', 'build.done {"ok":true}', '(2 background events dropped from this window)', 'Take this up when the current step is done.'].join('\n'),
    );
    expect(defaultNotice(decision('interrupt'), { maxLines: 1 })).toBe(
      ['(1 older events not shown)', 'Some events before these were lost.', 'build.done {"ok":true}', '(2 background events dropped from this window)', 'Attend to this now, even mid-task.'].join('\n'),
    );
  });
});
