import { describe, expect, test, vi } from 'vitest';
import fc from 'fast-check';
import { copyEventSized, copyWindowSliced } from '../src/shell/copy.js';
import { createScheduler } from '../src/shell/scheduler.js';
import { systemClock } from '../src/internal/clock.js';
import { estimateRenderedBytes } from '../src/core/window.js';
import { ConflictError, createAttend, createAttendInternal, defaultNotice, logSource, OutboxFullError, PermanentError, routes, type AttendConfig } from '../src/attend.js';
import { memoryLog, memoryStore } from '../src/memory.js';
import { drain, fakeClock, recordingSink, scriptedJudge, type ScriptedAnswer } from '../src/testing.js';
import type { DeadLetter, Decision, Event, EventLog, Judge, JudgeInput, Judgment, Kind, SettlePolicy, Sink, Source, SourceBatch, ScopeStore, Trace } from '../src/types.js';

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

function setup(script: ScriptedAnswer[], extra?: { settle?: Partial<SettlePolicy>; judge?: Judge; store?: ScopeStore; config?: Partial<AttendConfig>; run?: boolean }) {
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

/** A store whose commits wait while held, the way a slow database leaves a write in flight. */
function heldStore(): { store: ScopeStore; hold(): void; release(): void } {
  const inner = memoryStore();
  let gate: Promise<void> | null = null;
  let open: () => void = () => undefined;
  return {
    store: {
      ...inner,
      async commit(writes) {
        if (gate) await gate;
        return inner.commit(writes);
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

/** A store whose every put takes a millisecond of real time, so a producer looping on `push` leaves room for timers to fire. */
function pacedStore(): ScopeStore {
  const inner = memoryStore();
  return {
    ...inner,
    async commit(writes) {
      await new Promise((resolve) => setTimeout(resolve, 1));
      return inner.commit(writes);
    },
    due: (now, limit) => inner.due(now, limit),
    remove: (scope) => inner.remove(scope),
  };
}

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
        confidence: null,
        window: { id: ID_A, events: [{ ...ev('a'), kind: 'closing', seenAt: 1000 }], reason: 'closing', waits: 0, dropped: 0, gap: false, settledAt: 1500 },
        at: 1500,
      },
    ]);
    const record = await h.attend.inspect('s');
    expect(record).toMatchObject({ version: 4, batch: null, outbox: { decisions: [], deadLetters: [] }, recent: [{ id: ID_A, route: 'queue', at: 1500 }], dueAt: null });
    await h.stop();
  });

  test('capture adds its images to the judge input, fetched again on each retry', async () => {
    let shots = 0;
    const h = setup([{ fail: 'flaky' }, { route: 'note' }], {
      settle: { retryBackoffMs: [1000] },
      config: {
        capture: async (scope) => {
          shots += 1;
          return shots === 1 ? { data: `${scope}-${shots}`, mimeType: 'image/png' } : [{ data: `${scope}-${shots}`, mimeType: 'image/jpeg' }];
        },
      },
    });
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(500);
    await h.clock.advance(1000);
    expect(h.judge.calls.map((c) => c.images)).toEqual([[{ data: 's-1', mimeType: 'image/png' }], [{ data: 's-2', mimeType: 'image/jpeg' }]]);
    expect(h.sink.delivered.map((d) => d.route)).toEqual(['note']);
    await h.stop();
  });

  test('a capture that throws or returns null leaves the judge input without images, with one trace for the throw', async () => {
    const h = setup([{ route: 'note' }, { route: 'note' }], {
      config: {
        capture: async (scope) => {
          if (scope === 'boom') throw new Error('no display');
          return null;
        },
      },
    });
    await h.attend.push('boom', [ev('a')]);
    await h.attend.push('fine', [ev('b')]);
    await h.clock.advance(500);
    expect(h.judge.calls.map((c) => [c.scope, 'images' in c])).toEqual([
      ['boom', false],
      ['fine', false],
    ]);
    expect(h.traces.filter((t) => t.kind === 'captureFailed')).toEqual([{ kind: 'captureFailed', scope: 'boom', batchId: h.judge.calls[0]?.window.id, message: 'no display' }]);
    expect(h.sink.delivered.map((d) => d.scope)).toEqual(['boom', 'fine']);
    await h.stop();
  });

  test('a capture that outlives captureTimeoutMs is abandoned and the judge runs without images', async () => {
    const signals: AbortSignal[] = [];
    const h = setup([{ route: 'queue' }], {
      config: {
        capture: (_scope, signal) => {
          signals.push(signal);
          return new Promise(() => undefined);
        },
        captureTimeoutMs: 300,
      },
    });
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(500);
    await h.clock.advance(299);
    expect(h.judge.calls).toHaveLength(0);
    await h.clock.advance(1);
    expect(h.judge.calls.map((c) => 'images' in c)).toEqual([false]);
    expect(signals.map((s) => s.aborted)).toEqual([true]);
    expect(h.traces.filter((t) => t.kind === 'captureFailed')).toEqual([{ kind: 'captureFailed', scope: 's', batchId: ID_A, message: 'attend: capture timed out' }]);
    expect(h.sink.delivered.map((d) => d.route)).toEqual(['queue']);
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

  test('an observer that mutates trace payloads does not change the stored record', async () => {
    const observed: string[] = [];
    const h = setup([{ route: 'queue' }], {
      settle: { maxEvents: 1 },
      config: {
        sink: {
          deliver: () => {
            throw new PermanentError('refused');
          },
        },
        deadLetter: () => {
          throw new Error('handler down');
        },
        observe: (trace) => {
          if (trace.kind === 'deadLettered') {
            observed.push(trace.kind);
            trace.letter.message = 'changed';
            const entry = trace.letter.window.events[0];
            if (entry) entry.data['handled'] = true;
            if (trace.letter.decision) trace.letter.decision.route = 'interrupt';
            trace.letter.window.events.length = 0;
          }
          if (trace.kind === 'dropped') {
            observed.push(trace.kind);
            const entry = trace.entries[0];
            if (entry) entry.data['handled'] = true;
            trace.entries.length = 0;
          }
        },
      },
    });
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(500);
    await h.attend.push('s', [ev('b', 'out'), ev('c', 'out')]);
    expect(observed).toEqual(['deadLettered', 'dropped']);
    expect(await h.attend.inspect('s')).toMatchObject({
      buffer: { entries: [{ id: 'c', data: {} }], dropped: 1 },
      outbox: {
        deadLetters: [{
          item: {
            letter: {
              message: 'refused',
              window: { events: [{ id: 'a', data: {} }] },
              decision: { route: 'queue', window: { events: [{ id: 'a', data: {} }] } },
            },
          },
        }],
      },
    });
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
    expect((await first.attend.inspect('s'))?.cursors).toEqual({ 'log:s': '00000000000000000002', other: '2' });
    expect((await first.attend.inspect('s'))?.buffer.entries.map((e) => e.id).sort()).toEqual(['a', 'b', 'x', 'y']);
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
    expect((await second.attend.inspect('s'))?.cursors).toEqual({ 'log:s': '00000000000000000003', other: '3' });
    expect(second.traces.filter((t) => t.kind === 'ingested').map((t) => [t.count, t.duplicates])).toEqual([
      [1, 0],
      [1, 0],
    ]);
    expect((await second.attend.inspect('s'))?.buffer.entries.map((e) => e.id).sort()).toEqual(['a', 'b', 'c', 'x', 'y', 'z']);
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
    expect([...((await a.attend.inspect('s'))?.seen ?? [])].sort()).toEqual(['a', 'b']);
    expect([...a.traces, ...b.traces]).toContainEqual({ kind: 'conflict', scope: 's', version: 0 });
    await a.stop();
    await b.stop();
  });

  test('a writer that never wins backs off between tries and gives up after eight conflicts', async () => {
    const inner = memoryStore();
    let commits = 0;
    const store: ScopeStore = {
      ...inner,
      async commit(writes) {
        commits += 1;
        return writes.map(() => 'conflict' as const);
      },
    };
    const h = setup([], { store });
    const started = Date.now();
    await expect(h.attend.push('s', [ev('a')])).rejects.toBeInstanceOf(ConflictError);
    expect(Date.now() - started).toBeGreaterThanOrEqual(50);
    expect(commits).toBe(8);
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
    expect(await producer.attend.inspect('s')).toMatchObject({ buffer: { entries: [{ id: 'x' }], forcedAt: 1000 }, batch: null, dueAt: 1000 });
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
    expect((await first.attend.inspect('s'))?.cursors).toEqual({ 'log:s': '00000000000000000002' });
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

  test('pushes that queue behind a write in flight land in one write, in order', async () => {
    const held = heldStore();
    let commits = 0;
    const store: ScopeStore = { ...held.store, commit: (writes) => (commits += 1, held.store.commit(writes)) };
    const { attend, traces, stop } = setup([{ route: 'skip' }], { store });
    held.hold();
    const first = attend.push('s', [ev('a')]);
    await drain();
    const queued = Promise.all([attend.push('s', [ev('b'), ev('c')]), attend.push('s', [ev('d')])]);
    await drain();
    expect(commits).toBe(1);
    held.release();
    await first;
    await queued;
    expect(commits).toBe(2);
    expect((await attend.inspect('s'))?.buffer.entries.map((e) => e.id)).toEqual(['a', 'b', 'c', 'd']);
    expect(traces.filter((t) => t.kind === 'ingested')).toEqual([
      { kind: 'ingested', scope: 's', count: 1, duplicates: 0 },
      { kind: 'ingested', scope: 's', count: 3, duplicates: 0 },
    ]);
    await stop();
  });

  test('a flush between two pushes shares one commit with three inputs in arrival order', async () => {
    const inner = memoryStore();
    let commits = 0;
    const inputs: string[] = [];
    const store: ScopeStore = { ...inner, commit: (writes) => { commits += 1; inputs.push(...writes.flatMap((w) => w.inputs.map(({ input }) => input.kind))); return inner.commit(writes); } };
    const { attend, traces, stop } = setup([{ route: 'skip' }], { store });
    await Promise.all([attend.push('s', [ev('a')]), attend.flush('s'), attend.push('s', [ev('b')])]);
    expect(commits).toBe(1);
    expect(inputs).toEqual(['events', 'flush', 'events']);
    expect((await attend.inspect('s'))?.version).toBe(3);
    expect(traces.map((t) => t.kind)).toEqual(['ingested', 'ingested']);
    expect((await attend.inspect('s'))?.buffer).toMatchObject({ forcedAt: 1000, entries: [{ id: 'a' }, { id: 'b' }] });
    await stop();
  });

  test('a merged write that fails rejects every push it carried', async () => {
    const inner = memoryStore();
    const store: ScopeStore = { ...inner, commit: () => Promise.reject(new Error('disk full')) };
    const { attend, stop } = setup([{ route: 'skip' }], { store });
    const first = attend.push('s', [ev('a')]);
    const second = attend.push('s', [ev('b')]);
    await expect(first).rejects.toThrow('disk full');
    await expect(second).rejects.toThrow('disk full');
    expect(await attend.inspect('s')).toBeNull();
    await stop();
  });

  test('batches a source emits without awaiting merge into one write with the last cursor and the gap kept', async () => {
    const inner = memoryStore();
    let commits = 0;
    const store: ScopeStore = { ...inner, commit: (writes) => (commits += 1, inner.commit(writes)) };
    const { attend, traces, stop } = setup([{ route: 'skip' }], { store });
    const source: Source = {
      key: 'src',
      async subscribe(_cursor, emit) {
        await Promise.all([emit({ events: [ev('a')], cursor: '1', truncated: true }), emit({ events: [ev('b')], cursor: '2' }), emit({ events: [ev('c')], cursor: '3' })]);
      },
    };
    await attend.attach('s', source);
    expect(commits).toBe(1);
    expect(await attend.inspect('s')).toMatchObject({ cursors: { src: '3' }, buffer: { gap: true, entries: [{ id: 'a' }, { id: 'b' }, { id: 'c' }] } });
    expect(traces.filter((t) => t.kind === 'gap')).toEqual([{ kind: 'gap', scope: 's', source: 'src', cursor: '3' }]);
    await stop();
  });

  test('a due tick fires under a steady stream of writes faster than its timer', async () => {
    const judge = scriptedJudge([{ route: 'skip' }]);
    const attend = createAttend({ store: memoryStore(), judge, sink: recordingSink(), kind: () => 'continuous', deadLetter: () => undefined, settle: { maxEvents: 10 } });
    const controller = new AbortController();
    const running = attend.run({ signal: controller.signal, sweepMs: 60_000 });
    let settledMidStream = false;
    for (let i = 0; i < 400; i += 1) {
      await attend.push('s', [ev(`e${i}`, 'drag')]);
      await new Promise((resolve) => setImmediate(resolve));
      if (judge.calls.length > 0) settledMidStream = true;
    }
    controller.abort();
    await running;
    expect(settledMidStream).toBe(true);
  });

  test('run resolves on abort and refuses to run twice', async () => {
    const h = setup([]);
    await expect(h.attend.run({ signal: new AbortController().signal })).rejects.toThrow('already running');
    await h.stop();
  });

  test('an aborted run waits for the judge in flight, persists its verdict, releases the delivery it will not start, and the next runner delivers at once', async () => {
    const store = memoryStore();
    const held = heldJudge();
    const first = setup([], { store, judge: held.judge });
    await first.attend.push('s', [ev('a')]);
    await first.clock.advance(500);
    const stopping = first.stop();
    expect(await pendingOrResolved(stopping)).toBe('pending');
    held.release({ route: 'queue' });
    await stopping;
    expect(first.sink.delivered).toEqual([]);
    expect(await first.attend.inspect('s')).toMatchObject({
      batch: null,
      outbox: { decisions: [{ item: { id: ID_A, kind: 'decision' }, phase: { phase: 'retrying', attempt: 0, failures: 0, at: 1500 } }], deadLetters: [] },
      dueAt: 1500,
    });
    expect(first.traces).toContainEqual({ kind: 'released', scope: 's', stage: 'sink', id: ID_A, attempt: 1 });
    const second = setup([], { store, config: { clock: first.clock } });
    await first.clock.advance(0);
    expect(second.sink.delivered.map((d) => [d.id, d.route, d.at])).toEqual([[ID_A, 'queue', 1500]]);
    expect(second.sink.attempts.get(ID_A)).toBe(1);
    expect(second.traces.filter((t) => t.kind === 'retrying')).toEqual([]);
    expect(first.sink.delivered).toEqual([]);
    await second.stop();
  });

  test('an aborted run releases the judge call for a window that settled on the late verdict, and the next runner judges it at once', async () => {
    const store = memoryStore();
    const held = heldJudge();
    const first = setup([], { store, judge: held.judge });
    await first.attend.push('s', [ev('a')]);
    await first.clock.advance(500);
    await first.attend.push('s', [ev('b')]);
    await first.clock.advance(500);
    const stopping = first.stop();
    held.release({ route: 'queue' });
    await stopping;
    expect(held.calls.map((c) => c.window.id)).toEqual([ID_A]);
    expect(await first.attend.inspect('s')).toMatchObject({
      batch: { window: { id: ID_B, reason: 'closing' }, phase: { phase: 'retrying', attempt: 0, failures: 0, at: 2000 } },
      outbox: { decisions: [{ item: { id: ID_A }, phase: { phase: 'retrying', attempt: 0, failures: 0, at: 2000 } }], deadLetters: [] },
      dueAt: 2000,
    });
    expect(first.traces.filter((t) => t.kind === 'released')).toEqual([
      { kind: 'released', scope: 's', stage: 'sink', id: ID_A, attempt: 1 },
      { kind: 'released', scope: 's', stage: 'judge', id: ID_B, attempt: 1 },
    ]);
    const second = setup([{ route: 'note' }], { store, config: { clock: first.clock } });
    await first.clock.advance(0);
    expect(second.judge.calls.map((c) => [c.window.id, c.window.reason])).toEqual([[ID_B, 'closing']]);
    expect(second.sink.delivered.map((d) => [d.id, d.route, d.at])).toEqual([
      [ID_A, 'queue', 2000],
      [ID_B, 'note', 2000],
    ]);
    expect(second.traces.filter((t) => t.kind === 'retrying')).toEqual([]);
    await second.stop();
  });

  test('an aborted run waits for a tick whose write is in flight, so the store sees it before run resolves', async () => {
    const slow = heldStore();
    const stuck: Sink = { deliver: () => new Promise(() => undefined) };
    const h = setup([{ route: 'queue' }], { store: slow.store, config: { sink: stuck } });
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(500);
    expect(await h.attend.inspect('s')).toMatchObject({ outbox: { decisions: [{ item: { id: ID_A }, phase: { phase: 'inFlight', attempt: 1, leaseUntil: 11_500 } }] } });
    slow.hold();
    await h.clock.advance(10_000);
    const stopping = h.stop();
    expect(await pendingOrResolved(stopping)).toBe('pending');
    expect(await h.attend.inspect('s')).toMatchObject({ outbox: { decisions: [{ phase: { phase: 'inFlight' } }] } });
    slow.release();
    await stopping;
    expect(await h.attend.inspect('s')).toMatchObject({
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
    expect(await h.attend.inspect('s')).toBeNull();
    const stopping = h.stop();
    expect(await pendingOrResolved(stopping)).toBe('pending');
    slow.release();
    await stopping;
    expect(await h.attend.inspect('s')).toMatchObject({ version: 1, cursors: { k: '1' }, buffer: { entries: [{ id: 'x', kind: 'consequence' }] } });
    await attached;
  });

  test('an aborted run prevents a source from writing after it resolves, without waiting for source I/O', async () => {
    const h = setup([]);
    let release: () => void = () => undefined;
    const pending = new Promise<void>((resolve) => {
      release = () => resolve();
    });
    const source: Source = {
      key: 'k',
      async subscribe(_cursor, emit, signal) {
        await emit({ events: [ev('x', 'out')], cursor: '1' });
        await new Promise<void>((resolve) => signal.addEventListener('abort', () => resolve(), { once: true }));
        await pending;
        for (const id of ['y', 'z']) {
          await emit({ events: [ev(id, 'out')], cursor: id });
          await realTick();
        }
      },
    };
    const attached = h.attend.attach('s', source);
    await drain();
    await h.stop();
    expect(await h.attend.inspect('s')).toMatchObject({ version: 1, cursors: { k: '1' }, buffer: { entries: [{ id: 'x' }] } });
    release();
    for (let turn = 0; turn < 3; turn += 1) {
      await realTick();
      await h.clock.advance(1000);
      expect(await h.attend.inspect('s')).toMatchObject({ version: 1, cursors: { k: '1' }, buffer: { entries: [{ id: 'x' }] } });
    }
    await attached;
  });

  test('an aborted run resolves while a producer keeps pushing on the same instance, and those pushes still persist', async () => {
    const h = setup([], { store: pacedStore() });
    let pushes = 0;
    let stop = false;
    const producer = (async () => {
      while (!stop) {
        pushes += 1;
        await h.attend.push('s', [ev(`p${pushes}`, 'look')]);
      }
    })();
    await realTick();
    const stopping = h.stop();
    const outcome = await Promise.race([stopping.then(() => 'resolved'), new Promise((resolve) => setTimeout(() => resolve('pending after 100 ms'), 100))]);
    expect(outcome).toBe('resolved');
    const pushesAtResolve = pushes;
    await realTick();
    stop = true;
    await producer;
    expect(pushes).toBeGreaterThan(pushesAtResolve);
    expect((await h.attend.inspect('s'))?.version).toBe(pushes);
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
    confidence: null,
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

function recordedStore(durable = false) {
  const inner = memoryStore();
  const groups: import('../src/types.js').ScopeWrite[][] = [];
  const store: ScopeStore = { ...inner, durable, async commit(writes) { groups.push([...writes]); return inner.commit(writes); } };
  return { store, groups, writes: () => groups.flat() };
}

function manualClock(start = 1000) {
  let now = start;
  const timers = new Set<{ at: number; fn(): void }>();
  return {
    now: () => now,
    at(at: number, fn: () => void) { const timer = { at, fn }; timers.add(timer); return () => { timers.delete(timer); }; },
    set(at: number) { now = at; },
    fire() { const first = [...timers].sort((a, b) => a.at - b.at)[0]; if (first) { timers.delete(first); first.fn(); } },
  };
}

describe('owned shell', () => {
  test('end settles at the next tick as closing with wait, and an empty end writes nothing', async () => {
    const recorded = recordedStore();
    const h = setup([{ route: 'note' }], { store: recorded.store });
    await h.attend.end('empty');
    expect(recorded.groups).toEqual([]);
    await h.attend.push('s', [ev('a', 'out')]);
    await h.attend.end('s');
    expect(h.judge.calls).toEqual([]);
    await h.clock.advance(0);
    expect(h.judge.calls[0]?.window).toMatchObject({ reason: 'closing', settledAt: 1000 });
    expect(h.judge.calls[0]?.routes.map((r) => r.route)).toEqual(['interrupt', 'queue', 'note', 'ask', 'skip', 'wait']);
    expect(h.sink.delivered.map((d) => d.id)).toEqual([ID_A]);
    await h.stop();
  });

  test('log modes append final rows and leave both delivery lanes empty', async () => {
    const h = setup([{ route: 'queue' }, { fail: 'refused', permanent: true }], { config: { sink: 'log', deadLetter: 'log' } });
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(500);
    await h.attend.push('s', [ev('b')]);
    await h.clock.advance(500);
    expect(h.sink.delivered).toEqual([]);
    expect(h.deadLetters).toEqual([]);
    expect((await h.store.log.read({ after: null, limit: 10 })).rows.map((row: import('../src/types.js').DecisionRow) => [row.kind, row.id])).toEqual([['decision', ID_A], ['deadLetter', ID_B]]);
    expect((await h.attend.inspect('s'))?.outbox).toEqual({ decisions: [], deadLetters: [] });
    await h.stop();
  });

  test('judgeConcurrency defers the second scope without spending an attempt or a failure', async () => {
    const held = heldJudge();
    const h = setup([], { judge: held.judge, config: { judgeConcurrency: 1 } });
    await Promise.all([h.attend.push('s', [ev('a')]), h.attend.push('t', [ev('b')])]);
    await h.clock.advance(500);
    expect(held.calls.map((call) => call.scope)).toEqual(['s']);
    expect(h.traces.filter((trace) => trace.kind === 'deferred')).toEqual([{ kind: 'deferred', scope: 't', batchId: held.calls[0] ? (await h.attend.inspect('t'))?.batch?.window.id : '', attempt: 1, until: 1600 }]);
    expect((await h.attend.inspect('t'))?.batch?.phase).toEqual({ phase: 'retrying', attempt: 0, failures: 0, at: 1600, offerWait: true });
    held.release({ route: 'queue' });
    await h.clock.advance(100);
    expect(held.calls.map((call) => call.scope)).toEqual(['s', 't']);
    expect(h.traces.flatMap((trace) => trace.kind === 'judging' && trace.scope === 't' ? [trace.attempt] : [])).toEqual([1, 1]);
    await h.stop();
  });

  test('admission counts every queued push even when the raw events merge', async () => {
    const held = heldStore();
    held.hold();
    const h = setup([], { store: held.store, run: false, config: { admission: { maxQueuedInputs: 2 } } });
    const first = h.attend.push('s', [ev('a')]);
    await drain();
    const second = h.attend.push('s', [ev('b')]);
    const third = h.attend.push('s', [ev('c')]);
    await expect(h.attend.push('s', [ev('d')])).rejects.toMatchObject({ name: 'BackpressureError', retryAfterMs: 100, queued: 2 });
    expect(h.traces).toContainEqual({ kind: 'backpressure', queued: 2, lagMs: 0 });
    held.release();
    await Promise.all([first, second, third]);
    expect((await h.attend.inspect('s'))?.buffer.entries.map((entry) => entry.id)).toEqual(['a', 'b', 'c']);
  });

  test('scheduler lag rejects intake with its rounded retry delay', async () => {
    const clock = manualClock();
    const h = setup([{ route: 'skip' }], { config: { clock, admission: { maxLagMs: 10 } } });
    await h.attend.push('s', [ev('a')]);
    clock.set(1623.2);
    clock.fire();
    await expect(h.attend.push('s', [ev('b')])).rejects.toMatchObject({ name: 'BackpressureError', retryAfterMs: 124, queued: 0 });
    expect(h.traces).toContainEqual({ kind: 'backpressure', queued: 0, lagMs: 123.20000000000005 });
    await h.stop();
  });

  test('an overdue deadline logs tick before events and closes the earlier window first', async () => {
    const clock = manualClock();
    const recorded = recordedStore();
    const h = setup([{ route: 'queue' }], { store: recorded.store, config: { clock } });
    await h.attend.push('s', [ev('a')]);
    clock.set(1800);
    await h.attend.push('s', [ev('b')]);
    expect(recorded.groups[1]?.[0]?.inputs.map(({ input }) => input.kind)).toEqual(['tick', 'events']);
    await drain();
    expect(h.judge.calls.map((call) => call.window.events.map((entry) => entry.id))).toEqual([['a']]);
    expect((await h.attend.inspect('s'))?.buffer.entries.map((entry) => entry.id)).toEqual(['b']);
    await h.stop();
  });

  test('version counts committed inputs, including multi-input writes and excluding no-ops', async () => {
    const recorded = recordedStore();
    const h = setup([], { store: recorded.store, run: false });
    await Promise.all([h.attend.push('s', [ev('a')]), h.attend.flush('s'), h.attend.push('s', [ev('b')])]);
    await h.attend.push('s', [ev('a')]);
    await h.attend.push('s', []);
    expect(recorded.groups).toHaveLength(1);
    expect(recorded.writes().map((write) => write.inputs.length)).toEqual([3]);
    expect((await h.attend.inspect('s'))?.version).toBe(3);
  });

  test('a duplicate waits for the open write to become durable', async () => {
    const held = heldStore();
    held.hold();
    const h = setup([], { store: held.store, run: false });
    const first = h.attend.push('s', [ev('a')]);
    const duplicate = h.attend.push('s', [ev('a')]);
    expect(await pendingOrResolved(duplicate)).toBe('pending');
    held.release();
    await Promise.all([first, duplicate]);
    expect((await h.attend.inspect('s'))?.version).toBe(1);
  });

  test('a changed policy snapshots even a no-op tick and recomputes the exact deadline', async () => {
    const recorded = recordedStore();
    const first = setup([], { store: recorded.store, run: false, settle: { closingGraceMs: 0 } });
    await first.attend.push('s', [ev('a')]);
    const second = setup([{ route: 'skip' }], { store: recorded.store, settle: { closingGraceMs: 750 } });
    await second.clock.advance(0);
    const write = recorded.writes()[1];
    expect(write?.inputs.map(({ input }) => input.kind)).toEqual(['tick']);
    expect(write?.snapshot?.policy.closingGraceMs).toBe(750);
    expect(write?.snapshot?.record.version).toBe(2);
    expect((await second.attend.inspect('s'))?.dueAt).toBe(1750);
    await second.stop();
  });

  test('three scopes share one sorted group and a failed member is isolated by bisection', async () => {
    const inner = memoryStore();
    const groups: string[][] = [];
    let blocked = true;
    let loads = 0;
    const store: ScopeStore = { ...inner, async load(scope) { if (scope === 'b') loads += 1; return inner.load(scope); }, async commit(writes) {
      groups.push(writes.map((write) => write.scope));
      if (blocked && writes.some((write) => write.scope === 'b')) throw new Error('b unavailable');
      return inner.commit(writes);
    } };
    const h = setup([], { store, run: false });
    const results = await Promise.allSettled(['c', 'a', 'b'].map((scope) => h.attend.push(scope, [ev(scope)])));
    expect(groups).toEqual([['a', 'b', 'c'], ['a'], ['b', 'c'], ['b'], ['c']]);
    expect(results.map((result) => result.status)).toEqual(['fulfilled', 'fulfilled', 'rejected']);
    expect((await h.attend.inspect('a'))?.seen).toEqual(['a']);
    expect((await h.attend.inspect('c'))?.seen).toEqual(['c']);
    expect(await h.attend.inspect('b')).toBeNull();
    blocked = false;
    await h.attend.push('b', [ev('recovered')]);
    expect(loads).toBe(3);
    expect((await h.attend.inspect('b'))?.seen).toEqual(['recovered']);
  });

  test('durable writes serialize each input, snapshot and append and only change rounded due when needed', async () => {
    const recorded = recordedStore(true);
    const h = setup([{ route: 'note' }], { store: recorded.store });
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(20);
    await h.attend.push('s', [ev('b')]);
    await h.clock.advance(500);
    for (const write of recorded.writes()) {
      expect(write.serialized).toEqual({ inputs: write.inputs.map((input) => JSON.stringify(input)), snapshot: write.snapshot ? JSON.stringify(write.snapshot.record) : null, appends: write.appends.map((append) => JSON.stringify(append.body)) });
    }
    expect(recorded.writes()[0]?.due).toBe(2000);
    expect(Object.hasOwn(recorded.writes()[1]!, 'due')).toBe(false);
    expect(recorded.writes().at(-1)?.due).toBeNull();
    await h.stop();
  });

  test('ratio snapshots start at the first write and trigger when the tail reaches the byte floor', async () => {
    const recorded = recordedStore();
    const h = setup([], { store: recorded.store, run: false });
    for (let i = 0; i < 37; i += 1) await h.attend.push('s', [{ ...ev(String(i).padStart(3, '0')), text: 'x' }]);
    expect(recorded.writes().flatMap((write, i) => write.snapshot ? [i + 1] : [])).toEqual([1]);
    await h.attend.push('s', [{ ...ev('037'), text: 'x' }]);
    expect(recorded.writes().flatMap((write, i) => write.snapshot ? [i + 1] : [])).toEqual([1, 38]);
    const fresh = setup([], { store: recorded.store, run: false });
    expect(await fresh.attend.inspect('s')).toEqual(await h.attend.inspect('s'));
  });

  test('a tail above 4096 inputs snapshots even below the byte ratio', async () => {
    const recorded = recordedStore();
    const h = setup([], { store: recorded.store, run: false, settle: { maxEvents: 5000, seenCount: 5000 } });
    await h.attend.push('s', [{ ...ev('seed'), text: 'x', data: { padding: 'x'.repeat(1000000) } }]);
    for (let i = 0; i < 4096; i += 1) await h.attend.push('s', [{ ...ev(String(i)), text: 'x' }]);
    expect(recorded.writes().filter((write) => write.snapshot)).toHaveLength(1);
    await h.attend.push('s', [{ ...ev('last'), text: 'x' }]);
    expect(recorded.writes().filter((write) => write.snapshot).map((write) => write.snapshot?.version)).toEqual([1, 4098]);
    expect((await h.attend.inspect('s'))?.version).toBe(4098);
  });

  test('always snapshot mode leaves no tail and replays the writer record', async () => {
    const recorded = recordedStore();
    const config: AttendConfig = { store: recorded.store, judge: async () => ({ route: 'skip' }), sink: 'log', deadLetter: 'log', kind, clock: fakeClock() };
    const attend = createAttendInternal(config, 'always');
    await attend.push('s', [ev('a')]);
    await attend.push('s', [ev('b')]);
    await attend.end('s');
    expect(recorded.writes().map((write) => write.snapshot?.version)).toEqual([1, 2, 3]);
    expect((await recorded.store.load('s'))?.tail).toEqual([]);
    expect(await createAttend(config).inspect('s')).toEqual(await attend.inspect('s'));
  });

  test('only idle quiescent slots are evicted at a sweep', async () => {
    const inner = memoryStore();
    const loads: string[] = [];
    const store: ScopeStore = { ...inner, async load(scope) { loads.push(scope); return inner.load(scope); } };
    const h = setup([{ route: 'skip' }], { store, config: { cache: { idleMs: 200 } } });
    await h.attend.push('s', [ev('a')]);
    await h.attend.push('busy', [ev('b', 'out')]);
    await h.clock.advance(1000);
    const before = loads.length;
    await h.attend.push('s', [ev('c')]);
    await h.attend.push('busy', [ev('d', 'out')]);
    expect(loads.slice(before)).toEqual(['s']);
    await h.stop();
  });

  test.each([
    ['derived', undefined, true, 2],
    ['explicit', 1601, true, 3],
    ['unbudgeted', undefined, false, 0],
  ] as const)('window budget: %s', async (_name, maxWindowBytes, budget, expectedCount) => {
    const judge = scriptedJudge([{ route: 'note' }]);
    if (budget) judge.budget = { contextTokens: 1000, reservedTokens: 200 };
    const h = setup([], { judge, settle: maxWindowBytes === undefined ? {} : { maxWindowBytes } });
    for (let i = 0; i < 3; i += 1) { await h.attend.push('s', [{ ...ev(String(i)), text: 'x'.repeat(755) }]); await h.clock.advance(0); }
    expect(judge.calls[0]?.window.reason ?? null).toBe(expectedCount ? 'size' : null);
    expect(judge.calls[0]?.window.events.length ?? 0).toBe(expectedCount);
    await h.stop();
  });

  test.each([0, 1.5, NaN, Infinity])('rejects invalid windowShare and actThreshold %s', (value) => {
    const config: AttendConfig = { store: memoryStore(), judge: async () => ({ route: 'skip' }), sink: 'log', deadLetter: 'log', kind };
    expect(() => createAttend({ ...config, windowShare: value })).toThrow(RangeError);
    expect(() => createAttend({ ...config, actThreshold: value })).toThrow(RangeError);
  });

  test('rejects a nonpositive or nonfinite derived judge budget', () => {
    const judge = Object.assign(async (): Promise<Judgment> => ({ route: 'skip' }), { budget: { contextTokens: 0 } });
    expect(() => createAttend({ store: memoryStore(), judge, sink: 'log', deadLetter: 'log', kind })).toThrow(RangeError);
  });

  test.each([
    [0.5, 'queue', 'interrupt'],
    [0.8, 'interrupt', 'queue'],
    [undefined, 'queue', 'queue'],
  ] as const)('actThreshold %s preserves probabilities and confidence while selecting %s as %s', async (threshold, route, expected) => {
    const h = setup([{ route, probabilities: { interrupt: 0.5, queue: 0.4, ask: 0.1 }, confidence: 0.7, predicates: { asked: 0.2 } }], { config: threshold === undefined ? {} : { actThreshold: threshold } });
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(500);
    expect(h.sink.delivered[0]).toMatchObject({ route: expected, probabilities: { interrupt: 0.5, queue: 0.4, ask: 0.1, note: 0, skip: 0, wait: 0 }, confidence: 0.7, predicates: { asked: 0.2 } });
    expect(h.traces.filter((trace) => trace.kind === 'routeThresholded')).toEqual(threshold === undefined ? [] : [{ kind: 'routeThresholded', scope: 's', batchId: ID_A, from: route, to: expected, act: 0.6 }]);
    await h.stop();
  });
});

function logRow(scope: string, id: string, route: Decision['route'] = 'queue', at = 1000): import('../src/types.js').DecisionAppend {
  const body: Decision = { id, scope, route, probabilities: { interrupt: 0, queue: 1, note: 0, ask: 0, skip: 0, wait: 0 }, predicates: {}, confidence: null,
    at, window: { id, events: [{ ...ev(id), kind: 'closing', seenAt: at }], reason: 'closing', waits: 0, dropped: 0, gap: false, settledAt: at } };
  return { kind: 'decision', id, scope, route, at, body };
}

async function appendRows(store: ScopeStore, rows: import('../src/types.js').DecisionAppend[]) {
  const byScope = new Map<string, import('../src/types.js').DecisionAppend[]>();
  for (const row of rows) {
    const group = byScope.get(row.scope) ?? [];
    group.push(row);
    byScope.set(row.scope, group);
  }
  const writes: import('../src/types.js').ScopeWrite[] = [];
  for (const [scope, appends] of byScope) writes.push({ scope, expectedVersion: (await store.load(scope))?.version ?? 0, inputs: [{ input: { kind: 'tick' }, now: 1000 }], snapshot: null, appends });
  await store.commit(writes);
}

describe('decision subscriptions', () => {
  test('a null cursor starts at the head, later pages are ordered deep copies, and abort ends iteration', async () => {
    const h = setup([], { run: false });
    await appendRows(h.store, [logRow('s', 'old')]);
    const controller = new AbortController();
    const iterator = h.attend.decisions({}, { cursor: null, signal: controller.signal })[Symbol.asyncIterator]();
    const first = await iterator.next();
    expect(first.value).toEqual({ rows: [], cursor: `v1.${await h.store.log.epoch()}.1`, truncated: false });
    await appendRows(h.store, [logRow('s', 'a'), logRow('s', 'b')]);
    const next = await iterator.next();
    expect(next.value?.rows.map((row: import('../src/types.js').DecisionRow) => row.id)).toEqual(['a', 'b']);
    const row = next.value?.rows[0];
    if (!row || row.kind !== 'decision') throw new Error('missing decision');
    row.body.window.events[0]!.data['mutated'] = true;
    row.body.route = 'interrupt';
    expect((await h.store.log.find('decision', 'a'))?.body).toMatchObject({ route: 'queue', window: { events: [{ data: {} }] } });
    controller.abort();
    expect(await iterator.next()).toEqual({ done: true, value: undefined });
  });

  test('a saved cursor resumes, and epochs, retention and age gaps mark only the first page', async () => {
    const h = setup([], { run: false });
    const signal = new AbortController();
    const epoch = await h.store.log.epoch();
    await appendRows(h.store, [logRow('s', 'a', 'queue', 1), logRow('s', 'b', 'note', 900), logRow('s', 'c', 'ask', 1000)]);
    const resumed = h.attend.decisions({}, { cursor: `v1.${epoch}.1`, signal: signal.signal })[Symbol.asyncIterator]();
    expect((await resumed.next()).value?.rows.map((row: import('../src/types.js').DecisionRow) => row.id)).toEqual(['b', 'c']);
    const unknown = h.attend.decisions({}, { cursor: 'v1.other.999', pageSize: 1, signal: signal.signal })[Symbol.asyncIterator]();
    expect((await unknown.next()).value).toMatchObject({ truncated: true, rows: [{ id: 'a' }] });
    expect((await unknown.next()).value).toMatchObject({ truncated: false, rows: [{ id: 'b' }] });
    const age = h.attend.decisions({}, { cursor: `v1.${epoch}.`, maxAgeMs: 200, signal: signal.signal })[Symbol.asyncIterator]();
    expect((await age.next()).value).toMatchObject({ truncated: true, rows: [{ id: 'b' }, { id: 'c' }] });
    await h.store.log.trim({ maxRows: 1 });
    const retained = h.attend.decisions({}, { cursor: `v1.${epoch}.1`, signal: signal.signal })[Symbol.asyncIterator]();
    expect((await retained.next()).value).toMatchObject({ truncated: true, rows: [{ id: 'c' }] });
    signal.abort();
    for (const iterator of [resumed, unknown, age, retained]) expect((await iterator.next()).done).toBe(true);
  });

  test('scope, prefix, route and kind filters return exactly their matching rows', async () => {
    const h = setup([], { run: false });
    const row = logRow('team/a', 'a');
    const dead: import('../src/types.js').DecisionAppend = { kind: 'deadLetter', id: 'dead', scope: 'team/a', at: 1000, route: null,
      body: { id: 'dead', scope: 'team/a', stage: 'judge', window: row.body.window, decision: null, message: 'refused', attempts: 1, permanent: true, at: 1000 } };
    await appendRows(h.store, [row, logRow('team/b', 'b', 'ask'), logRow('else', 'c', 'note'), dead]);
    const epoch = await h.store.log.epoch();
    const controller = new AbortController();
    for (const [filter, expected] of [
      [{ scope: 'team/a' }, ['a', 'dead']], [{ scopePrefix: 'team/' }, ['a', 'dead', 'b']],
      [{ routes: ['ask'] }, ['b']], [{ kinds: ['deadLetter'] }, ['dead']],
    ] satisfies [import('../src/shell/types.js').DecisionFilter, string[]][]) {
      const iterator = h.attend.decisions(filter, { cursor: `v1.${epoch}.`, signal: controller.signal })[Symbol.asyncIterator]();
      expect((await iterator.next()).value?.rows.map((entry: import('../src/types.js').DecisionRow) => entry.id)).toEqual(expected);
      await iterator.return?.();
    }
    controller.abort();
  });

  test('a rare prefix scans past a thousand other rows without waiting for an append', async () => {
    const inner = memoryStore();
    let waits = 0;
    const store: ScopeStore = { ...inner, log: { ...inner.log, wait: async () => { waits += 1; throw new Error('unexpected wait'); } } };
    const h = setup([], { store, run: false });
    await appendRows(store, [...Array.from({ length: 1000 }, (_, i) => logRow(`other/${i}`, String(i))), logRow('rare/a', 'found')]);
    const controller = new AbortController();
    const iterator = h.attend.decisions({ scopePrefix: 'rare/' }, { cursor: `v1.${await store.log.epoch()}.`, pageSize: 3, signal: controller.signal })[Symbol.asyncIterator]();
    let ids: string[] = [];
    for (let i = 0; i < 100 && ids.length === 0; i += 1) ids = (await iterator.next()).value?.rows.map((row: import('../src/types.js').DecisionRow) => row.id) ?? [];
    expect(ids).toEqual(['found']);
    expect(waits).toBe(0);
    controller.abort();
    expect((await iterator.next()).done).toBe(true);
  });

  test.each(['bad', 'v1..1', 'v1.epoch'])('rejects malformed cursor %s', async (cursor) => {
    const h = setup([], { run: false });
    const iterator = h.attend.decisions({}, { cursor, signal: new AbortController().signal })[Symbol.asyncIterator]();
    await expect(iterator.next()).rejects.toThrow(TypeError);
  });
});

describe('copying and scheduling', () => {
  test.each([['id', 1], ['name', null], ['text', 7], ['text', null]])('push and emit reject invalid %s without a write', async (field, value) => {
    const recorded = recordedStore();
    const h = setup([], { store: recorded.store, run: false });
    const event = ev('a');
    Reflect.set(event, field, value);
    let result: Promise<void> | undefined;
    expect(() => { result = h.attend.push('s', [event]); }).not.toThrow();
    await expect(result).rejects.toThrow(TypeError);
    const controller = new AbortController();
    await h.attend.attach('s', { key: 'bad', async subscribe(_cursor, emit) {
      await expect(emit({ events: [event] })).rejects.toThrow(TypeError);
      controller.abort();
    } }, { signal: controller.signal });
    expect(recorded.groups).toEqual([]);
  });

  test('copyEventSized equals the rendered estimate for arbitrary Unicode and nested JSON', () => {
    const unicode = fc.array(fc.integer({ min: 0, max: 0x10ffff }), { maxLength: 20 }).map((points) => String.fromCodePoint(...points));
    fc.assert(fc.property(unicode, unicode, fc.option(unicode, { nil: undefined }), fc.dictionary(unicode, fc.jsonValue()), (id, name, text, data) => {
      const original: Event = { id, name, at: 10, data: JSON.parse(JSON.stringify(data)), ...(text === undefined ? {} : { text }) };
      const copied = copyEventSized(original);
      expect(copied.bytes).toBe(estimateRenderedBytes(copied.event));
      expect(copied.event).toEqual(original);
      expect(copied.event.data).not.toBe(data);
    }), { numRuns: 200 });
  });

  test('copyWindowSliced copies every nested value and only yields above one slice', async () => {
    const window: Decision['window'] = { id: 'window', reason: 'closing', waits: 0, dropped: 0, gap: false, settledAt: 0,
      events: Array.from({ length: 300 }, (_, i) => ({ ...ev(String(i)), kind: 'closing', seenAt: i, data: { nested: { values: [i, null, '雪'] } } })) };
    let yielded = false;
    setImmediate(() => { yielded = true; });
    const small = await copyWindowSliced({ ...window, events: window.events.slice(0, 64) });
    expect(yielded).toBe(false);
    expect(small.events).toHaveLength(64);
    const large = await copyWindowSliced(window);
    expect(yielded).toBe(true);
    expect(large).toEqual(structuredClone(window));
    for (let i = 0; i < 300; i += 1) {
      expect(large.events[i]).not.toBe(window.events[i]);
      expect(large.events[i]?.data).not.toBe(window.events[i]?.data);
      expect(large.events[i]?.data['nested']).not.toBe(window.events[i]?.data['nested']);
    }
  });

  test('scheduler lazy arms once as a deadline moves later and fires in time and sequence order', async () => {
    const clock = fakeClock();
    let armed = 0;
    const seen: string[] = [];
    const scheduler = createScheduler({ clock: { now: clock.now, at(at, fn) { armed += 1; return clock.at(at, fn); } }, onDue: (scope) => seen.push(scope) });
    scheduler.arm('stream', 10);
    for (let at = 11; at < 1000; at += 1) scheduler.arm('stream', at);
    scheduler.arm('stream', null);
    expect(armed).toBe(1);
    scheduler.arm('b', 20);
    scheduler.arm('c', 20);
    scheduler.arm('a', 15);
    await clock.set(20);
    expect(seen).toEqual(['stream', 'a', 'b', 'c']);
    expect(scheduler.lagMs()).toBe(0);
    scheduler.stop();
    expect(clock.nextAt()).toBeNull();
  });

  test('scheduler slices yield through setImmediate and report actual lag', async () => {
    const clock = manualClock(100);
    const seen: string[] = [];
    let time = 0;
    const spy = vi.spyOn(performance, 'now').mockImplementation(() => time++);
    const scheduler = createScheduler({ clock, sliceUs: 50, onDue: (scope) => seen.push(scope) });
    try {
      scheduler.arm('a', 0);
      scheduler.arm('b', 0);
      scheduler.arm('c', 0);
      setImmediate(() => seen.push('yield'));
      clock.fire();
      expect(seen).toEqual(['a']);
      await drain();
      await drain();
      expect(seen).toEqual(['a', 'yield', 'b', 'c']);
      expect(scheduler.lagMs()).toBe(100);
      scheduler.stop();
    } finally { spy.mockRestore(); }
  });

  test('systemClock schedules past instants on setImmediate and cancellation prevents the call', async () => {
    const seen: string[] = [];
    await new Promise<void>((resolve) => setImmediate(() => {
      const clock = systemClock();
      clock.at(Date.now() - 1, () => seen.push('immediate'));
      clock.at(Date.now() - 1, () => seen.push('cancelled'))();
      setTimeout(() => { seen.push('timeout'); resolve(); }, 0);
    }));
    expect(seen).toEqual(['immediate', 'timeout']);
  });
});
