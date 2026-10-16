import { describe, expect, test } from 'vitest';
import { createAttend, defaultNotice, logSource, OutboxFullError, routes, type AttendConfig } from '../src/attend.js';
import { memoryLog, memoryStore } from '../src/memory.js';
import { fakeClock, recordingSink, scriptedJudge, type ScriptedAnswer } from '../src/testing.js';
import type { DeadLetter, Decision, Event, Judge, Kinded, SettlePolicy, StateStore, Trace } from '../src/types.js';

const kind = (e: Event): Kinded => {
  switch (e.name) {
    case 'close':
      return { kind: 'closing' };
    case 'say':
      return { kind: 'flush' };
    case 'out':
      return { kind: 'consequence' };
    default:
      return { kind: 'context' };
  }
};

const ev = (id: string, name = 'close'): Event => ({ id, at: 0, name, data: {}, text: `${name} ${id}` });
const ID_A = 'af63dc4c8601ec8c';
const ID_AB = 'e5beb1190415e670';

function setup(script: ScriptedAnswer[], extra?: { settle?: Partial<SettlePolicy>; judge?: Judge; store?: StateStore; config?: Partial<AttendConfig> }) {
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
  const done = attend.run({ signal: controller.signal, sweepMs: 1000 });
  const stop = async () => {
    controller.abort();
    await done;
  };
  return { clock, store, judge, sink, deadLetters, traces, attend, stop };
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
        window: { id: ID_A, events: [{ ...ev('a'), kind: 'closing', seenAt: 1000 }], reason: 'closing', waits: 0, dropped: 0, gap: false, settledAt: 1500 },
        at: 1500,
      },
    ]);
    const record = await h.attend.inspect('s');
    expect(record).toMatchObject({ version: 4, batch: null, outbox: [], recent: [{ id: ID_A, route: 'queue', at: 1500 }], dueAt: null });
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

  test('a wait carries the window into the next settle', async () => {
    const h = setup([{ route: 'wait' }, { route: 'queue' }]);
    await h.attend.push('s', [ev('a')]);
    await h.clock.advance(500);
    expect(h.judge.calls).toHaveLength(1);
    expect(await h.attend.inspect('s')).toMatchObject({ batch: { phase: 'waiting', waits: 1, until: 11_500 } });
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
    expect(await h.attend.inspect('s')).toMatchObject({ batch: null, outbox: [] });
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
    expect(await h.attend.inspect('s')).toMatchObject({ batch: { phase: 'judging', leaseUntil: 1500 + 30_000 }, dueAt: 31_500 });
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
    expect(await h.attend.inspect('s')).toMatchObject({ outbox: [] });
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
    expect(await h.attend.inspect('s')).toMatchObject({ outbox: [] });
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
    await new Promise((r) => setTimeout(r, 10));
    expect(h.traces).toContainEqual({ kind: 'backpressure', scope: 's', outbox: 1 });
    await h.clock.advance(1000);
    await new Promise((r) => setTimeout(r, 10));
    expect(h.sink.delivered.map((d) => d.id)).toEqual([ID_A]);
    expect((await h.attend.inspect('s'))?.buffer.entries.map((e) => e.id)).toEqual(['c']);
    await h.stop();
    await attached;
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

  test('a writer that never wins gives up after eight conflicts', async () => {
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
    await expect(h.attend.push('s', [ev('a')])).rejects.toThrow('after 8 conflicts');
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
    expect((await store.get('s'))?.cursor).toBe('00000000000000000002');
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

  test('run resolves on abort and refuses to run twice', async () => {
    const h = setup([]);
    await expect(h.attend.run({ signal: new AbortController().signal })).rejects.toThrow('already running');
    await h.stop();
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
      ['Some events before these were lost.', 'close a', 'build.done {"ok":true}', '(2 background events left out)', 'Take this up when the current step is done.'].join('\n'),
    );
    expect(defaultNotice(decision('interrupt'), { maxLines: 1 }).split('\n')[0]).toBe('(1 earlier events left out)');
  });
});
