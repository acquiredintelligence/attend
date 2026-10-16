import fc from 'fast-check';
import { describe, expect, test } from 'vitest';
import { createAttend } from '../src/attend.js';
import { isPermanent } from '../src/errors.js';
import { DEFAULT_CRITERIA, DEFAULT_SYSTEM, llmJudge, type Complete, type CompleteRequest } from '../src/llm.js';
import { memoryStore } from '../src/memory.js';
import { fakeClock, recordingSink } from '../src/testing.js';
import type { DeadLetter, Event, JudgeInput, Kind } from '../src/types.js';

function fakeComplete(reply: unknown) {
  const requests: CompleteRequest[] = [];
  const complete: Complete = async (request) => {
    requests.push(request);
    return reply;
  };
  return { complete, requests };
}

function judgeInput(overrides?: Partial<JudgeInput>): JudgeInput {
  return {
    scope: 's',
    window: {
      id: 'w1',
      events: [
        { id: 'a', at: 0, name: 'commit', data: { sha: 'abc' }, text: 'Pushed 2 commits to main', kind: 'closing', seenAt: 1800 },
        { id: 'b', at: 0, name: 'ci', data: { status: 'failed' }, kind: 'consequence', seenAt: 4100 },
      ],
      reason: 'closing',
      waits: 1,
      dropped: 2,
      gap: true,
      settledAt: 4000,
    },
    routes: [{ route: 'interrupt' }, { route: 'queue' }, { route: 'note' }, { route: 'ask' }, { route: 'skip' }],
    guidance: 'An agent that reviews pull requests.',
    predicates: [{ name: 'asked', question: 'Did the user ask for this?' }],
    facts: { turn: 'idle' },
    recent: [{ id: 'old', route: 'note', at: 1000 }],
    now: 5000,
    ...overrides,
  };
}

const signal = new AbortController().signal;
const FIVE = ['interrupt', 'queue', 'note', 'ask', 'skip'];

describe('llmJudge', () => {
  test('asks for a strict schema over exactly the offered routes and the asked predicates', async () => {
    const fake = fakeComplete({ route: 'skip', probabilities: {}, predicates: {} });
    const judge = llmJudge({ complete: fake.complete });
    await judge(judgeInput(), signal);
    await judge(judgeInput({ routes: [{ route: 'queue' }, { route: 'skip' }, { route: 'wait' }], predicates: [] }), signal);
    expect(fake.requests.map((r) => r.schema)).toEqual([
      {
        type: 'object',
        properties: {
          route: { type: 'string', enum: ['interrupt', 'queue', 'note', 'ask', 'skip'] },
          probabilities: {
            type: 'object',
            properties: { interrupt: { type: 'number' }, queue: { type: 'number' }, note: { type: 'number' }, ask: { type: 'number' }, skip: { type: 'number' } },
            required: ['interrupt', 'queue', 'note', 'ask', 'skip'],
            additionalProperties: false,
          },
          predicates: {
            type: 'object',
            properties: { asked: { type: 'number' } },
            required: ['asked'],
            additionalProperties: false,
          },
        },
        required: ['route', 'probabilities', 'predicates'],
        additionalProperties: false,
      },
      {
        type: 'object',
        properties: {
          route: { type: 'string', enum: ['queue', 'skip', 'wait'] },
          probabilities: {
            type: 'object',
            properties: { queue: { type: 'number' }, skip: { type: 'number' }, wait: { type: 'number' } },
            required: ['queue', 'skip', 'wait'],
            additionalProperties: false,
          },
        },
        required: ['route', 'probabilities'],
        additionalProperties: false,
      },
    ]);
  });

  test('renders the window, facts, recent routes, predicates and guidance as compact JSON', async () => {
    const fake = fakeComplete({ route: 'note' });
    await llmJudge({ complete: fake.complete })(judgeInput(), signal);
    expect(fake.requests[0]?.prompt).toBe(
      '{"guidance":"An agent that reviews pull requests.","window":{"reason":"closing","waits":1,"dropped":2,"gap":true,"events":[' +
        '{"kind":"closing","name":"commit","text":"Pushed 2 commits to main","ago":"3.2s"},' +
        '{"kind":"consequence","name":"ci","data":{"status":"failed"},"ago":"0.9s"}]},' +
        '"facts":{"turn":"idle"},"recent":[{"route":"note","ago":"4.0s"}],"predicates":{"asked":"Did the user ask for this?"}}',
    );
  });

  test('passes the input images to complete, and an empty list when there are none', async () => {
    const fake = fakeComplete({ route: 'note' });
    const images = [{ data: 'iVBORw0KGgo=', mimeType: 'image/png' as const }];
    await llmJudge({ complete: fake.complete })(judgeInput({ images }), signal);
    await llmJudge({ complete: fake.complete })(judgeInput(), signal);
    expect(fake.requests.map((r) => r.images)).toEqual([images, []]);
  });

  test('a custom render replaces the prompt', async () => {
    const fake = fakeComplete({ route: 'note' });
    await llmJudge({ complete: fake.complete, render: (input) => `scope ${input.scope}, ${input.window.events.length} events` })(judgeInput(), signal);
    expect(fake.requests[0]?.prompt).toBe('scope s, 2 events');
  });

  test("criteria come from the input first, then the judge's options, then the defaults", async () => {
    const fake = fakeComplete({ route: 'skip' });
    const input = judgeInput({ routes: [{ route: 'interrupt', criterion: 'input interrupt' }, { route: 'queue' }, { route: 'skip' }], predicates: [] });
    await llmJudge({ complete: fake.complete, system: 'Decide.', criteria: { interrupt: 'option interrupt', queue: 'option queue' } })(input, signal);
    await llmJudge({ complete: fake.complete })(judgeInput({ routes: [{ route: 'skip' }] }), signal);
    expect(fake.requests.map((r) => r.system)).toEqual([
      'Decide.\n\nRoutes on offer:\n- interrupt: input interrupt\n- queue: option queue\n' +
        `- skip: ${DEFAULT_CRITERIA.skip}\n\nAnswer with the route you choose and a probability from 0 to 1 for each route on offer.`,
      `${DEFAULT_SYSTEM}\n\nRoutes on offer:\n- skip: ${DEFAULT_CRITERIA.skip}\n\n` +
        'Answer with the route you choose, a probability from 0 to 1 for each route on offer, and for each entry under predicates the probability from 0 to 1 that the answer to its question is yes.',
    ]);
  });

  test('keeps the finite numbers under offered routes and asked predicates', async () => {
    const reply = {
      route: 'queue',
      probabilities: { interrupt: 0.1, queue: 0.7, note: '0.2', skip: Number.POSITIVE_INFINITY, wait: 0.4 },
      predicates: { asked: 0.25, unasked: 1 },
    };
    const judgment = await llmJudge({ complete: fakeComplete(reply).complete })(judgeInput(), signal);
    expect(judgment).toEqual({ route: 'queue', probabilities: { interrupt: 0.1, queue: 0.7 }, predicates: { asked: 0.25 } });
  });

  test('a reply with garbled ride-alongs still yields its route', async () => {
    const judgment = await llmJudge({ complete: fakeComplete({ route: 'ask', probabilities: [0.9], predicates: 'yes' }).complete })(judgeInput(), signal);
    expect(judgment).toEqual({ route: 'ask', probabilities: {}, predicates: {} });
  });

  test('a malformed reply throws a plain, retryable Error', async () => {
    const replies: unknown[] = [null, '{"route":"queue"}', ['queue'], { route: 'wait' }, { probabilities: { queue: 1 } }];
    const outcomes: [string, string][] = [];
    for (const reply of replies) {
      try {
        await llmJudge({ complete: fakeComplete(reply).complete })(judgeInput(), signal);
        outcomes.push(['resolved', '']);
      } catch (error) {
        outcomes.push(error instanceof Error ? [error.name, error.message] : ['thrown', String(error)]);
      }
    }
    expect(outcomes).toEqual([
      ['Error', 'llmJudge: expected a JSON object, got null'],
      ['Error', 'llmJudge: expected a JSON object, got string'],
      ['Error', 'llmJudge: expected a JSON object, got an array'],
      ['Error', `llmJudge: route "wait" is not one of ${FIVE.join(', ')}`],
      ['Error', `llmJudge: route undefined is not one of ${FIVE.join(', ')}`],
    ]);
  });

  test('passes the abort signal through to complete', async () => {
    const fake = fakeComplete({ route: 'note' });
    const controller = new AbortController();
    await llmJudge({ complete: fake.complete })(judgeInput(), controller.signal);
    controller.abort();
    expect(fake.requests[0]?.signal.aborted).toBe(true);
  });
});

describe('llmJudge inside createAttend', () => {
  test('a settled window is judged through complete and delivered to the sink', async () => {
    const clock = fakeClock(1000);
    const sink = recordingSink();
    const deadLetters: DeadLetter[] = [];
    const fake = fakeComplete({
      route: 'interrupt',
      probabilities: { interrupt: 0.625, queue: 0.25, note: 0.125, ask: 0, skip: 0, wait: 0 },
      predicates: { asked: 0.75 },
    });
    const attend = createAttend({
      store: memoryStore(),
      judge: llmJudge({ complete: fake.complete, criteria: { interrupt: 'judge interrupt' } }),
      sink,
      kind: (e: Event): Kind => (e.name === 'commit' ? 'closing' : 'consequence'),
      deadLetter: (letter) => {
        deadLetters.push(letter);
      },
      criteria: { interrupt: 'app interrupt' },
      predicates: { asked: 'Did the user ask for this?' },
      clock,
    });
    const controller = new AbortController();
    const running = attend.run({ signal: controller.signal });

    await attend.push('s', [{ id: 'a', at: 0, name: 'commit', data: {}, text: 'commit a' }]);
    await clock.advance(500);

    expect(fake.requests[0]?.prompt).toBe(
      '{"window":{"reason":"closing","waits":0,"dropped":0,"gap":false,"events":[{"kind":"closing","name":"commit","text":"commit a","ago":"0.5s"}]},' +
        '"facts":{},"recent":[],"predicates":{"asked":"Did the user ask for this?"}}',
    );
    expect(fake.requests[0]?.system.split('\n').filter((line) => line.startsWith('- '))).toEqual([
      '- interrupt: app interrupt',
      `- queue: ${DEFAULT_CRITERIA.queue}`,
      `- note: ${DEFAULT_CRITERIA.note}`,
      `- ask: ${DEFAULT_CRITERIA.ask}`,
      `- skip: ${DEFAULT_CRITERIA.skip}`,
      `- wait: ${DEFAULT_CRITERIA.wait}`,
    ]);
    const window = {
      id: '3ecbd4f2ef91c78f',
      events: [{ id: 'a', at: 0, name: 'commit', data: {}, text: 'commit a', kind: 'closing', seenAt: 1000 }],
      reason: 'closing',
      waits: 0,
      dropped: 0,
      gap: false,
      settledAt: 1500,
    };
    expect(sink.delivered).toEqual([
      {
        id: '3ecbd4f2ef91c78f',
        scope: 's',
        route: 'interrupt',
        probabilities: { interrupt: 0.625, queue: 0.25, note: 0.125, ask: 0, skip: 0, wait: 0 },
        predicates: { asked: 0.75 },
        confidence: null,
        window,
        at: 1500,
      },
    ]);
    expect(deadLetters).toEqual([]);

    controller.abort();
    await running;
  });
});

const CONTEXT_ERROR = '400 {"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: 210000 tokens > 200000 maximum"}}';
const contextError = () => Object.assign(new Error(CONTEXT_ERROR), { status: 400 });
const entry = (id: string, kind: Kind, text = id): JudgeInput['window']['events'][number] => ({ id, name: id, kind, text, data: {}, at: 0, seenAt: 0 });
function windowInput(events: JudgeInput['window']['events'], dropped = 0): JudgeInput {
  return judgeInput({ window: { id: 'w', events, reason: 'closing', waits: 0, dropped, gap: false, settledAt: 0 }, guidance: '', facts: {}, recent: [], predicates: [], now: 0 });
}
function expectedState(input: JudgeInput, events = input.window.events, dropped = input.window.dropped) {
  const ago = (at: number) => `${((input.now - at) / 1000).toFixed(1)}s`;
  return {
    ...(input.guidance === undefined ? {} : { guidance: input.guidance }),
    window: {
      reason: input.window.reason,
      waits: input.window.waits,
      dropped,
      gap: input.window.gap,
      events: events.map((e) => (e.text === undefined ? { kind: e.kind, name: e.name, data: e.data, ago: ago(e.seenAt) } : { kind: e.kind, name: e.name, text: e.text, ago: ago(e.seenAt) })),
    },
    facts: input.facts,
    recent: input.recent.map((v) => ({ route: v.route, ago: ago(v.at) })),
    predicates: Object.fromEntries(input.predicates.map((p) => [p.name, p.question])),
  };
}
const sheddable = (kind: Kind) => kind === 'consequence' || kind === 'context';
const windowArb = fc
  .tuple(
    fc.array(
      fc.record({
        kind: fc.constantFrom<Kind>('closing', 'continuous', 'passive', 'flush', 'consequence', 'context'),
        payload: fc.oneof(fc.string({ maxLength: 200 }), fc.record({ value: fc.string({ maxLength: 200 }) })),
      }),
      { maxLength: 40 },
    ),
    fc.integer({ min: 0, max: 1000 }),
  )
  .map(([events, dropped]) =>
    windowInput(
      events.map(({ kind, payload }, index) => (typeof payload === 'string' ? entry(`e${index}`, kind, payload) : { id: `e${index}`, name: `e${index}`, kind, data: payload, at: 0, seenAt: 0 })),
      dropped,
    ),
  );
const TWO = windowInput([entry('first', 'closing'), entry('second', 'closing')]);
const HALF_PROMPT =
  '{"guidance":"","window":{"reason":"closing","waits":0,"dropped":1,"gap":false,"events":[{"kind":"closing","name":"second","text":"second","ago":"0.0s"}]},"facts":{},"recent":[],"predicates":{}}';
const TRIM_INPUT = windowInput(
  [
    entry('old', 'consequence', 'x'.repeat(200)),
    entry('act', 'closing'),
    entry('context', 'context', 'y'.repeat(200)),
    entry('flush', 'flush'),
    entry('passive', 'passive'),
    entry('continuous', 'continuous'),
  ],
  8,
);
const TRIM_PROMPT =
  '{"guidance":"","window":{"reason":"closing","waits":0,"dropped":10,"gap":false,"events":[{"kind":"closing","name":"act","text":"act","ago":"0.0s"},{"kind":"flush","name":"flush","text":"flush","ago":"0.0s"},{"kind":"passive","name":"passive","text":"passive","ago":"0.0s"},{"kind":"continuous","name":"continuous","text":"continuous","ago":"0.0s"}]},"facts":{},"recent":[],"predicates":{}}';

describe('llmJudge budgets', () => {
  test('declares measured budgets only when requested and validates the allowance', () => {
    const complete = async () => ({ route: 'skip' });
    expect('budget' in llmJudge({ complete })).toBe(false);
    expect(llmJudge({ complete, contextTokens: 200_000 }).budget).toEqual({ contextTokens: 200_000, reservedTokens: 952 });
    expect(llmJudge({ complete, contextTokens: 200_000, system: 'Be quiet.', criteria: { skip: 'Ignore it.' } }).budget).toEqual({ contextTokens: 200_000, reservedTokens: 359 });
    for (const contextTokens of [952, 951, NaN])
      expect(() => llmJudge({ complete, contextTokens })).toThrow(new RangeError(`llmJudge: contextTokens must be more than the 952 tokens its own prompt takes, got ${contextTokens}`));
  });
  test('trims oldest sheddable events and preserves every act and flush', async () => {
    const fake = fakeComplete({ route: 'skip' });
    expect(await llmJudge({ complete: fake.complete, contextTokens: 1062 })(TRIM_INPUT, signal)).toEqual({ route: 'skip', probabilities: {}, predicates: {} });
    expect(fake.requests.map((r) => r.prompt)).toEqual([TRIM_PROMPT]);
    expect(TRIM_INPUT.window.events.map((e) => e.id)).toEqual(['old', 'act', 'context', 'flush', 'passive', 'continuous']);
    expect(TRIM_INPUT.window.dropped).toBe(8);
  });
  test('custom render receives the trimmed input', async () => {
    const seen: unknown[] = [];
    const fake = fakeComplete({ route: 'skip' });
    expect(
      await llmJudge({
        complete: fake.complete,
        contextTokens: 1062,
        render: (input) => {
          seen.push([input.window.events.map((e) => e.id), input.window.dropped]);
          return 'custom';
        },
      })(TRIM_INPUT, signal),
    ).toEqual({ route: 'skip', probabilities: {}, predicates: {} });
    expect(seen).toEqual([[['act', 'flush', 'passive', 'continuous'], 10]]);
    expect(fake.requests.map((r) => r.prompt)).toEqual(['custom']);
  });
  test('retries context length exactly once with the halved prompt', async () => {
    const requests: string[] = [];
    const judge = llmJudge({
      complete: async ({ prompt }) => {
        requests.push(prompt);
        if (requests.length === 1) throw contextError();
        return { route: 'skip' };
      },
    });
    expect(await judge(TWO, signal)).toEqual({ route: 'skip', probabilities: {}, predicates: {} });
    expect(requests).toHaveLength(2);
    expect(requests[1]).toBe(HALF_PROMPT);
  });
  test('second context rejection is a plain retryable error with provider fields', async () => {
    let calls = 0;
    const original = Object.assign(contextError(), { permanent: true, extra: 'omit', requestId: 'sdk-id', retryAfterMs: 7 });
    const judge = llmJudge({
      complete: async () => {
        calls += 1;
        throw original;
      },
    });
    const error: unknown = await judge(TWO, signal).catch((e: unknown) => e);
    expect(calls).toBe(2);
    expect(error).toBeInstanceOf(Error);
    expect(Object.getPrototypeOf(error)).toBe(Error.prototype);
    expect(error).toMatchObject({ message: `${CONTEXT_ERROR}; rejected again with the window halved`, status: 400, cause: original, requestId: 'sdk-id', retryAfterMs: 7 });
    expect(isPermanent(error)).toBe(false);
    expect(error).not.toHaveProperty('extra');
    expect(error).not.toHaveProperty('permanent');
  });
  test('one-event rejection cannot be halved and sends once', async () => {
    let calls = 0;
    const judge = llmJudge({
      complete: async () => {
        calls += 1;
        throw contextError();
      },
    });
    const error: unknown = await judge(windowInput([entry('one', 'closing')]), signal).catch((e: unknown) => e);
    expect(calls).toBe(1);
    expect(error).toMatchObject({ message: `${CONTEXT_ERROR}; the window cannot be halved`, status: 400 });
    expect(isPermanent(error)).toBe(false);
  });
  test.each([
    new Error('context length'),
    Object.assign(new Error('unrelated'), { status: 400 }),
    Object.assign(new Error('context length'), { status: 422 }),
    Object.assign(new Error('unrelated'), { status: 413 }),
  ])('non-context error is rethrown unchanged: %s', async (error) => {
    let calls = 0;
    const judge = llmJudge({
      complete: async () => {
        calls += 1;
        throw error;
      },
    });
    await expect(judge(TWO, signal)).rejects.toBe(error);
    expect(calls).toBe(1);
  });
  test('SDK context-length code is recognized without a matching message', async () => {
    let calls = 0;
    const judge = llmJudge({
      complete: async () => {
        calls += 1;
        if (calls === 1) throw Object.assign(new Error('bad request'), { status: 413, code: 'context_length_exceeded' });
        return { route: 'skip' };
      },
    });
    expect(await judge(TWO, signal)).toEqual({ route: 'skip', probabilities: {}, predicates: {} });
    expect(calls).toBe(2);
  });
  test('trimming preserves acts, exact rendering and oldest-first shedding (500 runs)', async () => {
    await fc.assert(
      fc.asyncProperty(windowArb, fc.integer({ min: 1, max: 3000 }), async (input, k) => {
        const fake = fakeComplete({ route: 'skip' });
        expect(await llmJudge({ complete: fake.complete, contextTokens: 952 + k })(input, signal)).toEqual({ route: 'skip', probabilities: {}, predicates: {} });
        expect(fake.requests).toHaveLength(1);
        const prompt = fake.requests[0]?.prompt ?? '';
        const parsed = JSON.parse(prompt);
        const names: string[] = parsed.window.events.map((e: { name: string }) => e.name);
        const kept = input.window.events.filter((e) => names.includes(e.name));
        const cut = input.window.events.filter((e) => !names.includes(e.name));
        const shed = input.window.events.filter((e) => sheddable(e.kind));
        expect(names).toEqual(kept.map((e) => e.name));
        expect(kept.filter((e) => !sheddable(e.kind))).toEqual(input.window.events.filter((e) => !sheddable(e.kind)));
        expect(cut).toEqual(shed.slice(0, cut.length));
        expect(parsed.window.dropped).toBe(input.window.dropped + cut.length);
        expect(prompt.length <= k * 4 || kept.every((e) => !sheddable(e.kind))).toBe(true);
        expect(prompt).toBe(JSON.stringify(expectedState(input, kept, input.window.dropped + cut.length)));
      }),
      { numRuns: 500 },
    );
  });
  test('halving preserves flushes, class order and one event (500 runs)', async () => {
    await fc.assert(
      fc.asyncProperty(windowArb, async (input) => {
        const prompts: string[] = [];
        const judge = llmJudge({
          complete: async ({ prompt }) => {
            prompts.push(prompt);
            if (prompts.length === 1) throw contextError();
            return { route: 'skip' };
          },
        });
        const result: unknown = await judge(input, signal).catch((error: unknown) => error);
        expect(prompts.length).toBeGreaterThan(0);
        const cuttable = input.window.events.length > 1 && input.window.events.some((e) => e.kind !== 'flush');
        expect(prompts).toHaveLength(cuttable ? 2 : 1);
        if (!cuttable) {
          expect(result).toBeInstanceOf(Error);
          return;
        }
        expect(result).toEqual({ route: 'skip', probabilities: {}, predicates: {} });
        const parsed = JSON.parse(prompts[1] ?? '');
        const names: string[] = parsed.window.events.map((e: { name: string }) => e.name);
        const kept = input.window.events.filter((e) => names.includes(e.name));
        const cut = input.window.events.filter((e) => !names.includes(e.name));
        expect(kept.length).toBeGreaterThanOrEqual(1);
        expect(names).toEqual(kept.map((e) => e.name));
        expect(kept.filter((e) => e.kind === 'flush')).toEqual(input.window.events.filter((e) => e.kind === 'flush'));
        if (kept.some((e) => sheddable(e.kind))) expect(cut.every((e) => sheddable(e.kind))).toBe(true);
        for (const shedding of [true, false]) {
          const group = input.window.events.filter((e) => e.kind !== 'flush' && sheddable(e.kind) === shedding);
          expect(cut.filter((e) => sheddable(e.kind) === shedding)).toEqual(group.slice(0, cut.filter((e) => sheddable(e.kind) === shedding).length));
        }
        const total = expectedState(input).window.events.reduce((sum, e) => sum + JSON.stringify(e).length, 0);
        const remaining = parsed.window.events.reduce((sum: number, e: unknown) => sum + JSON.stringify(e).length, 0);
        expect(remaining <= Math.floor(total / 2) || kept.length === 1 || kept.every((e) => e.kind === 'flush')).toBe(true);
        expect(prompts[1]).toBe(JSON.stringify(expectedState(input, kept, input.window.dropped + cut.length)));
      }),
      { numRuns: 500 },
    );
  });
  test('rendering yields every 64 events', async () => {
    const turns: number[] = [];
    for (const length of [1, 64, 65, 128, 129, 200]) {
      let count = 0;
      let timer: NodeJS.Immediate;
      const tick = () => {
        count += 1;
        timer = setImmediate(tick);
      };
      timer = setImmediate(tick);
      try {
        expect(
          await llmJudge({
            complete: async () => {
              turns.push(count);
              return { route: 'skip' };
            },
          })(windowInput(Array.from({ length }, (_, i) => entry(String(i), 'closing'))), signal),
        ).toEqual({ route: 'skip', probabilities: {}, predicates: {} });
      } finally {
        clearImmediate(timer);
      }
    }
    expect(turns).toEqual([0, 0, 1, 1, 2, 3]);
  });
  test('aborting during a render throws the signal reason before complete', async () => {
    const controller = new AbortController();
    const reason = new Error('stop rendering');
    let calls = 0;
    setImmediate(() => controller.abort(reason));
    const judge = llmJudge({
      complete: async () => {
        calls += 1;
        return { route: 'skip' };
      },
    });
    await expect(judge(windowInput(Array.from({ length: 129 }, (_, i) => entry(String(i), 'closing'))), controller.signal)).rejects.toBe(reason);
    expect(calls).toBe(0);
  });
});
