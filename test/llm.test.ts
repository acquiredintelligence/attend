import { describe, expect, test } from 'vitest';
import { createAttend } from '../src/attend.js';
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
