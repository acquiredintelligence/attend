import { describe, expect, test } from 'vitest';
import { decisionsJudge } from '../src/decisions.js';
import { isPermanent } from '../src/errors.js';
import { DEFAULT_CRITERIA } from '../src/llm.js';
import type { JudgeInput } from '../src/types.js';

interface Call {
  url: string;
  init: RequestInit | undefined;
}

function fakeFetch(respond: (init: RequestInit | undefined) => Promise<Response>) {
  const calls: Call[] = [];
  const fetch: typeof globalThis.fetch = async (url, init) => {
    calls.push({ url: String(url), init });
    return respond(init);
  };
  return { fetch, calls };
}

const json = (body: unknown, status = 200) => async () => new Response(JSON.stringify(body), { status });

const input: JudgeInput = {
  scope: 's',
  window: {
    id: 'w',
    events: [{ id: 'a', at: 0, name: 'deploy', data: { env: 'prod' }, kind: 'closing', seenAt: 9000 }],
    reason: 'closing',
    waits: 0,
    dropped: 0,
    gap: false,
    settledAt: 9500,
  },
  routes: [{ route: 'interrupt', criterion: 'input interrupt' }, { route: 'queue' }, { route: 'note' }, { route: 'ask' }, { route: 'skip' }],
  predicates: [{ name: 'asked', question: 'Did the user ask for this?' }],
  facts: {},
  recent: [],
  now: 10_000,
};

const signal = new AbortController().signal;
const ROUTE_ANSWER = { type: 'choice', name: 'route', choice: 'note', probabilities: [{ value: 'note', probability: 1 }] };

async function outcome(promise: Promise<unknown>): Promise<[string, string, boolean]> {
  try {
    await promise;
    return ['resolved', '', false];
  } catch (error) {
    return error instanceof Error ? [error.name, error.message, isPermanent(error)] : ['thrown', String(error), false];
  }
}

describe('decisionsJudge', () => {
  test('posts one choice question for the route and one predicate question per predicate', async () => {
    const fake = fakeFetch(json({ answers: [ROUTE_ANSWER] }));
    const judge = decisionsJudge({
      apiKey: 'sk-test',
      model: 'test-model',
      fetch: fake.fetch,
      criteria: { interrupt: 'option interrupt', queue: 'option queue' },
      instructions: 'Choose how the agent hears about this.',
    });
    await judge(input, signal);
    const init = fake.calls[0]?.init;
    expect({
      url: fake.calls[0]?.url,
      method: init?.method,
      headers: init?.headers,
      body: typeof init?.body === 'string' ? JSON.parse(init.body) : init?.body,
    }).toEqual({
      url: 'https://api.openai.com/v1/decisions',
      method: 'POST',
      headers: { authorization: 'Bearer sk-test', 'content-type': 'application/json' },
      body: {
        model: 'test-model',
        input:
          '{"window":{"reason":"closing","waits":0,"dropped":0,"gap":false,"events":[{"kind":"closing","name":"deploy","data":{"env":"prod"},"ago":"1.0s"}]},' +
          '"facts":{},"recent":[],"predicates":{"asked":"Did the user ask for this?"}}',
        questions: [
          {
            type: 'choice',
            name: 'route',
            instructions: 'Choose how the agent hears about this.',
            choices: [
              { value: 'interrupt', description: 'input interrupt' },
              { value: 'queue', description: 'option queue' },
              { value: 'note', description: DEFAULT_CRITERIA.note },
              { value: 'ask', description: DEFAULT_CRITERIA.ask },
              { value: 'skip', description: DEFAULT_CRITERIA.skip },
            ],
          },
          { type: 'predicate', name: 'predicate_0', instructions: 'Did the user ask for this?' },
        ],
      },
    });
  });

  test("reads the route's choice and probabilities and each asked predicate's probability", async () => {
    const fake = fakeFetch(
      json({
        answers: [
          { type: 'predicate', name: 'predicate_0', probability: 0.4 },
          {
            type: 'choice',
            name: 'route',
            choice: 'queue',
            probabilities: [
              { value: 'queue', probability: 0.75 },
              { value: 'note', probability: 0.25 },
              { value: 'wait', probability: 0.4 },
              { value: 'skip', probability: 'low' },
            ],
          },
          { type: 'predicate', name: 'asked', probability: 0.9 },
          { type: 'predicate', name: 'predicate_1', probability: 0.9 },
        ],
      }),
    );
    const judge = decisionsJudge({ apiKey: 'k', model: 'm', endpoint: 'https://proxy.example.com/v1/decisions', fetch: fake.fetch });
    expect(await judge(input, signal)).toEqual({ route: 'queue', probabilities: { queue: 0.75, note: 0.25 }, predicates: { asked: 0.4 } });
    expect(fake.calls.map((c) => c.url)).toEqual(['https://proxy.example.com/v1/decisions']);
  });

  test('a predicate named route is asked under its index, so it cannot collide with the route question', async () => {
    const fake = fakeFetch(
      json({
        answers: [
          ROUTE_ANSWER,
          { type: 'predicate', name: 'route', probability: 0.9 },
          { type: 'predicate', name: 'predicate_0', probability: 0.3 },
          { type: 'predicate', name: 'predicate_1', probability: 0.6 },
        ],
      }),
    );
    const judge = decisionsJudge({ apiKey: 'k', model: 'm', fetch: fake.fetch, instructions: 'Pick one.' });
    const judgment = await judge(
      {
        ...input,
        routes: [{ route: 'note', criterion: 'option note' }, { route: 'skip', criterion: 'option skip' }],
        predicates: [
          { name: 'route', question: 'Is a route obvious?' },
          { name: 'asked', question: 'Did the user ask for this?' },
        ],
      },
      signal,
    );
    const body = fake.calls[0]?.init?.body;
    expect(typeof body === 'string' ? JSON.parse(body) : body).toEqual({
      model: 'm',
      input:
        '{"window":{"reason":"closing","waits":0,"dropped":0,"gap":false,"events":[{"kind":"closing","name":"deploy","data":{"env":"prod"},"ago":"1.0s"}]},' +
        '"facts":{},"recent":[],"predicates":{"route":"Is a route obvious?","asked":"Did the user ask for this?"}}',
      questions: [
        {
          type: 'choice',
          name: 'route',
          instructions: 'Pick one.',
          choices: [
            { value: 'note', description: 'option note' },
            { value: 'skip', description: 'option skip' },
          ],
        },
        { type: 'predicate', name: 'predicate_0', instructions: 'Is a route obvious?' },
        { type: 'predicate', name: 'predicate_1', instructions: 'Did the user ask for this?' },
      ],
    });
    expect(judgment).toEqual({ route: 'note', probabilities: { note: 1 }, predicates: { route: 0.3, asked: 0.6 } });
  });

  test('a reply without an offered route is a retryable failure', async () => {
    const results = [
      await outcome(decisionsJudge({ apiKey: 'k', model: 'm', fetch: fakeFetch(json({ answers: [{ ...ROUTE_ANSWER, choice: 'wait' }] })).fetch })(input, signal)),
      await outcome(decisionsJudge({ apiKey: 'k', model: 'm', fetch: fakeFetch(json({ answers: [] })).fetch })(input, signal)),
      await outcome(decisionsJudge({ apiKey: 'k', model: 'm', fetch: fakeFetch(json({ output: [] })).fetch })(input, signal)),
    ];
    expect(results).toEqual([
      ['Error', 'decisionsJudge: route "wait" is not one of interrupt, queue, note, ask, skip', false],
      ['Error', 'decisionsJudge: route undefined is not one of interrupt, queue, note, ask, skip', false],
      ['Error', 'decisionsJudge: response has no answers array', false],
    ]);
  });

  test('408, 429, 5xx and network failures retry; other 4xx are permanent', async () => {
    const respond: ((init: RequestInit | undefined) => Promise<Response>)[] = [
      async () => new Response('{"error":{"message":"slow down"}}', { status: 429 }),
      async () => new Response(null, { status: 503 }),
      async () => new Response(null, { status: 408 }),
      async () => new Response('{"error":{"message":"unknown question type"}}', { status: 400 }),
      async () => new Response(null, { status: 401 }),
      async () => {
        throw new TypeError('fetch failed');
      },
    ];
    const results = [];
    for (const r of respond) results.push(await outcome(decisionsJudge({ apiKey: 'k', model: 'm', fetch: fakeFetch(r).fetch })(input, signal)));
    expect(results).toEqual([
      ['Error', 'decisionsJudge: HTTP 429: {"error":{"message":"slow down"}}', false],
      ['Error', 'decisionsJudge: HTTP 503', false],
      ['Error', 'decisionsJudge: HTTP 408', false],
      ['PermanentError', 'decisionsJudge: HTTP 400: {"error":{"message":"unknown question type"}}', true],
      ['PermanentError', 'decisionsJudge: HTTP 401', true],
      ['Error', 'decisionsJudge: request failed: fetch failed', false],
    ]);
  });

  test("aborting the signal cancels the request and rejects with the signal's reason", async () => {
    const fake = fakeFetch(
      (init) =>
        new Promise((_, reject) => {
          init?.signal?.addEventListener('abort', () => reject(new DOMException('This operation was aborted', 'AbortError')), { once: true });
        }),
    );
    const controller = new AbortController();
    const pending = decisionsJudge({ apiKey: 'k', model: 'm', fetch: fake.fetch })(input, controller.signal);
    const reason = new Error('attend: judge timed out');
    controller.abort(reason);
    await expect(pending).rejects.toBe(reason);
  });
});
