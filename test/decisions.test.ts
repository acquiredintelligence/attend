import { describe, expect, test } from 'vitest';
import { decisionsJudge } from '../src/decisions.js';
import { isPermanent } from '../src/errors.js';
import { DEFAULT_CRITERIA } from '../src/llm.js';
import type { JudgeImage, JudgeInput } from '../src/types.js';
import { png } from './support/images.js';

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

const json = (body: unknown, status = 200) => async (_init?: RequestInit) => new Response(JSON.stringify(body), { status });

const bodyOf = (call: Call | undefined): Record<string, unknown> => (typeof call?.init?.body === 'string' ? JSON.parse(call.init.body) : {});

const STATE_TEXT =
  '{"window":{"reason":"closing","waits":0,"dropped":0,"gap":false,"events":[{"kind":"closing","name":"deploy","data":{"env":"prod"},"ago":"1.0s"}]},' +
  '"facts":{},"recent":[],"predicates":{"asked":"Did the user ask for this?"}}';

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

  test('408, 429, 5xx and network failures retry; other 4xx and the billing and quota 429s are permanent', async () => {
    const respond: ((init: RequestInit | undefined) => Promise<Response>)[] = [
      async () => new Response('{"error":{"message":"slow down","type":"rate_limit_error","code":"slow_down"}}', { status: 429, headers: { 'x-request-id': 'req_42' } }),
      async () => new Response('{"error":{"message":"Rate limit reached for requests","type":"requests","code":null}}', { status: 429 }),
      async () => new Response('{"error":{"message":"overloaded","type":"service_unavailable_error","code":"server_is_overloaded"}}', { status: 503 }),
      async () => new Response(null, { status: 408 }),
      async () => new Response(null, { status: 500 }),
      async () => new Response('{"error":{"message":"unknown question type","type":"invalid_request_error","param":"questions","code":null}}', { status: 400 }),
      async () => new Response(null, { status: 401 }),
      async () => new Response('{"error":{"message":"no credits","type":"insufficient_quota","code":"credit_balance_exhausted"}}', { status: 429 }),
      async () => new Response('{"error":{"message":"spend limit","type":"insufficient_quota","code":"project_spend_limit_exceeded"}}', { status: 429 }),
      async () => new Response('{"error":{"message":"usage limit","type":"insufficient_quota","code":"organization_usage_limit_exceeded"}}', { status: 429 }),
      async () => new Response('{"error":{"message":"quota","type":"insufficient_quota"}}', { status: 429 }),
      async () => {
        throw new TypeError('fetch failed');
      },
    ];
    const results = [];
    for (const r of respond) results.push(await outcome(decisionsJudge({ apiKey: 'k', model: 'm', fetch: fakeFetch(r).fetch })(input, signal)));
    expect(results).toEqual([
      ['Error', 'decisionsJudge: HTTP 429: slow down (request req_42)', false],
      ['Error', 'decisionsJudge: HTTP 429: Rate limit reached for requests', false],
      ['Error', 'decisionsJudge: HTTP 503: overloaded', false],
      ['Error', 'decisionsJudge: HTTP 408', false],
      ['Error', 'decisionsJudge: HTTP 500', false],
      ['PermanentError', 'decisionsJudge: HTTP 400: unknown question type', true],
      ['PermanentError', 'decisionsJudge: HTTP 401', true],
      ['PermanentError', 'decisionsJudge: HTTP 429: no credits', true],
      ['PermanentError', 'decisionsJudge: HTTP 429: spend limit', true],
      ['PermanentError', 'decisionsJudge: HTTP 429: usage limit', true],
      ['PermanentError', 'decisionsJudge: HTTP 429: quota', true],
      ['Error', 'decisionsJudge: request failed: fetch failed', false],
    ]);
  });

  test('a refusal of the route question is a PermanentError; a refused predicate is left unanswered', async () => {
    const refusedRoute = fakeFetch(json({ answers: [{ type: 'refusal', name: 'route' }, { type: 'predicate', name: 'predicate_0', probability: 0.5 }] }));
    expect(await outcome(decisionsJudge({ apiKey: 'k', model: 'm', fetch: refusedRoute.fetch })(input, signal))).toEqual([
      'PermanentError',
      'decisionsJudge: the model refused to answer the route question',
      true,
    ]);
    const refusedPredicate = fakeFetch(json({ answers: [{ ...ROUTE_ANSWER, confidence: 0.9 }, { type: 'refusal', name: 'predicate_0' }] }));
    expect(await decisionsJudge({ apiKey: 'k', model: 'm', fetch: refusedPredicate.fetch })(input, signal)).toEqual({ route: 'note', probabilities: { note: 1 }, predicates: {}, confidence: 0.9 });
  });

  test('too many predicates or duplicate names are a PermanentError before any call', async () => {
    const fake = fakeFetch(json({ answers: [ROUTE_ANSWER] }));
    const judge = decisionsJudge({ apiKey: 'k', model: 'm', fetch: fake.fetch });
    const many = Array.from({ length: 200 }, (_, i) => ({ name: `p${i}`, question: 'Q?' }));
    expect(await outcome(judge({ ...input, predicates: many }, signal))).toEqual(['PermanentError', 'decisionsJudge: 200 predicates plus the route is over the 200 questions the API takes', true]);
    expect(await outcome(judge({ ...input, predicates: [many[0]!, many[0]!] }, signal))).toEqual(['PermanentError', 'decisionsJudge: predicate names must be unique, got p0, p0', true]);
    expect(await outcome(judge({ ...input, predicates: many.slice(0, 199) }, signal))).toEqual(['resolved', '', false]);
    expect(fake.calls).toHaveLength(1);
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

describe('decisionsJudge images', () => {
  const images: JudgeImage[] = [
    { data: png(640, 480), mimeType: 'image/png' },
    { data: 'UklGRg==', mimeType: 'image/webp' },
  ];

  test('sends the text and then each image as an input_image data URL in one user message', async () => {
    const fake = fakeFetch(json({ answers: [ROUTE_ANSWER] }));
    await decisionsJudge({ apiKey: 'k', model: 'm', fetch: fake.fetch, instructions: 'Pick.' })({ ...input, images }, signal);
    expect(bodyOf(fake.calls[0])).toEqual({
      model: 'm',
      input: [
        {
          role: 'user',
          content: [
            { type: 'input_text', text: STATE_TEXT },
            { type: 'input_image', image_url: `data:image/png;base64,${png(640, 480)}` },
            { type: 'input_image', image_url: 'data:image/webp;base64,UklGRg==' },
          ],
        },
      ],
      questions: [
        { type: 'choice', name: 'route', instructions: 'Pick.', choices: input.routes.map((r) => ({ value: r.route, description: r.criterion ?? DEFAULT_CRITERIA[r.route] })) },
        { type: 'predicate', name: 'predicate_0', instructions: 'Did the user ask for this?' },
      ],
    });
  });

  test('keeps the first 128 images and reports the rest', async () => {
    const fake = fakeFetch(json({ answers: [ROUTE_ANSWER] }));
    const errors: string[] = [];
    const given = Array.from({ length: 130 }, (): JudgeImage => ({ data: 'AAAA', mimeType: 'image/jpeg' }));
    await decisionsJudge({ apiKey: 'k', model: 'm', fetch: fake.fetch, onImageError: (e) => errors.push(e.message) })({ ...input, images: given }, signal);
    const message = (bodyOf(fake.calls[0]).input as { content: unknown[] }[])[0];
    expect(message?.content).toHaveLength(129);
    expect(errors).toEqual([
      'decisionsJudge: image 128 dropped: the API takes at most 128 images per request',
      'decisionsJudge: image 129 dropped: the API takes at most 128 images per request',
    ]);
  });

  test('with images off the input is the plain text', async () => {
    const fake = fakeFetch(json({ answers: [ROUTE_ANSWER] }));
    await decisionsJudge({ apiKey: 'k', model: 'm', fetch: fake.fetch, images: false })({ ...input, images }, signal);
    expect(bodyOf(fake.calls[0]).input).toBe(STATE_TEXT);
  });

  test('a 400 on a request with images is retried once as plain text, and the retry is reported', async () => {
    const fake = fakeFetch(async (init) => {
      const body = typeof init?.body === 'string' ? JSON.parse(init.body) : {};
      return typeof body.input === 'string' ? json({ answers: [ROUTE_ANSWER] })(init) : json({ error: { message: 'Invalid image', type: 'invalid_request_error' } }, 400)(init);
    });
    const errors: string[] = [];
    expect(await decisionsJudge({ apiKey: 'k', model: 'm', fetch: fake.fetch, onImageError: (e) => errors.push(e.message) })({ ...input, images }, signal)).toEqual({
      route: 'note',
      probabilities: { note: 1 },
      predicates: {},
    });
    expect(fake.calls.map((c) => typeof bodyOf(c).input)).toEqual(['object', 'string']);
    expect(errors).toEqual(['decisionsJudge: the provider rejected the request with its 2 images, retrying without them: decisionsJudge: HTTP 400: Invalid image']);
    const twice = fakeFetch(json({ error: { message: 'bad' } }, 400));
    expect(await outcome(decisionsJudge({ apiKey: 'k', model: 'm', fetch: twice.fetch })({ ...input, images }, signal))).toEqual(['PermanentError', 'decisionsJudge: HTTP 400: bad', true]);
    expect(twice.calls).toHaveLength(2);
  });
});

const CONTEXT_BODY = { error: { message: 'Your input exceeds the context window of this model.', type: 'invalid_request_error', param: 'input', code: 'context_length_exceeded' } };
const TWO_EVENTS: JudgeInput = { ...input, window: { ...input.window, events: [...input.window.events, { id: 'b', name: 'newest', kind: 'closing', data: { env: 'prod' }, at: 0, seenAt: 9000 }] } };
const HALVED_INPUT =
  '{"window":{"reason":"closing","waits":0,"dropped":1,"gap":false,"events":[{"kind":"closing","name":"newest","data":{"env":"prod"},"ago":"1.0s"}]},"facts":{},"recent":[],"predicates":{"asked":"Did the user ask for this?"}}';

function sentState(call: Call) {
  const value = bodyOf(call).input;
  return JSON.parse(typeof value === 'string' ? value : Array.isArray(value) ? value[0].content[0].text : 'null');
}

describe('decisionsJudge budgets', () => {
  test('declares a measured budget only when requested and validates its allowance', () => {
    expect('budget' in decisionsJudge({ apiKey: 'k', model: 'm' })).toBe(false);
    expect(decisionsJudge({ apiKey: 'k', model: 'm', contextTokens: 200_000 }).budget).toEqual({ contextTokens: 200_000, reservedTokens: 837 });
    for (const contextTokens of [837, 836, NaN])
      expect(() => decisionsJudge({ apiKey: 'k', model: 'm', contextTokens })).toThrow(
        new RangeError(`decisionsJudge: contextTokens must be more than the 837 tokens its own prompt takes, got ${contextTokens}`),
      );
  });
  test('retries a context-length 400 once with halved input', async () => {
    let calls = 0;
    const fake = fakeFetch(async () => (++calls === 1 ? new Response(JSON.stringify(CONTEXT_BODY), { status: 400 }) : json({ answers: [ROUTE_ANSWER] })()));
    expect(await decisionsJudge({ apiKey: 'k', model: 'm', fetch: fake.fetch })(TWO_EVENTS, signal)).toEqual({ route: 'note', probabilities: { note: 1 }, predicates: {} });
    expect(fake.calls).toHaveLength(2);
    expect(bodyOf(fake.calls[1]).input).toBe(HALVED_INPUT);
  });
  test('second context rejection is retryable and keeps status and request ID', async () => {
    const fake = fakeFetch(async () => new Response(JSON.stringify(CONTEXT_BODY), { status: 400, headers: { 'x-request-id': 'req-budget' } }));
    const error: unknown = await decisionsJudge({ apiKey: 'k', model: 'm', fetch: fake.fetch })(TWO_EVENTS, signal).catch((e: unknown) => e);
    expect(fake.calls).toHaveLength(2);
    expect(Object.getPrototypeOf(error)).toBe(Error.prototype);
    expect(isPermanent(error)).toBe(false);
    expect(error).toMatchObject({
      message: 'decisionsJudge: HTTP 400: Your input exceeds the context window of this model. (request req-budget); rejected again with the window halved',
      status: 400,
      requestId: 'req-budget',
    });
  });
  test.each([
    { status: 413, body: CONTEXT_BODY, calls: 2, outcome: ['resolved', '', false] },
    { status: 422, body: CONTEXT_BODY, calls: 1, outcome: ['PermanentError', 'decisionsJudge: HTTP 422: Your input exceeds the context window of this model.', true] },
    { status: 400, body: { message: 'unrelated' }, calls: 1, outcome: ['PermanentError', 'decisionsJudge: HTTP 400: unrelated', true] },
  ])('context classification at status $status with $calls calls', async ({ status, body, calls, outcome: expected }) => {
    let count = 0;
    const fake = fakeFetch(async () => (++count === 1 ? new Response(JSON.stringify(body), { status }) : json({ answers: [ROUTE_ANSWER] })()));
    const pending = decisionsJudge({ apiKey: 'k', model: 'm', fetch: fake.fetch })(TWO_EVENTS, signal);
    if (calls === 2) await expect(pending).resolves.toEqual({ route: 'note', probabilities: { note: 1 }, predicates: {} });
    expect(await outcome(pending)).toEqual(expected);
    expect(fake.calls).toHaveLength(calls);
    if (calls === 2) expect(bodyOf(fake.calls[1]).input).toBe(HALVED_INPUT);
  });
  test('halving keeps images and a later image rejection drops them once', async () => {
    let count = 0;
    const fake = fakeFetch(async () => {
      count += 1;
      if (count === 1) return new Response(JSON.stringify(CONTEXT_BODY), { status: 400 });
      if (count === 2) return new Response(JSON.stringify({ message: 'bad image' }), { status: 400 });
      return json({ answers: [ROUTE_ANSWER] })();
    });
    const errors: string[] = [];
    const judge = decisionsJudge({ apiKey: 'k', model: 'm', fetch: fake.fetch, onImageError: (error) => errors.push(error.message) });
    expect(await judge({ ...TWO_EVENTS, images: [{ data: png(280, 280), mimeType: 'image/png' }] }, signal)).toEqual({ route: 'note', probabilities: { note: 1 }, predicates: {} });
    expect(fake.calls.map((call) => [typeof bodyOf(call).input, sentState(call).window.events.length])).toEqual([
      ['object', 2],
      ['object', 1],
      ['string', 1],
    ]);
    expect(bodyOf(fake.calls[2]).input).toBe(HALVED_INPUT);
    expect(errors).toEqual(['decisionsJudge: the provider rejected the request with its 1 images, retrying without them: decisionsJudge: HTTP 400: bad image']);
  });
  test('context-length retry with images never reports an image error', async () => {
    let count = 0;
    const fake = fakeFetch(async () => (++count === 1 ? new Response(JSON.stringify(CONTEXT_BODY), { status: 400 }) : json({ answers: [ROUTE_ANSWER] })()));
    const errors: Error[] = [];
    const judge = decisionsJudge({ apiKey: 'k', model: 'm', fetch: fake.fetch, onImageError: (error) => errors.push(error) });
    expect(await judge({ ...TWO_EVENTS, images: [{ data: png(280, 280), mimeType: 'image/png' }] }, signal)).toEqual({ route: 'note', probabilities: { note: 1 }, predicates: {} });
    expect(fake.calls.map((call) => [typeof bodyOf(call).input, sentState(call).window.events.length])).toEqual([
      ['object', 2],
      ['object', 1],
    ]);
    expect(errors).toEqual([]);
  });
  test("the Decisions API's own token-limit 400 is retried once with the window halved", async () => {
    const body = { error: { message: 'Decision input exceeds the token limit.', type: 'invalid_request_error', param: null, code: null } };
    let calls = 0;
    const fake = fakeFetch(async () => (++calls === 1 ? new Response(JSON.stringify(body), { status: 400 }) : json({ answers: [ROUTE_ANSWER] })()));
    expect(await decisionsJudge({ apiKey: 'k', model: 'm', fetch: fake.fetch })(TWO_EVENTS, signal)).toEqual({ route: 'note', probabilities: { note: 1 }, predicates: {} });
    expect(fake.calls.map((call) => bodyOf(call).input)).toEqual([expect.any(String), HALVED_INPUT]);
  });
  test('trims oldest sheddable input at render time', async () => {
    const fake = fakeFetch(json({ answers: [ROUTE_ANSWER] }));
    const original: JudgeInput = {
      ...input,
      window: {
        ...input.window,
        events: [
          { id: 'old', name: 'old', kind: 'consequence', data: {}, text: 'x'.repeat(200), at: 0, seenAt: 0 },
          ...input.window.events,
          { id: 'context', name: 'context', kind: 'context', data: {}, text: 'y'.repeat(200), at: 0, seenAt: 0 },
        ],
      },
    };
    expect(await decisionsJudge({ apiKey: 'k', model: 'm', fetch: fake.fetch, contextTokens: 897 })(original, signal)).toEqual({ route: 'note', probabilities: { note: 1 }, predicates: {} });
    expect(fake.calls).toHaveLength(1);
    expect(bodyOf(fake.calls[0]).input).toBe(
      '{"window":{"reason":"closing","waits":0,"dropped":2,"gap":false,"events":[{"kind":"closing","name":"deploy","data":{"env":"prod"},"ago":"1.0s"}]},"facts":{},"recent":[],"predicates":{"asked":"Did the user ask for this?"}}',
    );
  });
});
