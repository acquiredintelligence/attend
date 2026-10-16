import { describe, expect, test } from 'vitest';
import { isPermanent } from '../src/errors.js';
import { JEV_IMAGE_LIMITS, jevJudge } from '../src/jev.js';
import { DEFAULT_CRITERIA, DEFAULT_SYSTEM } from '../src/llm.js';
import type { JudgeImage, JudgeInput } from '../src/types.js';
import { base64, jpeg, png, pngBytes } from './support/images.js';

interface Call {
  url: string;
  init: RequestInit | undefined;
}

type Respond = (init: RequestInit | undefined, call: number) => Promise<Response>;

function fakeFetch(respond: Respond) {
  const calls: Call[] = [];
  const fetch: typeof globalThis.fetch = async (url, init) => {
    calls.push({ url: String(url), init });
    return respond(init, calls.length);
  };
  return { fetch, calls };
}

const json =
  (body: unknown, status = 200, headers: Record<string, string> = {}): Respond =>
  async () =>
    new Response(JSON.stringify(body), { status, headers });

const bodyOf = (call: Call | undefined): Record<string, unknown> => (typeof call?.init?.body === 'string' ? JSON.parse(call.init.body) : {});

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

const STATE = { window: { reason: 'closing', waits: 0, dropped: 0, gap: false, events: [{ kind: 'closing', name: 'deploy', data: { env: 'prod' }, ago: '1.0s' }] }, facts: {}, recent: [], predicates: { asked: 'Did the user ask for this?' } };

const signal = new AbortController().signal;
const ROUTE_ANSWER = { type: 'choice', choice: 'note', probabilities: { note: 1 }, confidence: 1 };
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

async function outcome(promise: Promise<unknown>): Promise<[string, string, boolean]> {
  try {
    await promise;
    return ['resolved', '', false];
  } catch (error) {
    return error instanceof Error ? [error.name, error.message, isPermanent(error)] : ['thrown', String(error), false];
  }
}

describe('jevJudge', () => {
  test('posts one choice question keyed route and one noul question per predicate, with the state as JSON', async () => {
    const fake = fakeFetch(json({ answers: { route: ROUTE_ANSWER } }));
    const judge = jevJudge({
      apiKey: 'ts-test',
      fetch: fake.fetch,
      criteria: { interrupt: 'option interrupt', queue: 'option queue' },
      instructions: 'Choose how the agent hears about this.',
    });
    await judge(input, signal);
    const init = fake.calls[0]?.init;
    expect({ url: fake.calls[0]?.url, method: init?.method, headers: init?.headers, body: bodyOf(fake.calls[0]) }).toEqual({
      url: 'https://api.typesafe.ai/v1/systemone',
      method: 'POST',
      headers: { authorization: 'Bearer ts-test', 'content-type': 'application/json' },
      body: {
        model: 'jev-latest',
        state: STATE,
        questions: {
          route: {
            type: 'choice',
            instructions: 'Choose how the agent hears about this.',
            criteria: { interrupt: 'input interrupt', queue: 'option queue', note: DEFAULT_CRITERIA.note, ask: DEFAULT_CRITERIA.ask, skip: DEFAULT_CRITERIA.skip },
          },
          predicate_0: { type: 'noul', instructions: 'Did the user ask for this?' },
        },
      },
    });
  });

  test('model, endpoint and long context are sent as given, and the instructions default to DEFAULT_SYSTEM', async () => {
    const fake = fakeFetch(json({ answers: { route: ROUTE_ANSWER } }));
    const judge = jevJudge({ apiKey: 'k', model: 'jev-1.14.0', endpoint: 'https://openrouter.ai/api/v1/systemone', longContext: true, fetch: fake.fetch });
    await judge({ ...input, predicates: [] }, signal);
    const body = bodyOf(fake.calls[0]);
    expect({ url: fake.calls[0]?.url, headers: fake.calls[0]?.init?.headers, model: body.model, questions: Object.keys(body.questions ?? {}) }).toEqual({
      url: 'https://openrouter.ai/api/v1/systemone',
      headers: { authorization: 'Bearer k', 'content-type': 'application/json', 'TypeSafe-Danger-Zone-Long-Context': 'true' },
      model: 'jev-1.14.0',
      questions: ['route'],
    });
    expect((body.questions as Record<string, Record<string, unknown>>).route?.instructions).toBe(DEFAULT_SYSTEM);
  });

  test("reads the route's choice, probabilities and confidence, and each asked predicate's noul under its own name", async () => {
    const fake = fakeFetch(
      json({
        model: 'jev-1.13.0',
        answers: {
          route: { type: 'choice', choice: 'queue', probabilities: { queue: 0.75, note: 0.25, wait: 0.4, skip: 'low' }, confidence: 0.62 },
          predicate_0: { type: 'noul', noul: 0.4 },
          asked: { type: 'noul', noul: 0.9 },
          predicate_1: { type: 'noul', noul: 0.9 },
        },
        usage: { input_tokens: 10, output_tokens: 2 },
      }),
    );
    const judge = jevJudge({ apiKey: 'k', fetch: fake.fetch });
    expect(await judge(input, signal)).toEqual({ route: 'queue', probabilities: { queue: 0.75, note: 0.25 }, predicates: { asked: 0.4 }, confidence: 0.62 });
  });

  test('predicates named route and predicate_0 are asked under their positions, so neither collides', async () => {
    const fake = fakeFetch(
      json({
        answers: {
          route: ROUTE_ANSWER,
          predicate_0: { type: 'noul', noul: 0.3 },
          predicate_1: { type: 'noul', noul: 0.6 },
          predicate_2: { type: 'noul', noul: 0.8 },
        },
      }),
    );
    const judge = jevJudge({ apiKey: 'k', fetch: fake.fetch, instructions: 'Pick one.' });
    const judgment = await judge(
      {
        ...input,
        routes: [{ route: 'note', criterion: 'option note' }, { route: 'skip', criterion: 'option skip' }],
        predicates: [
          { name: 'route', question: 'Is a route obvious?' },
          { name: 'predicate_0', question: 'Is this the first?' },
          { name: 'asked', question: 'Did the user ask for this?' },
        ],
      },
      signal,
    );
    expect(bodyOf(fake.calls[0]).questions).toEqual({
      route: { type: 'choice', instructions: 'Pick one.', criteria: { note: 'option note', skip: 'option skip' } },
      predicate_0: { type: 'noul', instructions: 'Is a route obvious?' },
      predicate_1: { type: 'noul', instructions: 'Is this the first?' },
      predicate_2: { type: 'noul', instructions: 'Did the user ask for this?' },
    });
    expect(judgment).toEqual({ route: 'note', probabilities: { note: 1 }, predicates: { route: 0.3, predicate_0: 0.6, asked: 0.8 }, confidence: 1 });
  });

  test('duplicate predicate names are a PermanentError before any call', async () => {
    const fake = fakeFetch(json({ answers: { route: ROUTE_ANSWER } }));
    const judge = jevJudge({ apiKey: 'k', fetch: fake.fetch });
    const predicates = [
      { name: 'asked', question: 'A?' },
      { name: 'asked', question: 'B?' },
    ];
    expect(await outcome(judge({ ...input, predicates }, signal))).toEqual(['PermanentError', 'jevJudge: predicate names must be unique, got asked, asked', true]);
    expect(fake.calls).toHaveLength(0);
  });

  test('a reply without an offered route is a retryable failure', async () => {
    const results = [
      await outcome(jevJudge({ apiKey: 'k', fetch: fakeFetch(json({ answers: { route: { ...ROUTE_ANSWER, choice: 'wait' } } })).fetch })(input, signal)),
      await outcome(jevJudge({ apiKey: 'k', fetch: fakeFetch(json({ answers: {} })).fetch })(input, signal)),
      await outcome(jevJudge({ apiKey: 'k', fetch: fakeFetch(json({ answers: [] })).fetch })(input, signal)),
      await outcome(jevJudge({ apiKey: 'k', fetch: fakeFetch(async () => new Response('not json', { status: 200 })).fetch })(input, signal)),
    ];
    expect(results).toEqual([
      ['Error', 'jevJudge: route "wait" is not one of interrupt, queue, note, ask, skip', false],
      ['Error', 'jevJudge: route undefined is not one of interrupt, queue, note, ask, skip', false],
      ['Error', 'jevJudge: response has no answers map', false],
      ['Error', 'jevJudge: response is not JSON', false],
    ]);
  });

  test('408, 429, 529, 5xx and network failures retry; 400, 401, 413, 422 and a quota 429 are permanent', async () => {
    const cases: [Respond, [string, string, boolean]][] = [
      [json({ error: 'slow down' }, 429, { 'x-typesafe-request-id': 'req_1' }), ['Error', 'jevJudge: HTTP 429: slow down (request req_1)', false]],
      [json({ error: { message: 'overloaded' } }, 529), ['Error', 'jevJudge: HTTP 529: overloaded', false]],
      [async () => new Response(null, { status: 503 }), ['Error', 'jevJudge: HTTP 503', false]],
      [async () => new Response(null, { status: 408 }), ['Error', 'jevJudge: HTTP 408', false]],
      [json({ error: 'Unknown model: no-such-model' }, 400), ['PermanentError', 'jevJudge: HTTP 400: Unknown model: no-such-model', true]],
      [json({ message: 'max_tokens_exceeded' }, 400), ['Error', 'jevJudge: HTTP 400: max_tokens_exceeded; the window cannot be halved', false]],
      [async () => new Response(null, { status: 401 }), ['PermanentError', 'jevJudge: HTTP 401', true]],
      [async () => new Response('<html>too large</html>', { status: 413 }), ['PermanentError', 'jevJudge: HTTP 413: <html>too large</html>', true]],
      [
        json({ detail: [{ type: 'missing', loc: ['questions', 'q', 'score', 'criteria', 0], msg: 'Field required' }] }, 422),
        ['PermanentError', 'jevJudge: HTTP 422: questions.q.score.criteria.0: Field required', true],
      ],
      [json({ detail: { error_type: 'validation', message: 'bad field' } }, 422), ['PermanentError', 'jevJudge: HTTP 422: bad field', true]],
      [json({ error: { message: 'no credits', code: 'credit_balance_exhausted', type: 'insufficient_quota' } }, 429), ['PermanentError', 'jevJudge: HTTP 429: no credits', true]],
      [
        async () => {
          throw new TypeError('fetch failed');
        },
        ['Error', 'jevJudge: request failed: fetch failed', false],
      ],
    ];
    const results = [];
    for (const [respond] of cases) results.push(await outcome(jevJudge({ apiKey: 'k', fetch: fakeFetch(respond).fetch })(input, signal)));
    expect(results).toEqual(cases.map(([, expected]) => expected));
  });

  test('a status error carries the status, the request id and the Retry-After as fields', async () => {
    const at = (respond: Respond) =>
      jevJudge({ apiKey: 'k', fetch: fakeFetch(respond).fetch })(input, signal).then(
        () => null,
        (error: unknown) => (error instanceof Error ? { status: Reflect.get(error, 'status'), requestId: Reflect.get(error, 'requestId'), retryAfterMs: Reflect.get(error, 'retryAfterMs') } : null),
      );
    expect(await at(json({}, 429, { 'retry-after-ms': '1500', 'retry-after': '7', 'x-typesafe-request-id': 'req_9' }))).toEqual({ status: 429, requestId: 'req_9', retryAfterMs: 1500 });
    expect(await at(json({}, 429, { 'retry-after': '7' }))).toEqual({ status: 429, requestId: null, retryAfterMs: 7000 });
    expect(await at(json({}, 503, { 'retry-after': new Date(Date.now() + 30_000).toUTCString() }))).toMatchObject({ status: 503, retryAfterMs: expect.any(Number) });
    expect(await at(json({}, 503, { 'retry-after': 'soon' }))).toEqual({ status: 503, requestId: null, retryAfterMs: null });
  });

  test("aborting the signal cancels the request and rejects with the signal's reason", async () => {
    const fake = fakeFetch(
      (init) =>
        new Promise((_, reject) => {
          init?.signal?.addEventListener('abort', () => reject(new DOMException('This operation was aborted', 'AbortError')), { once: true });
        }),
    );
    const controller = new AbortController();
    const pending = jevJudge({ apiKey: 'k', fetch: fake.fetch })(input, controller.signal);
    const reason = new Error('attend: judge timed out');
    controller.abort(reason);
    await expect(pending).rejects.toBe(reason);
  });
});

describe('jevJudge images', () => {
  const images: JudgeImage[] = [
    { data: png(640, 480), mimeType: 'image/png' },
    { data: jpeg(1568, 1176), mimeType: 'image/jpeg' },
  ];

  test('sends each image once in assets, referenced from state.images, with the images header', async () => {
    const fake = fakeFetch(json({ answers: { route: ROUTE_ANSWER } }));
    const judge = jevJudge({ apiKey: 'k', fetch: fake.fetch });
    await judge({ ...input, images }, signal);
    const body = bodyOf(fake.calls[0]);
    const state = body.state as Record<string, unknown>;
    const refs = state.images as string[];
    expect(refs).toHaveLength(2);
    expect(refs.every((r) => UUID.test(r))).toBe(true);
    expect(new Set(refs).size).toBe(2);
    expect(fake.calls[0]?.init?.headers).toEqual({ authorization: 'Bearer k', 'content-type': 'application/json', 'TypeSafe-Danger-Zone-Images': 'true' });
    expect(state).toEqual({ ...STATE, images: refs });
    expect(body.assets).toEqual({
      [refs[0] ?? '']: { type: 'image', format: 'png', data_base64: png(640, 480) },
      [refs[1] ?? '']: { type: 'image', format: 'jpeg', data_base64: jpeg(1568, 1176) },
    });
  });

  test('drops images outside the documented format and size limits, reports each, and sends the rest', async () => {
    const fake = fakeFetch(json({ answers: { route: ROUTE_ANSWER } }));
    const errors: string[] = [];
    const judge = jevJudge({ apiKey: 'k', fetch: fake.fetch, onImageError: (e) => errors.push(e.message) });
    const big = 'A'.repeat(Math.ceil((JEV_IMAGE_LIMITS.fileBytes + 3) / 3) * 4);
    const dropped: JudgeImage[] = [
      { data: png(300, 300), mimeType: 'image/webp' },
      { data: png(300, 300), mimeType: 'image/gif' },
      { data: `${png(300, 300)}\n`, mimeType: 'image/png' },
      { data: big, mimeType: 'image/png' },
      { data: Buffer.from('not an image at all').toString('base64'), mimeType: 'image/png' },
      { data: png(300, 300, { animated: true }), mimeType: 'image/png' },
      { data: png(279, 300), mimeType: 'image/png' },
      { data: jpeg(300, 1569), mimeType: 'image/jpeg' },
      { data: png(280, 280), mimeType: 'image/png' },
      { data: jpeg(1568, 1568), mimeType: 'image/jpeg' },
    ];
    await judge({ ...input, images: [...dropped.slice(0, 4), images[0] ?? dropped[0]!, ...dropped.slice(4)] }, signal);
    expect(errors).toEqual([
      'jevJudge: image 0 dropped: image/webp is not accepted, only image/png and image/jpeg',
      'jevJudge: image 1 dropped: image/gif is not accepted, only image/png and image/jpeg',
      'jevJudge: image 2 dropped: data is not standard base64 with = padding and no whitespace',
      `jevJudge: image 3 dropped: ${(big.length / 4) * 3} bytes is over the ${JEV_IMAGE_LIMITS.fileBytes} byte file limit`,
      'jevJudge: image 5 dropped: the data is not a PNG or JPEG file with a readable header',
      'jevJudge: image 6 dropped: animated images are not accepted',
      'jevJudge: image 7 dropped: 279x300 px has a side under the 280 px minimum',
      'jevJudge: image 8 dropped: 300x1569 px has a side over the 1568 px maximum',
    ]);
    const body = bodyOf(fake.calls[0]);
    expect(Object.values(body.assets as Record<string, { format: string }>).map((a) => a.format)).toEqual(['png', 'png', 'jpeg']);
    expect(((body.state as Record<string, unknown>).images as string[]).length).toBe(3);
  });

  test('drops the images that would take all images past 32 MiB together', async () => {
    const fake = fakeFetch(json({ answers: { route: ROUTE_ANSWER } }));
    const errors: string[] = [];
    const judge = jevJudge({ apiKey: 'k', fetch: fake.fetch, onImageError: (e) => errors.push(e.message) });
    const bytes = 12 * 1024 * 1024;
    const padded = base64(Buffer.concat([pngBytes(500, 500), Buffer.alloc(bytes - pngBytes(500, 500).length)]));
    await judge({ ...input, images: [1, 2, 3].map(() => ({ data: padded, mimeType: 'image/png' })) }, signal);
    expect(errors).toEqual([`jevJudge: image 2 dropped: ${3 * bytes} bytes of images is over the ${JEV_IMAGE_LIMITS.allFilesBytes} byte limit for all images`]);
    expect(Object.keys(bodyOf(fake.calls[0]).assets as object)).toHaveLength(2);
  });

  test('with every image dropped, or with images off, the request has no assets and no images header', async () => {
    const cases: [JudgeImage[], boolean][] = [
      [[{ data: png(10, 10), mimeType: 'image/png' }], true],
      [images, false],
    ];
    for (const [given, enabled] of cases) {
      const fake = fakeFetch(json({ answers: { route: ROUTE_ANSWER } }));
      await jevJudge({ apiKey: 'k', fetch: fake.fetch, images: enabled })({ ...input, images: given }, signal);
      expect({ headers: fake.calls[0]?.init?.headers, body: bodyOf(fake.calls[0]) }).toEqual({
        headers: { authorization: 'Bearer k', 'content-type': 'application/json' },
        body: { model: 'jev-latest', state: STATE, questions: expect.any(Object) },
      });
    }
  });

  test('a 400 on a request with images is retried once without them, and the retry is reported', async () => {
    const fake = fakeFetch((init, call) =>
      call === 1 ? json({ error: 'asset a3f1 is not referenced' }, 400, { 'x-typesafe-request-id': 'req_img' })(init, call) : json({ answers: { route: { ...ROUTE_ANSWER, choice: 'queue' } } })(init, call),
    );
    const errors: string[] = [];
    const judge = jevJudge({ apiKey: 'k', fetch: fake.fetch, onImageError: (e) => errors.push(e.message) });
    expect(await judge({ ...input, images }, signal)).toEqual({ route: 'queue', probabilities: { note: 1 }, predicates: {}, confidence: 1 });
    expect(fake.calls.map((c) => Object.keys(bodyOf(c)))).toEqual([
      ['model', 'state', 'questions', 'assets'],
      ['model', 'state', 'questions'],
    ]);
    expect(fake.calls[1]?.init?.headers).toEqual({ authorization: 'Bearer k', 'content-type': 'application/json' });
    expect(errors).toEqual(['jevJudge: the provider rejected the request with its 2 images, retrying without them: jevJudge: HTTP 400: asset a3f1 is not referenced (request req_img)']);
  });

  test('the retry without images is the last: its failure is thrown, and a 400 without images is never retried', async () => {
    const twice = fakeFetch(json({ error: 'bad request' }, 400));
    expect(await outcome(jevJudge({ apiKey: 'k', fetch: twice.fetch })({ ...input, images }, signal))).toEqual(['PermanentError', 'jevJudge: HTTP 400: bad request', true]);
    expect(twice.calls).toHaveLength(2);
    const once = fakeFetch(json({ error: 'bad request' }, 400));
    expect(await outcome(jevJudge({ apiKey: 'k', fetch: once.fetch })(input, signal))).toEqual(['PermanentError', 'jevJudge: HTTP 400: bad request', true]);
    expect(once.calls).toHaveLength(1);
  });

  test('a 429 or 5xx with images is not blamed on them', async () => {
    const fake = fakeFetch(async () => new Response(null, { status: 503 }));
    expect(await outcome(jevJudge({ apiKey: 'k', fetch: fake.fetch })({ ...input, images }, signal))).toEqual(['Error', 'jevJudge: HTTP 503', false]);
    expect(fake.calls).toHaveLength(1);
  });
});

const HIDDEN_CONTEXT = { error: { message: 'Request too long', type: 'invalid_request_error' }, reason: 'max_tokens_exceeded' };
const TWO_EVENTS: JudgeInput = { ...input, window: { ...input.window, events: [...input.window.events, { id: 'b', name: 'newest', kind: 'closing', data: { env: 'prod' }, at: 0, seenAt: 9000 }] } };
const HALVED_WINDOW = { reason: 'closing', waits: 0, dropped: 1, gap: false, events: [{ kind: 'closing', name: 'newest', data: { env: 'prod' }, ago: '1.0s' }] };

describe('jevJudge budgets', () => {
  test('declares 32k or 262k and measures the complete route question', () => {
    expect(jevJudge({ apiKey: 'k' }).budget).toEqual({ contextTokens: 32_000, bytesPerToken: 2, reservedTokens: 1594 });
    expect(jevJudge({ apiKey: 'k', longContext: true }).budget).toEqual({ contextTokens: 262_000, bytesPerToken: 2, reservedTokens: 1594 });
    expect(jevJudge({ apiKey: 'k', instructions: 'Be quiet.', criteria: { skip: 'Ignore it.' } }).budget).toEqual({ contextTokens: 32_000, bytesPerToken: 2, reservedTokens: 404 });
  });
  test('recognizes a context code in an unparsed field and halves once', async () => {
    const fake = fakeFetch(async (_, call) => (call === 1 ? json(HIDDEN_CONTEXT, 400)(undefined, call) : json({ answers: { route: ROUTE_ANSWER } })(undefined, call)));
    expect(await jevJudge({ apiKey: 'k', fetch: fake.fetch })(TWO_EVENTS, signal)).toEqual({ route: 'note', probabilities: { note: 1 }, predicates: {}, confidence: 1 });
    expect(fake.calls).toHaveLength(2);
    expect(bodyOf(fake.calls[1]).state).toEqual({ ...STATE, window: HALVED_WINDOW });
  });
  test("TypeSafe's own max_tokens_exceeded body is retried once halved, then fails retryable", async () => {
    const fake = fakeFetch(json({ detail: { error_type: 'max_tokens_exceeded' } }, 400));
    expect(await outcome(jevJudge({ apiKey: 'k', fetch: fake.fetch })(TWO_EVENTS, signal))).toEqual([
      'Error',
      'jevJudge: HTTP 400: {"detail":{"error_type":"max_tokens_exceeded"}}; rejected again with the window halved',
      false,
    ]);
    expect(fake.calls.map((call) => (bodyOf(call).state as { window: { dropped: number } }).window.dropped)).toEqual([0, 1]);
  });
  test('second context rejection is retryable and preserves provider metadata', async () => {
    const fake = fakeFetch(json(HIDDEN_CONTEXT, 400, { 'x-typesafe-request-id': 'req-jev', 'retry-after-ms': '1200' }));
    const error: unknown = await jevJudge({ apiKey: 'k', fetch: fake.fetch })(TWO_EVENTS, signal).catch((e: unknown) => e);
    expect(fake.calls).toHaveLength(2);
    expect(Object.getPrototypeOf(error)).toBe(Error.prototype);
    expect(isPermanent(error)).toBe(false);
    expect(error).toMatchObject({ message: 'jevJudge: HTTP 400: Request too long (request req-jev); rejected again with the window halved', status: 400, requestId: 'req-jev', retryAfterMs: 1200 });
  });
  test('halving with images preserves body order, image references and assets', async () => {
    const fake = fakeFetch(async (_, call) => (call === 1 ? json(HIDDEN_CONTEXT, 400)(undefined, call) : json({ answers: { route: ROUTE_ANSWER } })(undefined, call)));
    const errors: Error[] = [];
    expect(
      await jevJudge({ apiKey: 'k', fetch: fake.fetch, onImageError: (error) => errors.push(error) })({ ...TWO_EVENTS, images: [{ data: png(280, 280), mimeType: 'image/png' }] }, signal),
    ).toEqual({ route: 'note', probabilities: { note: 1 }, predicates: {}, confidence: 1 });
    expect(fake.calls).toHaveLength(2);
    const states = fake.calls.map((call) => {
      const body = bodyOf(call);
      expect(Object.keys(body)).toEqual(['model', 'state', 'questions', 'assets']);
      const state = typeof call.init?.body === 'string' ? JSON.parse(call.init.body).state : null;
      expect(Object.keys(state)).toEqual(['window', 'facts', 'recent', 'predicates', 'images']);
      expect(state.images).toHaveLength(1);
      expect(state.images[0]).toMatch(UUID);
      expect(Object.keys(body.assets ?? {})).toEqual(state.images);
      return state;
    });
    expect(states[1]).toEqual({ ...STATE, window: HALVED_WINDOW, images: states[0].images });
    expect(bodyOf(fake.calls[1]).assets).toEqual(bodyOf(fake.calls[0]).assets);
    expect(errors).toEqual([]);
  });
  test('rendering yields every 64 events before posting', async () => {
    const turns: number[] = [];
    for (const length of [64, 65, 129]) {
      let count = 0;
      let timer: NodeJS.Immediate;
      const tick = () => {
        count += 1;
        timer = setImmediate(tick);
      };
      timer = setImmediate(tick);
      const fake = fakeFetch(async () => {
        turns.push(count);
        return json({ answers: { route: ROUTE_ANSWER } })(undefined, 1);
      });
      try {
        expect(
          await jevJudge({ apiKey: 'k', fetch: fake.fetch })(
            { ...input, window: { ...input.window, events: Array.from({ length }, (_, i) => ({ id: String(i), name: 'event', kind: 'closing', data: {}, at: 0, seenAt: 0 })) } },
            signal,
          ),
        ).toEqual({ route: 'note', probabilities: { note: 1 }, predicates: {}, confidence: 1 });
      } finally {
        clearImmediate(timer);
      }
    }
    expect(turns).toEqual([0, 1, 2]);
  });
});
