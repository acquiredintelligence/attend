/**
 * The hosted-judge half of the shared conformance suite. Each case is a judge configuration, one
 * judge input and the replies a fake transport gives in order. The fixture records every request
 * the judge sent, byte for byte, and how the call ended, so both ports must build the same
 * request bodies, trim and halve windows the same way and classify the same errors.
 */
import { isPermanent } from '../../src/errors.js';
import { decisionsJudge } from '../../src/decisions.js';
import { jevJudge } from '../../src/jev.js';
import { llmJudge } from '../../src/llm.js';
import type { Entry, JudgeImage, JudgeInput, JsonValue, Kind, Route } from '../../src/types.js';
import { jpeg, png } from '../support/images.js';

/** One reply: an HTTP response for the hosted judges, or what `complete` returns or throws for `llmJudge`. */
export type Reply =
  | { status: number; body: string; headers?: Record<string, string> }
  | { networkError: string }
  | { value: JsonValue }
  | { throws: { message: string; status?: number; code?: string; permanent?: boolean } };

export interface JudgeCase {
  name: string;
  judge: 'jev' | 'decisions' | 'llm';
  /** Plain JSON options; `fetch`, `complete` and `onImageError` are supplied by the runner. */
  options: Record<string, JsonValue>;
  input: JudgeInput;
  replies: Reply[];
  expected?: JudgeOutcome;
}

export interface JudgeOutcome {
  budget: JsonValue;
  /** Hosted: url, lowercase headers and the exact body text. llm: the request fields. UUIDs are numbered in order of appearance. */
  requests: JsonValue[];
  result: { judgment: JsonValue } | { error: { name: string; message: string; permanent: boolean; status: number | null; requestId: string | null; retryAfterMs: number | null } };
  imageErrors: string[];
}

const UUID = /[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}/g;

function numberUuids(texts: string[]): string[] {
  const seen = new Map<string, string>();
  return texts.map((text) => text.replace(UUID, (uuid) => {
    let name = seen.get(uuid);
    if (name === undefined) { name = `<uuid${seen.size}>`; seen.set(uuid, name); }
    return name;
  }));
}

/** Runs one case with a fake transport and returns what the fixture records. */
export async function runJudgeCase(c: Omit<JudgeCase, 'expected'>): Promise<JudgeOutcome> {
  const requests: { url?: string; headers?: Record<string, string>; body?: string; llm?: JsonValue }[] = [];
  const imageErrors: string[] = [];
  let next = 0;
  const reply = (): Reply => {
    const r = c.replies[Math.min(next, c.replies.length - 1)];
    next += 1;
    if (!r) throw new Error('no scripted reply');
    return r;
  };
  const fetch: typeof globalThis.fetch = async (url, init) => {
    const headers: Record<string, string> = {};
    for (const [name, value] of Object.entries((init?.headers ?? {}) as Record<string, string>)) headers[name.toLowerCase()] = value;
    requests.push({ url: String(url), headers, body: String(init?.body) });
    const r = reply();
    if ('networkError' in r) throw new TypeError(r.networkError);
    if (!('status' in r)) throw new Error('expected an HTTP reply');
    return new Response(r.body, { status: r.status, headers: r.headers ?? {} });
  };
  const onImageError = (error: Error) => { imageErrors.push(error.message); };
  const options = c.options as Record<string, never>;
  let judge;
  if (c.judge === 'jev') judge = jevJudge({ apiKey: 'k', ...options, fetch, onImageError });
  else if (c.judge === 'decisions') judge = decisionsJudge({ apiKey: 'k', model: 'm', ...options, fetch, onImageError });
  else {
    judge = llmJudge({
      ...options,
      complete: async ({ system, prompt, images, schema }) => {
        requests.push({ llm: JSON.parse(JSON.stringify({ system, prompt, images, schema })) as JsonValue });
        const r = reply();
        if ('value' in r) return r.value;
        if ('throws' in r) throw Object.assign(new Error(r.throws.message), r.throws.status === undefined ? {} : { status: r.throws.status },
          r.throws.code === undefined ? {} : { code: r.throws.code }, r.throws.permanent ? { permanent: true } : {});
        throw new Error('expected a complete reply');
      },
    });
  }
  let result: JudgeOutcome['result'];
  try {
    result = { judgment: JSON.parse(JSON.stringify(await judge(c.input, new AbortController().signal))) as JsonValue };
  } catch (error) {
    const e = error as Error & { status?: number; requestId?: string | null; retryAfterMs?: number | null };
    result = { error: { name: e.name, message: e.message, permanent: isPermanent(e), status: e.status ?? null, requestId: e.requestId ?? null, retryAfterMs: e.retryAfterMs ?? null } };
  }
  const bodies = numberUuids(requests.map((r) => r.body ?? ''));
  return {
    budget: (judge.budget ?? null) as JsonValue,
    requests: requests.map((r, i) => (r.llm !== undefined ? r.llm : { url: r.url ?? '', headers: r.headers ?? {}, body: bodies[i] ?? '' })),
    result,
    imageErrors,
  };
}

const entry = (id: string, kind: Kind, seenAt: number, extra: Partial<Entry> = {}): Entry => ({ id, at: 0, name: `${kind}.${id}`, data: { id }, kind, seenAt, ...extra });
const ALL: Route[] = ['interrupt', 'queue', 'note', 'ask', 'skip', 'wait'];

const base: JudgeInput = {
  scope: 's',
  window: {
    id: 'w', reason: 'closing', waits: 1, dropped: 2, gap: true, settledAt: 9500,
    events: [
      entry('a', 'closing', 9000, { name: 'deploy', data: { env: 'prod', '2': 'two', '1': 'one' } }),
      entry('b', 'consequence', 9100, { text: 'build ✓ "green"' }),
      entry('c', 'context', 9250, { text: 'agent: ok\n' }),
      entry('d', 'flush', 9999, { text: 'can you look? 🌍' }),
    ],
  },
  routes: [{ route: 'interrupt', criterion: 'input interrupt' }, { route: 'queue' }, { route: 'note' }, { route: 'ask' }, { route: 'skip' }, { route: 'wait' }],
  guidance: 'watches a repo',
  predicates: [{ name: 'asked', question: 'Did the user ask for this?' }, { name: 'route', question: 'Is it urgent?' }],
  facts: { turn: 'idle', depth: 2.5 },
  recent: [{ id: 'r1', route: 'note', at: 9750 }, { id: 'r2', route: 'skip', at: 4000 }],
  now: 10_000,
};

const { guidance: _guidance, ...unguided } = base;
const plain: JudgeInput = { ...unguided, routes: base.routes.filter((r) => r.route !== 'wait'), predicates: [] };

const many: JudgeInput = {
  ...base,
  window: { ...base.window, dropped: 0, events: Array.from({ length: 140 }, (_, i) => entry(`e${i}`, (['consequence', 'context', 'closing', 'consequence', 'passive', 'consequence', 'flush'] as Kind[])[i % 7]!, 1000 + i * 37, i % 3 === 0 ? { text: `line ${i} é` } : {})) },
};

const ok = (body: unknown, status = 200, headers?: Record<string, string>): Reply => ({ status, body: JSON.stringify(body), ...(headers ? { headers } : {}) });
const jevAnswer = { answers: { route: { type: 'choice', choice: 'queue', probabilities: { queue: 0.75, note: 0.25, wait: 'x', skip: 0.1 }, confidence: 0.62 }, predicate_0: { type: 'noul', noul: 0.4 }, predicate_1: { type: 'noul', noul: 'no' }, asked: { type: 'noul', noul: 0.9 } } };
const decisionsAnswer = { answers: [
  { type: 'choice', name: 'route', choice: 'note', probabilities: [{ value: 'note', probability: 0.7 }, { value: 'skip', probability: 0.3 }, { value: 'bogus', probability: 1 }], confidence: 0.55 },
  { type: 'predicate', name: 'predicate_0', probability: 0.2 },
  { type: 'refusal', name: 'predicate_1' },
] };
const tooLong = (provider: 'jev' | 'decisions'): Reply => (provider === 'jev'
  ? { status: 400, body: JSON.stringify({ detail: { code: 'max_tokens_exceeded', message: 'state too long' } }), headers: { 'x-typesafe-request-id': 'req-1' } }
  : { status: 400, body: JSON.stringify({ error: { message: 'Input exceeds the token limit of the model', code: 'context_length_exceeded' } }), headers: { 'x-request-id': 'req-2' } });

const images: JudgeImage[] = [
  { data: png(800, 600), mimeType: 'image/png' },
  { data: jpeg(400, 300, { app1Bytes: 40 }), mimeType: 'image/jpeg' },
  { data: png(100, 100), mimeType: 'image/png' },
  { data: png(2000, 900), mimeType: 'image/png' },
  { data: png(600, 600, { animated: true }), mimeType: 'image/png' },
  { data: 'not base64!', mimeType: 'image/png' },
  { data: png(500, 500), mimeType: 'image/webp' },
  { data: jpeg(300, 300, { progressive: true }), mimeType: 'image/jpeg' },
];

/** The status table, shared by both hosted judges. */
const STATUSES: [string, Reply][] = [
  ['408', ok({ error: { message: 'timeout' } }, 408)],
  ['429', ok({ error: { message: 'slow down', code: 'rate_limit_exceeded' } }, 429, { 'retry-after': '3' })],
  ['429 quota', ok({ error: { message: 'no credit', code: 'insufficient_quota' } }, 429)],
  ['429 spend limit as type', ok({ error: { message: 'limit', type: 'organization_spend_limit_exceeded' } }, 429)],
  ['500 with detail list', ok({ detail: [{ loc: ['body', 'state'], msg: 'bad' }, { msg: 'worse' }] }, 500, { 'retry-after-ms': '250' })],
  ['529 plain text', { status: 529, body: 'overloaded' }],
  ['503 detail object', ok({ detail: { message: 'down for maintenance' } }, 503, { 'retry-after': 'Wed, 21 Oct 2015 07:28:00 GMT' })],
  ['400 error string', ok({ error: 'bad request' }, 400)],
  ['401 message', ok({ message: 'bad key' }, 401)],
  ['413 not context', ok({ error: { message: 'payload too large' } }, 413)],
  ['422 detail string', ok({ detail: 'unprocessable' }, 422)],
  ['418 array body', ok([1, 2], 418)],
  ['network error', { networkError: 'fetch failed' }],
  ['200 not JSON', { status: 200, body: 'not json' }],
  ['200 without answers', ok({ nothing: true })],
];

export function judgeFixtures(): { cases: JudgeCase[] } {
  const cases: Omit<JudgeCase, 'expected'>[] = [];
  const add = (c: Omit<JudgeCase, 'expected'>) => cases.push(c);
  for (const judge of ['jev', 'decisions'] as const) {
    const answer = judge === 'jev' ? jevAnswer : decisionsAnswer;
    add({ name: `${judge}: the full request and parsed answers`, judge, options: { criteria: { interrupt: 'option interrupt', queue: 'option queue' }, instructions: 'Choose.' }, input: base, replies: [ok(answer)] });
    add({ name: `${judge}: defaults with no predicates and no wait`, judge, options: {}, input: plain, replies: [ok(answer)] });
    add({ name: `${judge}: a reply naming a route not offered retries`, judge, options: {}, input: plain, replies: [ok(judge === 'jev' ? { answers: { route: { type: 'choice', choice: 'wait' } } } : { answers: [{ type: 'choice', name: 'route', choice: 'wait' }] })] });
    for (const [label, r] of STATUSES) add({ name: `${judge}: ${label}`, judge, options: {}, input: plain, replies: [r] });
    add({ name: `${judge}: a context-length 400 is retried once halved and succeeds`, judge, options: {}, input: many, replies: [tooLong(judge), ok(answer)] });
    add({ name: `${judge}: a second context-length 400 fails retryable`, judge, options: {}, input: many, replies: [tooLong(judge)] });
    add({ name: `${judge}: a one-event window cannot be halved`, judge, options: {}, input: { ...plain, window: { ...plain.window, events: [plain.window.events[0]!] } }, replies: [tooLong(judge)] });
    add({ name: `${judge}: images are sent, checked and dropped`, judge, options: {}, input: { ...plain, images }, replies: [ok(answer)] });
    add({ name: `${judge}: a 400 with images retries once without them`, judge, options: {}, input: { ...plain, images: images.slice(0, 2) }, replies: [ok({ error: { message: 'bad image' } }, 400), ok(answer)] });
    add({ name: `${judge}: a 500 with images is not blamed on them`, judge, options: {}, input: { ...plain, images: images.slice(0, 1) }, replies: [ok({ error: 'down' }, 500)] });
    add({ name: `${judge}: images off sends none`, judge, options: { images: false }, input: { ...plain, images }, replies: [ok(answer)] });
    add({ name: `${judge}: duplicate predicate names are permanent before any call`, judge, options: {}, input: { ...plain, predicates: [{ name: 'x', question: 'one' }, { name: 'x', question: 'two' }] }, replies: [ok(answer)] });
  }
  add({ name: 'jev: long context, model and endpoint', judge: 'jev', options: { longContext: true, model: 'jev-1.14.0', endpoint: 'https://openrouter.ai/api/v1/systemone' }, input: base, replies: [ok(jevAnswer)] });
  add({ name: 'jev: a budget trims the oldest sheddable events at render', judge: 'jev', options: {}, input: { ...many, window: { ...many.window, events: Array.from({ length: 300 }, (_, i) => entry(`e${i}`, i % 50 === 0 ? 'closing' : 'consequence', i, { data: { line: 'x'.repeat(400) } })) } }, replies: [ok(jevAnswer)] });
  add({ name: 'decisions: a refused route is permanent', judge: 'decisions', options: {}, input: plain, replies: [ok({ answers: [{ type: 'refusal', name: 'route' }] })] });
  add({ name: 'decisions: more than 199 predicates are permanent', judge: 'decisions', options: {}, input: { ...plain, predicates: Array.from({ length: 200 }, (_, i) => ({ name: `p${i}`, question: 'q' })) }, replies: [ok(decisionsAnswer)] });
  add({ name: 'decisions: 130 images keep the first 128', judge: 'decisions', options: {}, input: { ...plain, images: Array.from({ length: 130 }, (_, i) => ({ data: `AAA${i}`, mimeType: 'image/gif' as const })) }, replies: [ok(decisionsAnswer)] });
  add({ name: 'decisions: a budget trims at render', judge: 'decisions', options: { contextTokens: 3000 }, input: many, replies: [ok(decisionsAnswer)] });
  add({ name: 'decisions: a context-length 400 with images halves and keeps the images', judge: 'decisions', options: {}, input: { ...many, images: images.slice(0, 1) }, replies: [tooLong('decisions'), ok(decisionsAnswer)] });

  const llmReply = { route: 'note', probabilities: { note: 0.6, skip: 0.4, wait: 'x', bogus: 1 }, predicates: { asked: 0.3, route: 'no', extra: 1 } };
  add({ name: 'llm: the full request and parsed reply', judge: 'llm', options: { criteria: { queue: 'option queue' } }, input: { ...base, images: images.slice(0, 1) }, replies: [{ value: llmReply }] });
  add({ name: 'llm: a custom system, no predicates', judge: 'llm', options: { system: 'Decide.' }, input: plain, replies: [{ value: { route: 'skip', probabilities: {} } }] });
  add({ name: 'llm: a reply that is not an object retries', judge: 'llm', options: {}, input: plain, replies: [{ value: ['note'] }] });
  add({ name: 'llm: a reply with a route not offered retries', judge: 'llm', options: {}, input: plain, replies: [{ value: { route: 'wait' } }] });
  add({ name: 'llm: null reply', judge: 'llm', options: {}, input: plain, replies: [{ value: null }] });
  add({ name: 'llm: a refusal propagates as permanent', judge: 'llm', options: {}, input: plain, replies: [{ throws: { message: 'judge refused', permanent: true } }] });
  add({ name: 'llm: a context-length rejection halves once', judge: 'llm', options: { contextTokens: 8000 }, input: many, replies: [{ throws: { message: 'prompt is too long: 9000 tokens', status: 400 } }, { value: llmReply }] });
  add({ name: 'llm: an SDK context-length code halves once, then fails', judge: 'llm', options: {}, input: many, replies: [{ throws: { message: 'bad', status: 400, code: 'context_length_exceeded' } }] });
  add({ name: 'llm: a 400 that is not about context is thrown as is', judge: 'llm', options: {}, input: many, replies: [{ throws: { message: 'bad schema', status: 400 } }] });
  return { cases: cases.map((c) => c) as JudgeCase[] };
}

/** Records each case's outcome next to it. */
export async function recordJudgeFixtures(): Promise<{ cases: JudgeCase[] }> {
  const { cases } = judgeFixtures();
  for (const c of cases) c.expected = await runJudgeCase(c);
  return { cases: JSON.parse(JSON.stringify(cases)) as JudgeCase[] };
}

export { ALL };
