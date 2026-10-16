# Judges

A judge is `(input: JudgeInput, signal: AbortSignal) => Promise<Judgment>`.

## The input

| Field | Holds |
|---|---|
| `scope` | The scope key |
| `window` | The settled window: `events` with kinds, `reason`, `waits`, `dropped`, `gap` |
| `routes` | The routes on offer, each with `criterion` when `AttendConfig.criteria` sets one. `wait` is absent when the window may not wait. |
| `guidance` | `AttendConfig.guidance`, when set |
| `predicates` | Yes-or-no questions, as `{ name, question }`, from `AttendConfig.predicates` and `context()` |
| `facts` | `context(scope, signal).facts`, fetched on every call |
| `images` | What `AttendConfig.capture` returned for this call, when it returned any |
| `recent` | The scope's last verdicts, oldest first, at most `recentCount` |
| `now` | attend's clock |

## The answer

Return `{ route, probabilities?, predicates?, confidence? }`.

- `route` must be one of `input.routes`. Any other route fails the attempt, and the attempt is retried.
- `probabilities` maps offered routes to numbers from 0 to 1. attend sets routes that were not offered to 0 and renormalises the rest. When they sum to 0, the chosen route gets 1.
- `predicates` maps predicate names to numbers from 0 to 1. attend drops values outside that range.
- `confidence` is a number from 0 to 1 when the judge has one, such as a classifier's confidence in the route. It reaches the sink as `decision.confidence`, null when absent.
- Throw to retry with backoff from `retryBackoffMs`. Throw `PermanentError` from `attend` to dead-letter the window at once.
- Stop when `signal` aborts. attend aborts the call after `judgeTimeoutMs`.

## llmJudge

`llmJudge({ complete })` from `attend/llm` writes the system prompt, renders the input as compact JSON, builds a strict JSON Schema for the answer, and validates the reply. `complete({ system, prompt, schema, signal })` makes one structured-output call and resolves with the parsed JSON.

Claude:

```ts
import Anthropic from '@anthropic-ai/sdk';
import { PermanentError } from 'attend';
import { llmJudge } from 'attend/llm';

const anthropic = new Anthropic();

export const judge = llmJudge({
  complete: async ({ system, prompt, images, schema, signal }) => {
    const res = await anthropic.messages.create(
      {
        model: 'claude-opus-5-5',
        max_tokens: 1024,
        system,
        messages: [
          {
            role: 'user',
            content: [
              { type: 'text', text: prompt },
              ...images.map((image) => ({ type: 'image' as const, source: { type: 'base64' as const, media_type: image.mimeType, data: image.data } })),
            ],
          },
        ],
        output_config: { effort: 'low', format: { type: 'json_schema', schema } },
      },
      { signal },
    );
    if (res.stop_reason === 'refusal') throw new PermanentError('judge refused');
    const text = res.content.find((b) => b.type === 'text');
    return JSON.parse(text?.type === 'text' ? text.text : 'null');
  },
});
```

- Use `claude-opus-5-5`, or `claude-haiku-5-5` for a cheaper judge. Write the model id without a date suffix.
- Request structured output with `output_config.format`. Keep `effort: 'low'` for a classifier, and leave `thinking` and `tool_choice` out.
- Check `stop_reason === 'refusal'` before you read `content`. A refusal arrives with HTTP 200.
- Images go as `image` blocks with a base64 `source`. PNG, JPEG, GIF and WEBP are accepted, at most 10 MB each and 8000 px per side (2000 px when a request has more than 20 images).

OpenAI Responses API:

```ts
import OpenAI from 'openai';
import { PermanentError } from 'attend';
import { llmJudge } from 'attend/llm';

const openai = new OpenAI();

export const judge = llmJudge({
  complete: async ({ system, prompt, images, schema, signal }) => {
    const res = await openai.responses.create(
      {
        model: '<model id>',
        instructions: system,
        input: [
          {
            role: 'user',
            content: [
              { type: 'input_text', text: prompt },
              ...images.map((image) => ({ type: 'input_image' as const, image_url: `data:${image.mimeType};base64,${image.data}`, detail: 'auto' as const })),
            ],
          },
        ],
        text: { format: { type: 'json_schema', name: 'judgment', schema, strict: true } },
      },
      { signal },
    );
    if (res.output.some((item) => item.type === 'message' && item.content.some((part) => part.type === 'refusal'))) throw new PermanentError('judge refused');
    return JSON.parse(res.output_text);
  },
});
```

- A refusal is a `refusal` content part inside the `message` output item, not a status code. `response.status === 'incomplete'` means the answer was cut off; treat it as a retryable failure.
- Images go as `input_image` parts with a `data:` URL. PNG, JPEG, WEBP and non-animated GIF are accepted.

Gemini, through `@google/genai`. This snippet was written without the package installed, so check `generateContent` and the config fields against the version you use:

```ts
import { GoogleGenAI } from '@google/genai';
import { PermanentError } from 'attend';
import { llmJudge } from 'attend/llm';

const ai = new GoogleGenAI({});

export const judge = llmJudge({
  complete: async ({ system, prompt, images, schema, signal }) => {
    const res = await ai.models.generateContent({
      model: '<model id>',
      contents: [{ role: 'user', parts: [{ text: prompt }, ...images.map((image) => ({ inlineData: { mimeType: image.mimeType, data: image.data } }))] }],
      config: { systemInstruction: system, responseMimeType: 'application/json', responseJsonSchema: schema, abortSignal: signal },
    });
    const finish = res.candidates?.[0]?.finishReason;
    if (res.promptFeedback?.blockReason !== undefined || (finish !== undefined && finish !== 'STOP' && finish !== 'MAX_TOKENS')) throw new PermanentError('judge refused');
    return JSON.parse(res.text ?? 'null');
  },
});
```

- A blocked prompt has `promptFeedback.blockReason` and no candidates; a blocked answer has a `finishReason` such as `SAFETY`. Both are refusals. `MAX_TOKENS` is truncation, and the JSON parse then fails as a retryable error.
- Images go as `inlineData` parts. PNG, JPEG, WEBP, HEIC and HEIF are accepted; GIF is not. Keep the request under 20 MB.
- The JS SDK does not retry on its own. attend retries the judge call, so leave `httpOptions.retryOptions` unset.

Vercel AI SDK. This snippet was written without the `ai` package installed, so check `generateObject` and `jsonSchema` against the version you use:

```ts
import { openai } from '@ai-sdk/openai';
import { generateObject, jsonSchema } from 'ai';
import { llmJudge } from 'attend/llm';

export const judge = llmJudge({
  complete: async ({ system, prompt, images, schema, signal }) => {
    const { object } = await generateObject({
      model: openai('<model id>'),
      system,
      messages: [{ role: 'user', content: [{ type: 'text', text: prompt }, ...images.map((image) => ({ type: 'image' as const, image: image.data, mediaType: image.mimeType }))] }],
      schema: jsonSchema(schema),
      abortSignal: signal,
    });
    return object;
  },
});
```

Options:

- `system` replaces `DEFAULT_SYSTEM`. attend appends the offered routes and their criteria either way.
- `criteria` sets per-route criteria. A criterion from `AttendConfig.criteria` wins, and `DEFAULT_CRITERIA` fills the rest.
- `render(input)` replaces the default user prompt, which is compact JSON of the guidance, the window, the facts, the recent routes and the predicate questions.

## jevJudge

```ts
import { jevJudge } from 'attend/jev';

export const judge = jevJudge({ apiKey: process.env.TYPESAFE_API_KEY ?? '' });
```

It calls TypeSafe's Jev at `POST /v1/systemone` with the user's own key. It sends one `choice` question keyed `route`, with each offered route's criterion as its option description, one `noul` question per predicate keyed by position, and the rendered state as JSON `state`. The answer's `probabilities` and `confidence` reach the judgment.

Options:

- `model`: `jev-latest` by default, which is the latest stable release (`jev-1.13.0` today). `jev-preview` is the newest release (`jev-1.14.0` today). `jev-1.14.0` and `jev-1.13.0` pin a version; pin one when thresholds on `confidence` are tuned.
- `endpoint`: `https://api.typesafe.ai/v1/systemone` by default. OpenRouter serves the same API at `https://openrouter.ai/api/v1/systemone`, and LiteLLM at its own host under the same path.
- `longContext: true` sends `TypeSafe-Danger-Zone-Long-Context: true`, which raises the per-request and per-question context from 64k and 32k tokens to 262k. It is slower and less accurate, so use it only when the state is long.
- `criteria`, `instructions` and `fetch`, as on `decisionsJudge`.
- `images: false` never sends images. `onImageError(error)` hears about every image dropped before a call and about a call retried without images. See [Images](#images).

## decisionsJudge

```ts
import { decisionsJudge } from 'attend/decisions';

export const judge = decisionsJudge({ apiKey: process.env.OPENAI_API_KEY ?? '', model: '<model id>' });
```

It sends one `choice` question named `route`, with each offered route's criterion as its description, and one `predicate` question per predicate. Pass `fetch` to run it on a custom transport, and `endpoint` to point it elsewhere. `images: false` and `onImageError` work as on `jevJudge`.

## Errors from the hosted judges

Both `jevJudge` and `decisionsJudge` classify a response the same way:

| Response | Thrown | attend does |
|---|---|---|
| Network failure, timeout, 408, 429, 5xx, 529 | `Error` | Retries with backoff |
| 429 with a billing or quota code (`credit_balance_exhausted`, `organization_spend_limit_exceeded`, `project_spend_limit_exceeded`, `organization_usage_limit_exceeded`, `insufficient_quota`) | `PermanentError` | Dead-letters at once |
| Any other 4xx, including 400 for a request over the context length and 413 for one over the body limit | `PermanentError` | Dead-letters at once |
| A refusal of the route question (Decisions only; Jev has no refusals) | `PermanentError` | Dead-letters at once |
| A reply whose route was not offered | `Error` | Retries |

Every error from a response carries `status`, `requestId` (from `x-typesafe-request-id` or `x-request-id`) and `retryAfterMs` (from `retry-after-ms` or `Retry-After`, else null). attend's backoff does not read `retryAfterMs` yet; it is there for a wrapper that wants to.

Before a call, a duplicate predicate name throws `PermanentError`, and so does a Decisions call with more than 199 predicates, since the API takes 200 questions and the route is one.

## Images

`createAttend({ capture })` adds images to every judge call:

```ts
import { createAttend, memoryStore, routes } from 'attend';
import type { JudgeImage } from 'attend';

declare function screenshot(scope: string, signal: AbortSignal): Promise<JudgeImage>;

export const attend = createAttend({
  store: memoryStore(),
  kind: () => 'closing',
  judge: async () => ({ route: 'queue' }),
  sink: routes({ interrupt: async () => {}, queue: async () => {}, note: async () => {}, ask: async () => {} }),
  deadLetter: async () => {},
  capture: (scope, signal) => screenshot(scope, signal),
  captureTimeoutMs: 500,
});
```

- `capture(scope, signal)` returns one `JudgeImage`, a list, or null. A `JudgeImage` is `{ data, mimeType }` with `data` as base64 and no `data:` prefix, and `mimeType` one of `image/png`, `image/jpeg`, `image/webp` or `image/gif`.
- A capture that throws or outlives `captureTimeoutMs` (default 500) never fails the judge call. The judge runs without images, and `observe` sees one `captureFailed` trace with the message.
- `jevJudge` sends only PNG and JPEG, each side 280 to 1568 px, at most 16 MiB per file and 32 MiB in all, and drops anything else before the call. The provider never resizes, so capture at a size inside those limits. Images on this provider are experimental, and a request it still rejects over its images (400, 413, 415 or 422) is retried once without them.
- `decisionsJudge` sends up to 128 images of any format as `input_image` data URLs, and retries once without them on the same statuses, since the API documents no format or size limits.
- `llmJudge` hands the images to `complete` as `images`. The snippets above place them after the prompt in each model's own block format.

## A function judge

Write a plain function when a rule decides. Read `input.routes` before you answer `wait`:

```ts
import type { Judge } from 'attend';

export const judge: Judge = async ({ window, routes }) => {
  const has = (name: string) => window.events.some((e) => e.name === name);
  if (window.reason === 'flush') return { route: 'interrupt' };
  if (has('build.failed')) return { route: routes.some((o) => o.route === 'wait') ? 'wait' : 'queue' };
  return { route: 'skip' };
};
```

Judges compose as functions. Wrap a model judge so rules answer first:

```ts
import type { Judge } from 'attend';

export const withShortcut =
  (judge: Judge): Judge =>
  async (input, signal) =>
    input.window.reason === 'flush' ? { route: 'interrupt' } : judge(input, signal);
```

A hosted classifier is a judge too: send `input` as JSON, map its answer to `{ route }`, throw on retryable errors, and throw `PermanentError` on the rest.

## Configuring what the judge sees

```ts
import { createAttend, memoryStore, routes } from 'attend';

export const attend = createAttend({
  store: memoryStore(),
  kind: () => 'closing',
  judge: async () => ({ route: 'queue' }),
  sink: routes({ interrupt: async () => {}, queue: async () => {}, note: async () => {}, ask: async () => {} }),
  deadLetter: async () => {},
  guidance: 'An on-call agent for the payments service. Its user wants pages for customer impact only.',
  criteria: { interrupt: 'Customers are failing to pay right now.' },
  predicates: { asked: 'Did the user ask the agent to handle this?' },
  context: async (scope) => ({ facts: { midTurn: false, scope } }),
});
```

Each predicate's answer arrives on `decision.predicates[name]` as a probability. A sink never sees `skip`, so to wake the agent on a skipped window whose predicate is high, wrap the judge: call it, then replace `skip` with another route when the predicate passes your threshold.
