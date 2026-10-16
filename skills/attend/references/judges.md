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
| `recent` | The scope's last verdicts, oldest first, at most `recentCount` |
| `now` | attend's clock |

## The answer

Return `{ route, probabilities?, predicates? }`.

- `route` must be one of `input.routes`. Any other route fails the attempt, and the attempt is retried.
- `probabilities` maps offered routes to numbers from 0 to 1. attend sets routes that were not offered to 0 and renormalises the rest. When they sum to 0, the chosen route gets 1.
- `predicates` maps predicate names to numbers from 0 to 1. attend drops values outside that range.
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
  complete: async ({ system, prompt, schema, signal }) => {
    const res = await anthropic.messages.create(
      {
        model: 'claude-opus-5-5',
        max_tokens: 1024,
        system,
        messages: [{ role: 'user', content: prompt }],
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

OpenAI Responses API:

```ts
import OpenAI from 'openai';
import { llmJudge } from 'attend/llm';

const openai = new OpenAI();

export const judge = llmJudge({
  complete: async ({ system, prompt, schema, signal }) => {
    const res = await openai.responses.create(
      {
        model: '<model id>',
        instructions: system,
        input: prompt,
        text: { format: { type: 'json_schema', name: 'judgment', schema, strict: true } },
      },
      { signal },
    );
    return JSON.parse(res.output_text);
  },
});
```

Vercel AI SDK. This snippet was written without the `ai` package installed, so check `generateObject` and `jsonSchema` against the version you use:

```ts
import { openai } from '@ai-sdk/openai';
import { generateObject, jsonSchema } from 'ai';
import { llmJudge } from 'attend/llm';

export const judge = llmJudge({
  complete: async ({ system, prompt, schema, signal }) => {
    const { object } = await generateObject({
      model: openai('<model id>'),
      system,
      prompt,
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
- `render(input)` replaces `renderState`, which builds the user prompt.

## decisionsJudge

```ts
import { decisionsJudge } from 'attend/decisions';

export const judge = decisionsJudge({ apiKey: process.env.OPENAI_API_KEY ?? '', model: '<model id>' });
```

It sends one `choice` question named `route`, with each offered route's criterion as its description, and one `predicate` question per predicate. attend retries network errors and 408, 429 and 5xx responses. Any other 4xx throws `PermanentError`. Pass `fetch` to run it on a custom transport, and `endpoint` to point it elsewhere.

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
  kind: () => ({ kind: 'closing' }),
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
