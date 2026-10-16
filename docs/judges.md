# Choose a judge

**The speed default is a system-one classifier.** `jevJudge` (TypeSafe's Jev, with your own TypeSafe key) and `decisionsJudge` (the OpenAI Decisions API) answer a fixed multiple-choice question with probabilities in roughly 140 ms or more, which is what a real-time agent can afford on every settled window. `llmJudge` calls a general model you already use; it is slower, and its self-reported probabilities are close to one-hot. Use it when you want reasoning over speed.

A judge is a function, `(input: JudgeInput, signal: AbortSignal) => Promise<Judgment>`. It gets the settled `window`, the `routes` on offer with your criteria, your `guidance`, the `predicates` to answer, live `facts` from `context()` and the scope's `recent` verdicts. It returns `{ route, probabilities?, predicates? }`.

- Throw to retry with backoff. Throw a `PermanentError` to dead-letter the window at once.
- A route that was not offered counts as a retryable failure. Check `input.routes` before you answer `wait`.
- Stop when `signal` aborts. attend aborts the call after `judgeTimeoutMs`.

## llmJudge

`llmJudge` from `@acquiredintelligence/attend/llm` works with any model that answers in JSON matching a schema. You pass that one call as `complete`. attend writes the prompt and the schema and validates the reply. With Claude:

```ts
import Anthropic from '@anthropic-ai/sdk';
import { PermanentError } from '@acquiredintelligence/attend';
import { llmJudge } from '@acquiredintelligence/attend/llm';

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

[`skills/attend/references/judges.md`](../skills/attend/references/judges.md) has the same judge on the OpenAI Responses API and the Vercel AI SDK. The default prompt is `DEFAULT_SYSTEM`, and each route's default criterion is in `DEFAULT_CRITERIA`. Override them with `system` and `criteria`.

## jevJudge

`jevJudge` from `@acquiredintelligence/attend/jev` calls TypeSafe's Jev, a classifier built for this kind of question, with your own TypeSafe key. It asks one `choice` question for the route and one `noul` question per predicate, and sends the state as JSON. The model defaults to `jev-latest`; `jev-preview`, `jev-1.14.0` and `jev-1.13.0` are the others. Pass `endpoint` to go through OpenRouter or LiteLLM, and `longContext: true` for 262k tokens of context at some cost in speed and accuracy. Its probabilities are real, so [the act threshold](#the-act-threshold) works with it.

```ts
import { jevJudge } from '@acquiredintelligence/attend/jev';

export const judge = jevJudge({ apiKey: process.env.TYPESAFE_API_KEY ?? '' });
```

## decisionsJudge

`decisionsJudge` from `@acquiredintelligence/attend/decisions` calls the OpenAI Decisions API. It asks one `choice` question for the route and one `predicate` question per predicate.

```ts
import { decisionsJudge } from '@acquiredintelligence/attend/decisions';

export const judge = decisionsJudge({ apiKey: process.env.OPENAI_API_KEY ?? '', model: '<model id>' });
```

Both hosted judges retry network errors and 408, 429 and 5xx responses, and dead-letter the window at once on any other 4xx, on a billing or quota 429, and when the model refuses the route question. Every error from a response carries `status`, `requestId` and `retryAfterMs`. Both return the classifier's `confidence` in the route on `decision.confidence`.

## The window budget

A window must never overload the judge that reads it. A judge declares its input budget (`jevJudge` declares 32k tokens, or 262k with `longContext`; `llmJudge` and `decisionsJudge` take `contextTokens`), and attend turns it into a size limit on the open window: `(contextTokens - reservedTokens) * windowShare * bytesPerToken`, with `windowShare` defaulting to 0.5. attend keeps a running size estimate for each window as events arrive. A window that reaches its share settles early with reason `size`, and a judge that still gets more than fits drops sheddable events (consequence and context, oldest first) when it renders and never an act or a flush. As a backstop, a provider's context-length rejection triggers exactly one retry with the window halved. A judge that declares no budget leaves the limit off. Set `settle.maxWindowBytes` yourself to override.

## The act threshold

By default the judge's route stands. A judge that returns real probabilities (`jevJudge`, `decisionsJudge`) also ranks how much the agent should act, and only your app knows whether an interruption or a missed moment costs more. Set `actThreshold` in (0, 1] on `createAttend`, and the route becomes an acting route (`interrupt` or `ask`, whichever is likelier and offered) exactly when `p(interrupt) + p(ask)` is at least the threshold, and a quieter route (`skip`, `note`, `queue` or `wait`, by probability) otherwise. It is off by default. A judgment without probabilities is never changed, and a changed route emits a `routeThresholded` trace. The threshold is applied before the decision is logged, so replays stay deterministic.

Pick it from your own traffic instead of guessing. Replay a recorded stream with the threshold off, record each judgment, and `suggestThreshold` from `@acquiredintelligence/attend/testing` returns the smallest threshold whose acts fit an interruptions-per-hour budget, with the rate at every candidate:

```ts
import { suggestThreshold } from '@acquiredintelligence/attend/testing';
import type { Judge, Judgment } from '@acquiredintelligence/attend';

export const judgments: Judgment[] = [];
export const recording = (judge: Judge): Judge => async (input, signal) => {
  const judgment = await judge(input, signal);
  judgments.push(judgment);
  return judgment;
};

// After replaying an hour of recorded events through createAttend({ judge: recording(judge), ... }):
export const { threshold, candidates } = suggestThreshold(judgments, { durationMs: 3_600_000, maxActsPerHour: 6 });
```

`threshold` is null when no threshold fits the budget. `llmJudge`'s probabilities are near one-hot, so a threshold changes little there.

## Images

Give `createAttend` a `capture(scope, signal)` that returns a `JudgeImage` (base64 `data` and a `mimeType`) or a list of them, such as a screenshot of what the user is looking at, and every judge call carries them as `input.images`. A capture that throws or takes longer than `captureTimeoutMs` (default 500) never fails the call: the judge runs without images and an observer sees a `captureFailed` trace. `jevJudge` sends PNG and JPEG within the provider's limits (280 to 1568 px per side, 16 MiB per file) and drops the rest, reporting each through `onImageError`; `decisionsJudge` sends up to 128 of any format. Either one retries once without images when the provider rejects them. `llmJudge` hands them to `complete` as `images`, as the snippet above shows.

## Your own judge

Any function with the judge signature works, and judges compose as functions. This one answers a direct message without calling a model:

```ts
import type { Judge } from '@acquiredintelligence/attend';

export const withShortcut =
  (judge: Judge): Judge =>
  async (input, signal) =>
    input.window.reason === 'flush' ? { route: 'interrupt' } : judge(input, signal);
```

To ask the judge yes-or-no questions beside the route, pass `predicates` to `createAttend`, such as `{ asked: 'Did the user ask the agent for this?' }`. Each answer arrives on `decision.predicates` as a probability.
