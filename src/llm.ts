/**
 * `attend/llm`: a judge over any model that can reply in JSON matching a schema. There is no SDK
 * dependency: you pass the one structured-output call as `complete`.
 *
 * Anthropic:
 *
 * ```ts
 * import Anthropic from '@anthropic-ai/sdk';
 * import { PermanentError } from 'attend';
 * import { llmJudge } from 'attend/llm';
 *
 * const anthropic = new Anthropic();
 *
 * export const judge = llmJudge({
 *   complete: async ({ system, prompt, schema, signal }) => {
 *     const res = await anthropic.messages.create(
 *       {
 *         model: 'claude-opus-5-5',
 *         max_tokens: 1024,
 *         system,
 *         messages: [{ role: 'user', content: prompt }],
 *         output_config: { effort: 'low', format: { type: 'json_schema', schema } },
 *       },
 *       { signal },
 *     );
 *     if (res.stop_reason === 'refusal') throw new PermanentError('judge refused');
 *     const text = res.content.find((b) => b.type === 'text');
 *     return JSON.parse(text?.type === 'text' ? text.text : 'null');
 *   },
 * });
 * ```
 *
 * OpenAI Responses API:
 *
 * ```ts
 * import OpenAI from 'openai';
 * import { llmJudge } from 'attend/llm';
 *
 * const openai = new OpenAI();
 *
 * export const judge = llmJudge({
 *   complete: async ({ system, prompt, schema, signal }) => {
 *     const res = await openai.responses.create(
 *       {
 *         model: '<model id>',
 *         instructions: system,
 *         input: prompt,
 *         text: { format: { type: 'json_schema', name: 'judgment', schema, strict: true } },
 *       },
 *       { signal },
 *     );
 *     return JSON.parse(res.output_text);
 *   },
 * });
 * ```
 *
 * Vercel AI SDK. Unverified: written without the `ai` package installed, so check
 * `generateObject` and `jsonSchema` against the version you use.
 *
 * ```ts
 * import { openai } from '@ai-sdk/openai';
 * import { generateObject, jsonSchema } from 'ai';
 * import { llmJudge } from 'attend/llm';
 *
 * export const judge = llmJudge({
 *   complete: async ({ system, prompt, schema, signal }) => {
 *     const { object } = await generateObject({
 *       model: openai('<model id>'),
 *       system,
 *       prompt,
 *       schema: jsonSchema(schema),
 *       abortSignal: signal,
 *     });
 *     return object;
 *   },
 * });
 * ```
 */
import { DEFAULT_SYSTEM, renderState, resolveCriteria } from './internal/prompt.js';
import { isRecord } from './internal/shared.js';
import type { JsonObject, Judge, JudgeInput, Judgment, OfferedRoute, Route } from './types.js';

export { DEFAULT_CRITERIA, DEFAULT_SYSTEM } from './internal/prompt.js';

export interface CompleteRequest {
  system: string;
  prompt: string;
  /**
   * JSON Schema for `{ route, probabilities, predicates }`. Strict-mode safe: every object sets
   * `additionalProperties: false` and requires all of its properties. `predicates` is left out
   * when no predicate is asked.
   */
  schema: JsonObject;
  signal: AbortSignal;
}

/** One structured-output call. Resolve with the parsed JSON reply; throw a `PermanentError` on a refusal. */
export type Complete = (request: CompleteRequest) => Promise<unknown>;

export interface LlmJudgeOptions {
  complete: Complete;
  /** Replaces `DEFAULT_SYSTEM`. The routes on offer and their criteria are appended either way, since they change per call. */
  system?: string;
  /** Per-route criteria. A criterion on the judge input wins over these, and `DEFAULT_CRITERIA` fills the rest. */
  criteria?: Partial<Record<Route, string>>;
  /** Builds the user prompt from the judge input. The default renders the window, facts, recent routes and predicate questions as compact JSON. */
  render?: (input: JudgeInput) => string;
}

export function llmJudge(options: LlmJudgeOptions): Judge {
  const { complete, system = DEFAULT_SYSTEM, criteria, render = renderState } = options;
  return async (input, signal) => {
    const routes = resolveCriteria(input.routes, criteria);
    const offered = routes.map((r) => r.route);
    const asked = input.predicates.map((p) => p.name);
    const reply = await complete({
      system: systemPrompt(system, routes, asked.length > 0),
      prompt: render(input),
      schema: judgmentSchema(offered, asked),
      signal,
    });
    return parseReply(reply, offered, asked);
  };
}

function systemPrompt(base: string, routes: Required<OfferedRoute>[], asksPredicates: boolean): string {
  const answer = asksPredicates
    ? 'Answer with the route you choose, a probability from 0 to 1 for each route on offer, and for each entry under predicates the probability from 0 to 1 that the answer to its question is yes.'
    : 'Answer with the route you choose and a probability from 0 to 1 for each route on offer.';
  return [base, '', 'Routes on offer:', ...routes.map((r) => `- ${r.route}: ${r.criterion}`), '', answer].join('\n');
}

/** No `minimum`/`maximum`: Anthropic's structured outputs reject numeric bounds, so the range lives in the system prompt. */
function judgmentSchema(routes: Route[], predicates: string[]): JsonObject {
  const numbers = (keys: string[]): JsonObject => ({
    type: 'object',
    properties: Object.fromEntries(keys.map((key): [string, JsonObject] => [key, { type: 'number' }])),
    required: keys,
    additionalProperties: false,
  });
  const properties: JsonObject = { route: { type: 'string', enum: routes }, probabilities: numbers(routes) };
  if (predicates.length > 0) properties.predicates = numbers(predicates);
  return { type: 'object', properties, required: Object.keys(properties), additionalProperties: false };
}

/**
 * Only the route is load-bearing. Probabilities and predicates keep the finite numbers found
 * under known keys and drop the rest, because the reducer renormalises and a garbled ride-along
 * should not cost a retry.
 */
function parseReply(reply: unknown, routes: Route[], predicates: string[]): Judgment {
  if (!isRecord(reply)) throw new Error(`llmJudge: expected a JSON object, got ${describe(reply)}`);
  const route = routes.find((r) => r === reply.route);
  if (route === undefined) throw new Error(`llmJudge: route ${JSON.stringify(reply.route)} is not one of ${routes.join(', ')}`);
  return { route, probabilities: numbersAt(reply.probabilities, routes), predicates: numbersAt(reply.predicates, predicates) };
}

function numbersAt(value: unknown, keys: string[]): Record<string, number> {
  const out: Record<string, number> = {};
  if (!isRecord(value)) return out;
  for (const key of keys) {
    const n = value[key];
    if (typeof n === 'number' && Number.isFinite(n)) out[key] = n;
  }
  return out;
}

function describe(value: unknown): string {
  if (value === null) return 'null';
  return Array.isArray(value) ? 'an array' : typeof value;
}
