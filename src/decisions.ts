/**
 * `attend/decisions`: a judge on the OpenAI Decisions API. One `choice` question named `route`
 * picks among the offered routes, with the criteria as the choice descriptions, and one
 * `predicate` question answers each ride-along predicate. Predicate questions are named by
 * position (`predicate_0`, `predicate_1`, …) so no predicate name can collide with `route`, and
 * the answers come back under the predicates' own names. The rendered state is the `input`.
 * Runs anywhere `fetch` does.
 *
 * ```ts
 * import { decisionsJudge } from 'attend/decisions';
 *
 * export const judge = decisionsJudge({ apiKey: process.env.OPENAI_API_KEY ?? '', model: '<model id>' });
 * ```
 *
 * Images on the judge input go as `input_image` data URLs after the text, at most 128 of them.
 * The API documents no format or size limits, so a request it rejects over its images (400, 413,
 * 415 or 422) is retried once without them, and that is reported through `onImageError`.
 */
import { PermanentError } from './errors.js';
import { isImageRejection, postJson, type ProviderFailure } from './internal/http.js';
import { DEFAULT_SYSTEM, renderState, resolveCriteria } from './internal/prompt.js';
import { isRecord } from './internal/shared.js';
import type { Judge, JudgeImage, Judgment, Route } from './types.js';

export type { ProviderFailure } from './internal/http.js';

export interface DecisionsJudgeOptions {
  apiKey: string;
  /** Required: attend does not choose a model for you. */
  model: string;
  /** Default `https://api.openai.com/v1/decisions`. */
  endpoint?: string;
  /** Default the global `fetch`. */
  fetch?: typeof fetch;
  /** Per-route criteria, sent as the choice descriptions. A criterion on the judge input wins, and `DEFAULT_CRITERIA` fills the rest. */
  criteria?: Partial<Record<Route, string>>;
  /** Instructions for the route question. Default `DEFAULT_SYSTEM` from `attend/llm`. */
  instructions?: string;
  /** Default true. False never sends images, whatever the input carries. */
  images?: boolean;
  /** Called once per image dropped before the call, and once when the provider rejected the images and the call was retried without them. */
  onImageError?: (error: Error) => void;
}

const PROVIDER = { name: 'decisionsJudge', requestIdHeader: 'x-request-id' };
const MAX_QUESTIONS = 200;
const MAX_IMAGES = 128;

/**
 * Throws a plain Error, which the core retries, on a network failure, a 408, a 429, a 5xx or a
 * reply without an offered route. Throws a `PermanentError` on any other 4xx, on a 429 whose code
 * is a billing or quota code, and when the model refuses the route question. Both carry
 * `ProviderFailure` when a response caused them. On abort it rethrows the signal's reason.
 */
export function decisionsJudge(options: DecisionsJudgeOptions): Judge {
  const { apiKey, model, endpoint = 'https://api.openai.com/v1/decisions', criteria, instructions = DEFAULT_SYSTEM, onImageError } = options;
  // Held unbound: calling the global fetch as a method of `options` throws in browsers and workers.
  const send = options.fetch ?? fetch;
  return async (input, signal) => {
    const names = input.predicates.map((p) => p.name);
    if (new Set(names).size !== names.length) throw new PermanentError(`decisionsJudge: predicate names must be unique, got ${names.join(', ')}`);
    if (names.length + 1 > MAX_QUESTIONS) throw new PermanentError(`decisionsJudge: ${names.length} predicates plus the route is over the ${MAX_QUESTIONS} questions the API takes`);
    const routes = resolveCriteria(input.routes, criteria);
    const text = renderState(input);
    const images = options.images === false ? [] : keep(input.images ?? [], onImageError);
    const questions = [
      { type: 'choice', name: 'route', instructions, choices: routes.map((r) => ({ value: r.route, description: r.criterion })) },
      ...input.predicates.map((p, index) => ({ type: 'predicate', name: predicateQuestion(index), instructions: p.question })),
    ];
    const post = (withImages: boolean) => {
      const parts = images.map((image) => ({ type: 'input_image', image_url: `data:${image.mimeType};base64,${image.data}` }));
      const body = { model, input: withImages && parts.length > 0 ? [{ role: 'user', content: [{ type: 'input_text', text }, ...parts] }] : text, questions };
      return postJson(PROVIDER, send, endpoint, { authorization: `Bearer ${apiKey}` }, body, signal);
    };
    let reply: unknown;
    try {
      reply = await post(true);
    } catch (error) {
      if (images.length === 0 || !isImageRejection(error)) throw error;
      onImageError?.(new Error(`decisionsJudge: the provider rejected the request with its ${images.length} images, retrying without them: ${error instanceof Error ? error.message : String(error)}`, { cause: error }));
      reply = await post(false);
    }
    return parseAnswers(
      reply,
      routes.map((r) => r.route),
      names,
    );
  };
}

/** The first 128 images; the rest are dropped and reported. */
function keep(images: JudgeImage[], onImageError: ((error: Error) => void) | undefined): JudgeImage[] {
  for (let index = MAX_IMAGES; index < images.length; index += 1) onImageError?.(new Error(`decisionsJudge: image ${index} dropped: the API takes at most ${MAX_IMAGES} images per request`));
  return images.slice(0, MAX_IMAGES);
}

function predicateQuestion(index: number): string {
  return `predicate_${index}`;
}

/**
 * Only the route is load-bearing; probabilities, predicates and confidence keep what parses, as
 * in `attend/llm`. A refusal of the route question is permanent, since the same window would be
 * refused again. A refused predicate is left unanswered.
 */
function parseAnswers(body: unknown, routes: Route[], predicates: string[]): Judgment {
  const answers = isRecord(body) && Array.isArray(body.answers) ? body.answers.filter(isRecord) : null;
  if (answers === null) throw new Error('decisionsJudge: response has no answers array');
  if (answers.some((a) => a.type === 'refusal' && a.name === 'route')) throw new PermanentError('decisionsJudge: the model refused to answer the route question');
  const choice = answers.find((a) => a.type === 'choice' && a.name === 'route');
  const route = routes.find((r) => r === choice?.choice);
  if (choice === undefined || route === undefined) {
    throw new Error(`decisionsJudge: route ${JSON.stringify(choice?.choice)} is not one of ${routes.join(', ')}`);
  }
  const probabilities: Record<string, number> = {};
  for (const entry of Array.isArray(choice.probabilities) ? choice.probabilities.filter(isRecord) : []) {
    const value = routes.find((r) => r === entry.value);
    if (value !== undefined && isFiniteNumber(entry.probability)) probabilities[value] = entry.probability;
  }
  const answered: Record<string, number> = {};
  for (const [index, name] of predicates.entries()) {
    const answer = answers.find((a) => a.type === 'predicate' && a.name === predicateQuestion(index));
    if (answer !== undefined && isFiniteNumber(answer.probability)) answered[name] = answer.probability;
  }
  const judgment: Judgment = { route, probabilities, predicates: answered };
  if (isFiniteNumber(choice.confidence)) judgment.confidence = choice.confidence;
  return judgment;
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}
