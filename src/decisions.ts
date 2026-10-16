/**
 * `attend/decisions`: a judge on the OpenAI Decisions API. One `choice` question named `route`
 * picks among the offered routes, with the criteria as the choice descriptions, and one
 * `predicate` question answers each ride-along predicate. Predicate questions are named by
 * position (`predicate_0`, `predicate_1`, …) so no predicate name can collide with `route`, and
 * the answers come back under the predicates' own names. The rendered state is the `input`.
 * Runs anywhere `fetch` does. With `contextTokens` it declares a budget; see `jevJudge` for how
 * a window is trimmed to it and halved once on a context-length rejection.
 *
 * ```ts
 * import { decisionsJudge } from '@acquiredintelligence/attend/decisions';
 *
 * export const judge = decisionsJudge({ apiKey: process.env.OPENAI_API_KEY ?? '', model: '<model id>' });
 * ```
 *
 * Images on the judge input go as `input_image` data URLs after the text, at most 128 of them.
 * The API documents no format or size limits, so a request it rejects over its images (400, 413,
 * 415 or 422) is retried once without them, and that is reported through `onImageError`.
 */
import type { JudgeBudget } from './core/types.js';
import { PermanentError } from './errors.js';
import { isContextLengthRejection, isImageRejection, postJson, type ProviderFailure } from './internal/http.js';
import { DEFAULT_SYSTEM, renderBytes, renderWithin, resolveCriteria, sendHalving, stateText, tokensOf, type Rendered } from './internal/prompt.js';
import { isRecord } from './internal/shared.js';
import { ROUTES, type Judge, type JudgeImage, type Judgment, type Route } from './types.js';

export type { ProviderFailure } from './internal/http.js';

export interface DecisionsJudgeOptions {
  apiKey: string;
  /** The model's context window in tokens. Set it to declare a budget. */
  contextTokens?: number;
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
 * With `contextTokens`, `budget.reservedTokens` is measured from the route question with every
 * route on offer. Throws a plain Error, which the core retries, on a network failure, a 408, a
 * 429, a 5xx, a reply without an offered route, or a context-length rejection that halving the
 * window did not clear. Throws a `PermanentError` on any other 4xx, on a 429 whose code is a
 * billing or quota code, and when the model refuses the route question. Both carry
 * `ProviderFailure` when a response caused them. On abort it rethrows the signal's reason.
 */
export function decisionsJudge(options: DecisionsJudgeOptions): Judge & { budget?: JudgeBudget } {
  const { apiKey, model, endpoint = 'https://api.openai.com/v1/decisions', criteria, instructions = DEFAULT_SYSTEM, onImageError, contextTokens } = options;
  // Held unbound: calling the global fetch as a method of `options` throws in browsers and workers.
  const send = options.fetch ?? fetch;
  const reservedTokens = tokensOf(
    JSON.stringify({
      type: 'choice',
      name: 'route',
      instructions,
      choices: resolveCriteria(
        ROUTES.map((route) => ({ route })),
        criteria,
      ).map((r) => ({ value: r.route, description: r.criterion })),
    }),
  );
  if (contextTokens !== undefined && !(contextTokens > reservedTokens))
    throw new RangeError(`decisionsJudge: contextTokens must be more than the ${reservedTokens} tokens its own prompt takes, got ${contextTokens}`);
  const budget = contextTokens === undefined ? undefined : { contextTokens, reservedTokens };
  const judge: Judge & { budget?: JudgeBudget } = async (input, signal) => {
    const names = input.predicates.map((p) => p.name);
    if (new Set(names).size !== names.length) throw new PermanentError(`decisionsJudge: predicate names must be unique, got ${names.join(', ')}`);
    if (names.length + 1 > MAX_QUESTIONS) throw new PermanentError(`decisionsJudge: ${names.length} predicates plus the route is over the ${MAX_QUESTIONS} questions the API takes`);
    const routes = resolveCriteria(input.routes, criteria);
    const rendered = await renderWithin(input, renderBytes(budget), signal);
    const images = options.images === false ? [] : keep(input.images ?? [], onImageError);
    const questions = [
      { type: 'choice', name: 'route', instructions, choices: routes.map((r) => ({ value: r.route, description: r.criterion })) },
      ...input.predicates.map((p, index) => ({ type: 'predicate', name: predicateQuestion(index), instructions: p.question })),
    ];
    let withImages = images.length > 0;
    const post = (r: Rendered) => {
      const text = stateText(r);
      const parts = images.map((image) => ({ type: 'input_image', image_url: `data:${image.mimeType};base64,${image.data}` }));
      const body = { model, input: withImages ? [{ role: 'user', content: [{ type: 'input_text', text }, ...parts] }] : text, questions };
      return postJson(PROVIDER, send, endpoint, { authorization: `Bearer ${apiKey}` }, JSON.stringify(body), signal);
    };
    const reply = await sendHalving(rendered, async (r) => {
      try {
        return await post(r);
      } catch (error) {
        if (!withImages || !isImageRejection(error) || isContextLengthRejection(error)) throw error;
        withImages = false;
        onImageError?.(
          new Error(`decisionsJudge: the provider rejected the request with its ${images.length} images, retrying without them: ${error instanceof Error ? error.message : String(error)}`, {
            cause: error,
          }),
        );
        return post(r);
      }
    });
    return parseAnswers(
      reply,
      routes.map((r) => r.route),
      names,
    );
  };
  if (budget !== undefined) judge.budget = budget;
  return judge;
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
