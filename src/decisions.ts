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
 */
import { PermanentError } from './errors.js';
import { DEFAULT_SYSTEM, renderState, resolveCriteria } from './internal/prompt.js';
import { isRecord } from './internal/shared.js';
import type { Judge, Judgment, Route } from './types.js';

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
}

/**
 * Throws a plain Error, which the core retries, on a network failure, a 408, a 429, a 5xx or a
 * reply without an offered route. Throws a `PermanentError` on any other 4xx. On abort it
 * rethrows the signal's reason.
 */
export function decisionsJudge(options: DecisionsJudgeOptions): Judge {
  const { apiKey, model, endpoint = 'https://api.openai.com/v1/decisions', criteria, instructions = DEFAULT_SYSTEM } = options;
  // Held unbound: calling the global fetch as a method of `options` throws in browsers and workers.
  const send = options.fetch ?? fetch;
  return async (input, signal) => {
    const routes = resolveCriteria(input.routes, criteria);
    const body = {
      model,
      input: renderState(input),
      questions: [
        { type: 'choice', name: 'route', instructions, choices: routes.map((r) => ({ value: r.route, description: r.criterion })) },
        ...input.predicates.map((p, index) => ({ type: 'predicate', name: predicateQuestion(index), instructions: p.question })),
      ],
    };
    let res: Response;
    try {
      res = await send(endpoint, {
        method: 'POST',
        headers: { authorization: `Bearer ${apiKey}`, 'content-type': 'application/json' },
        body: JSON.stringify(body),
        signal,
      });
    } catch (error) {
      signal.throwIfAborted();
      throw new Error(`decisionsJudge: request failed: ${error instanceof Error ? error.message : String(error)}`, { cause: error });
    }
    if (!res.ok) throw await statusError(res);
    return parseAnswers(
      await res.json(),
      routes.map((r) => r.route),
      input.predicates.map((p) => p.name),
    );
  };
}

function predicateQuestion(index: number): string {
  return `predicate_${index}`;
}

async function statusError(res: Response): Promise<Error> {
  const detail = (await res.text().catch(() => '')).slice(0, 500);
  const message = `decisionsJudge: HTTP ${res.status}${detail === '' ? '' : `: ${detail}`}`;
  const permanent = res.status >= 400 && res.status < 500 && res.status !== 408 && res.status !== 429;
  return permanent ? new PermanentError(message) : new Error(message);
}

/** Only the route is load-bearing; probabilities and predicates keep what parses, as in `attend/llm`. */
function parseAnswers(body: unknown, routes: Route[], predicates: string[]): Judgment {
  const answers = isRecord(body) && Array.isArray(body.answers) ? body.answers.filter(isRecord) : null;
  if (answers === null) throw new Error('decisionsJudge: response has no answers array');
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
  return { route, probabilities, predicates: answered };
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}
