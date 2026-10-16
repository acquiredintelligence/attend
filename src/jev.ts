/**
 * `attend/jev`: a judge on TypeSafe's Jev, through `POST /v1/systemone`. One `choice` question
 * keyed `route` picks among the offered routes, with the criteria as the option descriptions, and
 * one `noul` question answers each ride-along predicate. Predicate questions are keyed by position
 * (`predicate_0`, `predicate_1`, …) so no predicate name can collide with `route`, and the answers
 * come back under the predicates' own names. The rendered state is sent as JSON `state`. Runs
 * anywhere `fetch` does. Bring your own TypeSafe key.
 *
 * ```ts
 * import { jevJudge } from 'attend/jev';
 *
 * export const judge = jevJudge({ apiKey: process.env.TYPESAFE_API_KEY ?? '' });
 * ```
 *
 * Models, as `model`:
 * - `jev-latest` (the default): the latest stable release, `jev-1.13.0` today.
 * - `jev-preview`: the newest release, `jev-1.14.0` today.
 * - `jev-1.14.0` and `jev-1.13.0`: pinned versions. Pin one if you tune thresholds on `confidence`.
 *
 * Images are experimental on the provider side ("Dreamer-ready"). PNG and JPEG only, each side 280
 * to 1568 px, at most 16 MiB per file and 32 MiB in all. An image outside those limits is dropped
 * before the call and reported through `onImageError`; a request the provider still rejects over its
 * images is retried once without them, and that is reported the same way.
 */
import { PermanentError } from './errors.js';
import { isImageRejection, postJson, type ProviderFailure } from './internal/http.js';
import { decodedLength, imageHeader, isStandardBase64, type ImageHeader } from './internal/images.js';
import { DEFAULT_SYSTEM, resolveCriteria, stateObject } from './internal/prompt.js';
import { isRecord } from './internal/shared.js';
import type { Judge, JudgeImage, Judgment, JsonObject, Route } from './types.js';

export type { ProviderFailure } from './internal/http.js';

export interface JevJudgeOptions {
  apiKey: string;
  /** Default `jev-latest`. */
  model?: string;
  /** Default `https://api.typesafe.ai/v1/systemone`. OpenRouter is `https://openrouter.ai/api/v1/systemone`; LiteLLM mounts the same path. */
  endpoint?: string;
  /** Default the global `fetch`. */
  fetch?: typeof fetch;
  /** Per-route criteria, sent as the option descriptions. A criterion on the judge input wins, and `DEFAULT_CRITERIA` fills the rest. */
  criteria?: Partial<Record<Route, string>>;
  /** Instructions for the route question. Default `DEFAULT_SYSTEM` from `attend/llm`. */
  instructions?: string;
  /** Sends `TypeSafe-Danger-Zone-Long-Context: true`: 262k tokens of context instead of 64k, slower and less accurate. */
  longContext?: boolean;
  /** Default true. False never sends images, whatever the input carries. */
  images?: boolean;
  /** Called once per image dropped before the call, and once when the provider rejected the images and the call was retried without them. */
  onImageError?: (error: Error) => void;
}

const PROVIDER = { name: 'jevJudge', requestIdHeader: 'x-typesafe-request-id' };

/** The provider's documented image limits. Every bound is inclusive. */
export const JEV_IMAGE_LIMITS = {
  fileBytes: 16 * 1024 * 1024,
  allFilesBytes: 32 * 1024 * 1024,
  minSide: 280,
  maxSide: 1568,
  minPixels: 78_400,
  maxPixels: 2_458_624,
} as const;

/**
 * Throws a plain Error, which the core retries, on a network failure, a 408, a 429, a 5xx or a
 * reply without an offered route. Throws a `PermanentError` on any other 4xx. Both carry
 * `ProviderFailure`. On abort it rethrows the signal's reason.
 */
export function jevJudge(options: JevJudgeOptions): Judge {
  const { apiKey, model = 'jev-latest', endpoint = 'https://api.typesafe.ai/v1/systemone', criteria, instructions = DEFAULT_SYSTEM, longContext = false, onImageError } = options;
  // Held unbound: calling the global fetch as a method of `options` throws in browsers and workers.
  const send = options.fetch ?? fetch;
  return async (input, signal) => {
    const names = input.predicates.map((p) => p.name);
    if (new Set(names).size !== names.length) throw new PermanentError(`jevJudge: predicate names must be unique, got ${names.join(', ')}`);
    const routes = resolveCriteria(input.routes, criteria);
    const questions: Record<string, JsonObject> = {
      route: { type: 'choice', instructions, criteria: Object.fromEntries(routes.map((r) => [r.route, r.criterion])) },
      ...Object.fromEntries(input.predicates.map((p, index) => [`predicate_${index}`, { type: 'noul', instructions: p.question }])),
    };
    const state = stateObject(input);
    const assets = options.images === false ? [] : jevAssets(input.images ?? [], onImageError);

    const post = (withImages: boolean) => {
      const headers: Record<string, string> = { authorization: `Bearer ${apiKey}` };
      if (longContext) headers['TypeSafe-Danger-Zone-Long-Context'] = 'true';
      const body: JsonObject = { model, state, questions };
      if (withImages && assets.length > 0) {
        headers['TypeSafe-Danger-Zone-Images'] = 'true';
        body.state = { ...state, images: assets.map((a) => a.ref) };
        body.assets = Object.fromEntries(assets.map((a) => [a.ref, a.asset]));
      }
      return postJson(PROVIDER, send, endpoint, headers, body, signal);
    };

    let reply: unknown;
    try {
      reply = await post(true);
    } catch (error) {
      if (assets.length === 0 || !isImageRejection(error)) throw error;
      onImageError?.(new Error(`jevJudge: the provider rejected the request with its ${assets.length} images, retrying without them: ${error instanceof Error ? error.message : String(error)}`, { cause: error }));
      reply = await post(false);
    }
    return parseAnswers(reply, routes.map((r) => r.route), names);
  };
}

interface JevAsset {
  ref: string;
  asset: { type: 'image'; format: 'png' | 'jpeg'; data_base64: string };
}

/** The images that pass the provider's format and size limits, each under a fresh reference no state text can contain by accident. */
function jevAssets(images: JudgeImage[], onImageError: ((error: Error) => void) | undefined): JevAsset[] {
  const kept: JevAsset[] = [];
  let totalBytes = 0;
  images.forEach((image, index) => {
    const checked = checkImage(image, totalBytes);
    if (typeof checked === 'string') {
      onImageError?.(new Error(`jevJudge: image ${index} dropped: ${checked}`));
      return;
    }
    totalBytes += decodedLength(image.data);
    kept.push({ ref: crypto.randomUUID(), asset: { type: 'image', format: checked.format, data_base64: image.data } });
  });
  return kept;
}

/** The image's header when it is within every documented limit, else why the provider would reject it. */
function checkImage(image: JudgeImage, totalBytes: number): ImageHeader | string {
  const L = JEV_IMAGE_LIMITS;
  if (image.mimeType !== 'image/png' && image.mimeType !== 'image/jpeg') return `${image.mimeType} is not accepted, only image/png and image/jpeg`;
  if (!isStandardBase64(image.data)) return 'data is not standard base64 with = padding and no whitespace';
  const bytes = decodedLength(image.data);
  if (bytes > L.fileBytes) return `${bytes} bytes is over the ${L.fileBytes} byte file limit`;
  if (totalBytes + bytes > L.allFilesBytes) return `${totalBytes + bytes} bytes of images is over the ${L.allFilesBytes} byte limit for all images`;
  const header = imageHeader(image.data);
  if (header === null) return 'the data is not a PNG or JPEG file with a readable header';
  if (header.animated) return 'animated images are not accepted';
  const { width, height } = header;
  if (width < L.minSide || height < L.minSide) return `${width}x${height} px has a side under the ${L.minSide} px minimum`;
  if (width > L.maxSide || height > L.maxSide) return `${width}x${height} px has a side over the ${L.maxSide} px maximum`;
  const pixels = width * height;
  if (pixels < L.minPixels) return `${width}x${height} px is ${pixels} pixels, under the ${L.minPixels} pixel minimum`;
  if (pixels > L.maxPixels) return `${width}x${height} px is ${pixels} pixels, over the ${L.maxPixels} pixel maximum`;
  return header;
}

/** Only the route is load-bearing; probabilities, predicates and confidence keep what parses, as in `attend/llm`. */
function parseAnswers(body: unknown, routes: Route[], predicates: string[]): Judgment {
  const answers = isRecord(body) && isRecord(body.answers) ? body.answers : null;
  if (answers === null) throw new Error('jevJudge: response has no answers map');
  const choice = isRecord(answers.route) && answers.route.type === 'choice' ? answers.route : undefined;
  const route = routes.find((r) => r === choice?.choice);
  if (choice === undefined || route === undefined) {
    throw new Error(`jevJudge: route ${JSON.stringify(choice?.choice)} is not one of ${routes.join(', ')}`);
  }
  const probabilities: Record<string, number> = {};
  const reported = isRecord(choice.probabilities) ? choice.probabilities : {};
  for (const r of routes) if (isFiniteNumber(reported[r])) probabilities[r] = reported[r];
  const answered: Record<string, number> = {};
  for (const [index, name] of predicates.entries()) {
    const answer = answers[`predicate_${index}`];
    if (isRecord(answer) && answer.type === 'noul' && isFiniteNumber(answer.noul)) answered[name] = answer.noul;
  }
  const judgment: Judgment = { route, probabilities, predicates: answered };
  if (isFiniteNumber(choice.confidence)) judgment.confidence = choice.confidence;
  return judgment;
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}
