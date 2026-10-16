/**
 * What the hosted judges share at the HTTP boundary: one POST, one table from status to
 * retryable or permanent, the provider's error body parsed defensively, and the request id and
 * `Retry-After` carried on the thrown error.
 */
import { PermanentError } from '../errors.js';
import { isRecord } from './shared.js';

export interface Provider {
  /** Prefixes every error message. */
  name: string;
  requestIdHeader: string;
}

/** Fields every error thrown for a non-2xx response carries, retryable or not. */
export interface ProviderFailure {
  status: number;
  requestId: string | null;
  /** From `retry-after-ms` or `Retry-After`, when the response sent one. The core's backoff does not read it yet. */
  retryAfterMs: number | null;
}

/** 429 codes that a retry never clears: billing and quota, which openai-node would retry anyway. */
const QUOTA_CODES = new Set(['credit_balance_exhausted', 'organization_spend_limit_exceeded', 'project_spend_limit_exceeded', 'organization_usage_limit_exceeded', 'insufficient_quota']);

/** How OpenAI (including the Decisions API's "exceeds the token limit"), Anthropic, Gemini, TypeSafe and OpenRouter word a request too long for the model. */
const CONTEXT_LENGTH = /context[ _-]?(length|window)|max_tokens_exceeded|prompt is too long|input token count|too many (input )?tokens|token limit/i;
// TypeSafe names its code without saying which body field carries it, so the raw text is checked when the error is made.
const statusFailures = new WeakSet<Error>();
const contextLengthFailures = new WeakSet<Error>();

/** Statuses a provider returns when it rejects an image, when the request carried any. */
const IMAGE_REJECTIONS = new Set([400, 413, 415, 422]);

/**
 * POST `body` as JSON and return the parsed reply. A network failure is a retryable Error unless
 * `signal` aborted, in which case the signal's reason is thrown. A non-2xx response is classified
 * by `statusError`.
 */
export async function postJson(provider: Provider, send: typeof fetch, endpoint: string, headers: Record<string, string>, body: string, signal: AbortSignal): Promise<unknown> {
  // The judge renders the window across awaits before this, so the call may already be aborted.
  signal.throwIfAborted();
  let res: Response;
  try {
    res = await send(endpoint, { method: 'POST', body, headers: { 'content-type': 'application/json', ...headers }, signal });
  } catch (error) {
    signal.throwIfAborted();
    throw new Error(`${provider.name}: request failed: ${error instanceof Error ? error.message : String(error)}`, { cause: error });
  }
  if (!res.ok) throw statusError(provider, res, await res.text().catch(() => ''));
  try {
    return await res.json();
  } catch (error) {
    throw new Error(`${provider.name}: response is not JSON`, { cause: error });
  }
}

/**
 * Retryable (a plain Error): 408, 429 except the quota codes, and every 5xx, 529 included.
 * Permanent: everything else. Both carry `ProviderFailure`.
 */
export function statusError(provider: Provider, res: Response, text: string): Error & ProviderFailure {
  const { message, code } = parseErrorBody(text);
  const requestId = res.headers.get(provider.requestIdHeader);
  const retryable = res.status === 408 || res.status >= 500 || (res.status === 429 && !QUOTA_CODES.has(code ?? ''));
  const described = `${provider.name}: HTTP ${res.status}${message === '' ? '' : `: ${message}`}${requestId === null ? '' : ` (request ${requestId})`}`;
  const failure: ProviderFailure = { status: res.status, requestId, retryAfterMs: retryAfterMs(res.headers) };
  const error = Object.assign(retryable ? new Error(described) : new PermanentError(described), failure);
  statusFailures.add(error);
  if ((res.status === 400 || res.status === 413) && CONTEXT_LENGTH.test(text)) contextLengthFailures.add(error);
  return error;
}

/** True for an error from `statusError` whose status is one a provider uses to reject an image. */
export function isImageRejection(error: unknown): boolean {
  return error instanceof Error && 'status' in error && typeof error.status === 'number' && IMAGE_REJECTIONS.has(error.status);
}

/**
 * True for a 400 or 413 that names the model's context length: in the response text, for an error
 * from `statusError`, or in the `code` or `message` of any other error, such as a vendor SDK's.
 */
export function isContextLengthRejection(error: unknown): boolean {
  if (!(error instanceof Error) || !('status' in error) || (error.status !== 400 && error.status !== 413)) return false;
  if (statusFailures.has(error)) return contextLengthFailures.has(error);
  return CONTEXT_LENGTH.test(error.message) || ('code' in error && typeof error.code === 'string' && CONTEXT_LENGTH.test(error.code));
}

/**
 * The provider's own description, from any of the documented body shapes: `error` as a string or
 * `{ message, code, type }`, `message`, or `detail` as a string, `{ message }` or `[{ msg, loc }]`.
 * Anything else falls back to the raw text, truncated.
 */
export function parseErrorBody(text: string): { message: string; code: string | null } {
  let body: unknown;
  try {
    body = JSON.parse(text);
  } catch {
    return { message: text.slice(0, 500), code: null };
  }
  if (!isRecord(body)) return { message: text.slice(0, 500), code: null };
  const error = body.error;
  if (typeof error === 'string') return { message: error, code: null };
  if (isRecord(error)) {
    const code = typeof error.code === 'string' ? error.code : typeof error.type === 'string' ? error.type : null;
    return { message: typeof error.message === 'string' ? error.message : text.slice(0, 500), code };
  }
  if (typeof body.message === 'string') return { message: body.message, code: null };
  const detail = body.detail;
  if (typeof detail === 'string') return { message: detail, code: null };
  if (isRecord(detail) && typeof detail.message === 'string') return { message: detail.message, code: null };
  if (Array.isArray(detail)) {
    const lines = detail.filter(isRecord).map((d) => `${Array.isArray(d.loc) ? `${d.loc.join('.')}: ` : ''}${typeof d.msg === 'string' ? d.msg : JSON.stringify(d)}`);
    if (lines.length > 0) return { message: lines.join('; '), code: null };
  }
  return { message: text.slice(0, 500), code: null };
}

/** `retry-after-ms` first, then `Retry-After` as seconds or an HTTP date; null when absent or unreadable. */
export function retryAfterMs(headers: Headers, now = Date.now()): number | null {
  const ms = headers.get('retry-after-ms');
  if (ms !== null && /^\d+$/.test(ms.trim())) return Number(ms);
  const after = headers.get('retry-after');
  if (after === null) return null;
  if (/^\d+$/.test(after.trim())) return Number(after) * 1000;
  const at = Date.parse(after);
  return Number.isNaN(at) ? null : Math.max(0, at - now);
}
