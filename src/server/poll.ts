import { encodeCursor, resolveStart } from './cursor.js';
import { eventsError, matches, occurrenceFor, parseSubscription, queryFor } from './events.js';
import type { DecisionPage } from '../store/types.js';
import type { JsonObject } from '../core/types.js';
import type { PollResult, Principal, ServerContext } from './types.js';

/** Reads one bounded page, optionally waiting for a matching row. */
export async function pollEvents(context: ServerContext, params: unknown, principal: Principal, signal: AbortSignal): Promise<PollResult> {
  const { filter, cursor, maxAgeMs } = parseSubscription(params, principal);
  const values = params as Record<string, unknown>;
  function integer(key: string, fallback: number, minimum: number): number {
    const value = values[key];
    if (value === undefined) return fallback;
    if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < minimum) throw eventsError('InvalidParams', `Invalid ${key}`, { field: key, reason: `must be a safe integer >= ${minimum}` });
    return value;
  }
  const maxEvents = Math.min(integer('maxEvents', context.options.poll?.defaultMaxEvents ?? 100, 1), context.options.poll?.maxEvents ?? 1000);
  const waitMs = Math.min(integer('waitMs', 0, 0), 30_000);
  const now = Date.now();
  const start = await resolveStart(context.log, cursor, maxAgeMs, now);
  const until = now + waitMs;
  const combined = AbortSignal.any([signal, context.signal]);
  let checked = start.after;
  let page: DecisionPage = { rows: [], checked, more: false };
  let rows = page.rows;
  if (!start.fromNow || waitMs > 0) {
    do {
      page = await context.log.read({ after: checked, ...queryFor(filter), ...(maxAgeMs !== undefined && !start.fromNow ? { sinceAt: now - maxAgeMs } : {}), limit: maxEvents, scanLimit: 4 * maxEvents });
      checked = page.checked ?? checked;
      rows = page.rows.filter((row) => matches(filter, row));
      if (rows.length > 0 || page.more || combined.aborted || Date.now() >= until) break;
      await context.log.wait(checked, until - Date.now(), combined);
    } while (!combined.aborted && Date.now() < until);
  }
  return { resultType: 'complete', events: rows.map((row) => {
    const occurrence = occurrenceFor(row, filter.include, context.options.payload?.maxEventsBytes);
    return { ...occurrence, data: occurrence.data as unknown as JsonObject };
  }), cursor: encodeCursor({ epoch: start.epoch, position: checked ?? '' }), truncated: start.truncated, hasMore: page.more, nextPollMs: context.options.poll?.nextPollMs ?? 1000 };
}
