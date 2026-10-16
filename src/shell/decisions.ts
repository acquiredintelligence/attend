import type { Clock } from '../core/types.js';
import type { DecisionLog } from '../store/types.js';
import type { DecisionCursorPage, DecisionFilter, DecisionReadOptions } from './types.js';

function encode(epoch: string, position: string | null): string {
  return `v1.${epoch}.${position ?? ''}`;
}

function decode(cursor: string): { epoch: string; position: string | null } {
  const separator = cursor.indexOf('.', 3);
  if (!cursor.startsWith('v1.') || separator <= 3) throw new TypeError('attend: invalid decision cursor');
  return { epoch: cursor.slice(3, separator), position: cursor.slice(separator + 1) || null };
}

/** Streams copied decision rows, paging immediately while the log has more to scan. */
export async function* readDecisions(log: DecisionLog, clock: Clock, filter: DecisionFilter, options: DecisionReadOptions): AsyncIterable<DecisionCursorPage> {
  const { signal, pageSize = 100 } = options;
  if (signal.aborted) return;
  const epoch = await log.epoch();
  const bounds = await log.bounds();
  const cursor = options.cursor == null ? null : decode(options.cursor);
  let after = cursor === null ? bounds.last : cursor.epoch === epoch ? cursor.position : null;
  let truncated = cursor !== null && (cursor.epoch !== epoch || (after !== null && bounds.first !== null && log.compare(after, bounds.first) < 0));
  const sinceAt = options.maxAgeMs === undefined ? undefined : clock.now() - options.maxAgeMs;
  if (cursor !== null && sinceAt !== undefined) {
    const first = await log.read({ after, limit: 1 });
    if (first.rows[0] && first.rows[0].at < sinceAt) truncated = true;
  }
  let initial = true;
  while (!signal.aborted) {
    const page = await log.read({ after, ...filter, ...(sinceAt === undefined ? {} : { sinceAt }), limit: pageSize });
    if (signal.aborted) return;
    const checked = page.checked ?? after;
    if (initial || page.rows.length > 0 || checked !== after) {
      yield { rows: structuredClone(page.rows), cursor: encode(epoch, checked), truncated };
      initial = false;
      truncated = false;
    }
    after = checked;
    if (!page.more && !signal.aborted) await log.wait(after, 30000, signal);
  }
}
