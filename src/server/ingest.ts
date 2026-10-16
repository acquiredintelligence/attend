import type { Attend } from '../shell/types.js';
import type { HttpRoute, NdjsonIngestOptions } from './types.js';

export function ndjsonIngest(attend: Attend, options?: NdjsonIngestOptions): HttpRoute {
  throw new Error('attend: ndjsonIngest is not implemented');
}
