import type { Attend } from '../shell/types.js';
import type { ScopeStore } from '../store/types.js';
import type { ServeOptions } from './types.js';

/** What `attend serve` runs: attend.run, eventsServer over store.log, ndjsonIngest, one HTTP listener. */
export function serve(attend: Attend, store: ScopeStore, options: ServeOptions): { url(): string; close(): Promise<void> } {
  throw new Error('attend: serve is not implemented');
}
