import type { Event, Window } from '../core/types.js';

/** The boundary copy of an event as JSON, with its render-size estimate computed in the same walk. */
export function copyEventSized(event: Event): { event: Event; bytes: number } {
  throw new Error('attend: copyEventSized is not implemented');
}

/** A JSON-shaped copy of a window for the judge and the sink, `slice` entries per macrotask (default 64). */
export function copyWindowSliced(window: Window, slice?: number): Promise<Window> {
  throw new Error('attend: copyWindowSliced is not implemented');
}
