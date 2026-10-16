import type { DeadLetter, Decision, Event, Window } from '../core/types.js';
import { copyEvent } from '../internal/shared.js';
import { estimateRenderedBytes } from '../core/window.js';

/** Copies and validates an event at the JSON boundary, and measures its rendered bytes. */
export function copyEventSized(event: Event): { event: Event; bytes: number } {
  const copied = copyEvent(event);
  if (typeof copied.id !== 'string' || typeof copied.name !== 'string' || (copied.text !== undefined && typeof copied.text !== 'string')) {
    throw new TypeError('attend: event id, name and text must be strings');
  }
  return { event: copied, bytes: estimateRenderedBytes(copied) };
}

const copying = new Set<Promise<Window>>();

/** Waits for active window copies when advancing a deterministic test clock. */
export async function drainCopies(): Promise<void> {
  while (copying.size > 0) await Promise.all(copying);
}

/** Copies one slice synchronously and yields between any remaining slices. */
export function copyWindowSliced(window: Window, slice = 64): Window | Promise<Window> {
  if (!Number.isSafeInteger(slice) || slice < 1) throw new RangeError('attend: slice must be a positive integer');
  const events: Window['events'] = [];
  let i = 0;
  function copySlice(): Window | Promise<Window> {
    const end = Math.min(i + slice, window.events.length);
    for (; i < end; i += 1) events.push(structuredClone(window.events[i]!));
    if (i < window.events.length) return new Promise<void>((resolve) => setImmediate(resolve)).then(copySlice);
    return { ...window, events };
  }
  const result = copySlice();
  if (result instanceof Promise) {
    copying.add(result);
    void result.then(() => copying.delete(result), () => copying.delete(result));
  }
  return result;
}

/** Copies a decision and slices the copy of its window. */
export function copyDecision(decision: Decision): Decision | Promise<Decision> {
  const copy = { ...decision, probabilities: { ...decision.probabilities }, predicates: { ...decision.predicates } };
  const window = copyWindowSliced(decision.window);
  return window instanceof Promise ? window.then((window) => ({ ...copy, window })) : { ...copy, window };
}

/** Copies a dead letter and both windows it may contain. */
export function copyLetter(letter: DeadLetter): DeadLetter | Promise<DeadLetter> {
  const window = copyWindowSliced(letter.window);
  const decision = letter.decision === null ? null : copyDecision(letter.decision);
  if (window instanceof Promise || decision instanceof Promise) {
    return Promise.all([window, decision]).then(([window, decision]) => ({ ...letter, window, decision }));
  }
  return { ...letter, window, decision };
}

/** Estimates JSON bytes without serializing the record on the memory-store path. */
export function recordBytes(value: unknown): number {
  if (value === null) return 4;
  if (typeof value === 'string') return Buffer.byteLength(value) + 2;
  if (typeof value === 'number' || typeof value === 'boolean') return String(value).length;
  if (Array.isArray(value)) return 2 + value.reduce<number>((sum, item) => sum + recordBytes(item) + 1, 0);
  if (typeof value === 'object') return 2 + Object.entries(value).reduce((sum, [key, item]) => sum + Buffer.byteLength(key) + 4 + recordBytes(item), 0);
  return 0;
}
