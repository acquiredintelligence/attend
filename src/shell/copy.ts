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

/** Copies a window without monopolizing a macrotask for more than one slice of entries. */
export function copyWindowSliced(window: Window, slice = 64): Promise<Window> {
  const done = copyWindow(window, slice);
  copying.add(done);
  void done.then(() => copying.delete(done), () => copying.delete(done));
  return done;
}

async function copyWindow(window: Window, slice: number): Promise<Window> {
  if (!Number.isSafeInteger(slice) || slice < 1) throw new RangeError('attend: slice must be a positive integer');
  const events: Window['events'] = [];
  for (let i = 0; i < window.events.length; i += 1) {
    events.push(structuredClone(window.events[i]!));
    if ((i + 1) % slice === 0 && i + 1 < window.events.length) await new Promise<void>((resolve) => setImmediate(resolve));
  }
  return { ...window, events };
}

/** Copies a decision and slices the copy of its window. */
export async function copyDecision(decision: Decision): Promise<Decision> {
  return { ...decision, probabilities: { ...decision.probabilities }, predicates: { ...decision.predicates }, window: await copyWindowSliced(decision.window) };
}

/** Copies a dead letter and both windows it may contain. */
export async function copyLetter(letter: DeadLetter): Promise<DeadLetter> {
  return { ...letter, window: await copyWindowSliced(letter.window), decision: letter.decision === null ? null : await copyDecision(letter.decision) };
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
