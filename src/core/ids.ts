import type { FnvState } from './types.js';

/** FNV-1a 64 over `len:part` for the scope and each id, as 16 hex characters. Identical output to today's `windowId`. */
export function windowId(scope: string, ids: readonly string[]): string {
  throw new Error('attend: windowId is not implemented');
}

export function fnvStart(scope: string): FnvState {
  throw new Error('attend: fnvStart is not implemented');
}

/** Folds `len:id` (UTF-8 byte length, a colon, the UTF-8 bytes) into `state` in place. */
export function foldId(state: FnvState, id: string): void {
  throw new Error('attend: foldId is not implemented');
}

export function fnvHex(state: FnvState): string {
  throw new Error('attend: fnvHex is not implemented');
}
