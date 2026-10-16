import type { StateStore } from '../../src/types.js';

/** Freezes `value` and everything reachable from it, entering subtrees that are already frozen, so a nested part frozen only at the top is frozen through. */
export function deepFreeze<T>(value: T): T {
  if (typeof value !== 'object' || value === null) return value;
  Object.freeze(value);
  for (const inner of Object.values(value)) deepFreeze(inner);
  return value;
}

/** A store that freezes every record put into it, so a reducer or shell that mutates a stored record throws here (modules are strict). */
export function freezingStore(inner: StateStore): StateStore {
  return { ...inner, put: (record, expectedVersion) => inner.put(deepFreeze(record), expectedVersion) };
}
