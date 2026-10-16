import { stableJson } from './json.js';

export class ConformanceError extends Error {}

export function assertEqual(actual: unknown, expected: unknown, message: string): void {
  const a = stableJson(actual);
  const e = stableJson(expected);
  if (a !== e) throw new ConformanceError(`${message}\n  expected: ${e}\n  actual:   ${a}`);
}

export function assertOk(value: unknown, message: string): void {
  if (!value) throw new ConformanceError(message);
}
