import type { OutboxLane } from './types.js';

/** Throw from a judge, a sink or a source to skip retries: the judge or sink dead-letters at once, and `attach` stops. */
export class PermanentError extends Error {
  readonly permanent = true as const;
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = 'PermanentError';
  }
}

/** Duck-typed, so a PermanentError from a second copy of the package still counts. */
export function isPermanent(error: unknown): boolean {
  return typeof error === 'object' && error !== null && 'permanent' in error && error.permanent === true;
}

/**
 * `push` and a source's `emit` reject with this when a scope's outbox lane is at its cap
 * (`maxOutbox` for decisions, `maxDeadLetters` for dead letters). Nothing is dropped: a sink
 * or dead-letter handler that stays down eventually holds intake back until it recovers.
 */
export class OutboxFullError extends Error {
  readonly scope: string;
  readonly lane: OutboxLane;
  readonly size: number;
  constructor(scope: string, lane: OutboxLane, size: number) {
    super(`attend: outbox for scope "${scope}" holds ${size} undelivered ${lane === 'decisions' ? 'decisions' : 'dead letters'}`);
    this.name = 'OutboxFullError';
    this.scope = scope;
    this.lane = lane;
    this.size = size;
  }
}

/**
 * `push` and `flush` reject with this once the compare-and-swap budget is spent against
 * other writers. Safe to retry: every input is idempotent and leases recover in-flight work.
 */
export class ConflictError extends Error {
  readonly scope: string;
  constructor(scope: string, attempts: number) {
    super(`attend: gave up on scope "${scope}" after ${attempts} conflicts`);
    this.name = 'ConflictError';
    this.scope = scope;
  }
}
