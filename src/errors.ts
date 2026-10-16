/** Throw from a judge or a sink to skip retries and dead-letter at once. */
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

/** `push` rejects with this when the scope's outbox is at `maxOutbox`. The producer should back off and retry. */
export class OutboxFullError extends Error {
  readonly scope: string;
  readonly size: number;
  constructor(scope: string, size: number) {
    super(`attend: outbox for scope "${scope}" holds ${size} undelivered items`);
    this.name = 'OutboxFullError';
    this.scope = scope;
    this.size = size;
  }
}
