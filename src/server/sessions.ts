export interface Sessions {
  open(): string | null;
  touch(id: string): boolean;
  close(id: string): void;
}

/** attend's session table, with a cap and idle expiry. */
export function createSessions(options: { max: number; idleMs: number }): Sessions {
  throw new Error('attend: createSessions is not implemented');
}
