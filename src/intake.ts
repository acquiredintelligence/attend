import type { Attend, SourceBatch } from './types.js';

export type Prepare = (signal: AbortSignal) => Promise<SourceBatch>;

export interface IntakeOptions {
  capacity?: number;
}

export interface Intake {
  submit(prepare: Prepare, options?: { source?: string; signal?: AbortSignal }): Promise<void>;
  close(): Promise<void>;
}

/** Own local input preparation and immediate work for an untouched scope. */
export function openIntake(_attend: Attend, _scope: string, _options?: IntakeOptions): Intake {
  throw new Error('not implemented');
}
