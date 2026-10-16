import type { EndInput, EventsInput, FlushInput, Input, JudgeDeferredInput, ReleasedInput, ScopeRecord, StepConfig, Stepped, TraceEffect } from './types.js';

export type { StepConfig } from './types.js';
export { deadline, isAct } from './window.js';
export { offeredRoutes, validate } from './reducer.js';
export { windowId } from './ids.js';

/** The pure reducer for tests, evals and replays: `fromRecord`, then `apply`, then `toRecord`. Never mutates `record`. O(W + S) per call. The runtime never calls it. */
export function step(config: StepConfig, record: ScopeRecord, input: EventsInput | FlushInput | EndInput | ReleasedInput | JudgeDeferredInput, now: number): Stepped<TraceEffect>;
export function step(config: StepConfig, record: ScopeRecord, input: Input, now: number): Stepped;
export function step(config: StepConfig, record: ScopeRecord, input: Input, now: number): Stepped {
  throw new Error('attend: step is not implemented');
}

export function initialRecord(scope: string): ScopeRecord {
  throw new Error('attend: initialRecord is not implemented');
}
