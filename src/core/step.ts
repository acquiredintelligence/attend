import type { EndInput, EventsInput, FlushInput, Input, JudgeDeferredInput, ReleasedInput, ScopeRecord, StepConfig, Stepped, TraceEffect } from './types.js';
import { fromRecord, toRecord } from './state.js';
import { apply, newOutput } from './reducer.js';

export type { StepConfig } from './types.js';
export { deadline, isAct } from './window.js';
export { offeredRoutes, validate } from './reducer.js';
export { windowId } from './ids.js';

/**
 * The pure reducer for tests, evals and replays: `fromRecord`, then `apply`, then `toRecord`.
 * Never mutates `record`. O(W + S) per call, so a runtime that owns its state calls `apply`.
 */
export function step(config: StepConfig, record: ScopeRecord, input: EventsInput | FlushInput | EndInput | ReleasedInput | JudgeDeferredInput, now: number): Stepped<TraceEffect>;
export function step(config: StepConfig, record: ScopeRecord, input: Input, now: number): Stepped;
export function step(config: StepConfig, record: ScopeRecord, input: Input, now: number): Stepped {
  const state = fromRecord(record, config.policy);
  const out = newOutput();
  apply({ policy: config.policy, decisions: config.decisions ?? 'lane', deadLetters: config.deadLetters ?? 'lane', traces: true }, state, { input, now }, out);
  return { record: toRecord(state), effects: out.effects };
}

/** The empty snapshot at version zero. */
export function initialRecord(scope: string): ScopeRecord {
  return { version: 0, scope, cursors: {}, seen: [], buffer: { entries: [], dropped: 0, gap: false, forcedAt: null },
    batch: null, outbox: { decisions: [], deadLetters: [] }, recent: [], dueAt: null };
}
