/**
 * The reducer the shell runs: the in-place core (`./core/step.ts`) behind its pure `step` wrapper.
 * The core's types are wider than ./types.ts: the `end` and `judgeDeferred` inputs, the `size`
 * reason, `Buffer.endedAt` and new traces. The shell's inputs and policy can produce none of them,
 * so these exports are narrowed by a cast to today's signatures until the shell moves onto the core.
 */
import { deadline as coreDeadline, initialRecord as coreInitialRecord, step as coreStep } from './core/step.js';
import type { Buffer, EventsInput, FlushInput, Input, ReleasedInput, ScopeRecord, SettlePolicy, SettleReason, Stepped, TraceEffect } from './types.js';

/** What the reducer needs from the config: the policy. */
export interface StepConfig {
  policy: SettlePolicy;
}

/** Overload: ingest, flush and a release never emit anything but traces. */
export const step = coreStep as {
  (config: StepConfig, record: ScopeRecord, input: EventsInput | FlushInput | ReleasedInput, now: number): Stepped<TraceEffect>;
  (config: StepConfig, record: ScopeRecord, input: Input, now: number): Stepped;
};

export const initialRecord = coreInitialRecord as (scope: string) => ScopeRecord;

/** The earliest instant the open window settles, with its reason, or null while it holds no act and no flush. */
export const deadline = coreDeadline as (policy: SettlePolicy, buffer: Buffer) => { at: number; reason: SettleReason } | null;

export { isAct, offeredRoutes, validate, windowId } from './core/step.js';
