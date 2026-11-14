import { AsyncLocalStorage } from 'node:async_hooks';
import type { Attend, Intake, IntakeOptions } from './shell/types.js';
import { openRegisteredIntake, type IntakeOwner } from './shell/intake.js';

export type { Intake, IntakeOptions, Prepare } from './shell/types.js';
export { IntakeError } from './errors.js';

const context = new AsyncLocalStorage<IntakeOwner>();

/** Own local input preparation and immediate work for an untouched scope. */
export function openIntake(attend: Attend, scope: string, options?: IntakeOptions): Intake {
  return openRegisteredIntake(attend, scope, options, context);
}
