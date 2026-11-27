import { apply, COMPLETION_REVISION, DELIVERY_RETRY_REVISION, newOutput } from '../core/reducer.js';
import { fromRecord, initialState, toRecord } from '../core/state.js';
import { DEFAULT_POLICY, type ApplyConfig, type ScopeRecord, type ScopeState } from '../core/types.js';
import type { ScopeStore, StoredScope } from '../store/types.js';

/** Replays immutable stored data under the snapshot's policy, ignoring effects and appends. */
export function replay(scope: string, stored: StoredScope, modes: Pick<ApplyConfig, 'decisions' | 'deadLetters'>): ScopeState {
  let policy = stored.snapshot?.policy ?? DEFAULT_POLICY;
  if (stored.snapshot && stored.snapshot.reducer < DELIVERY_RETRY_REVISION && Object.hasOwn(policy, 'deliveryRetryBackoffMs')) {
    policy = { ...policy };
    delete policy.deliveryRetryBackoffMs;
  }
  const state = stored.snapshot ? fromRecord(stored.snapshot.record, policy) : initialState(scope, policy);
  // A tail under an older snapshot was logged by an older reducer, where `settles` meant nothing.
  const completion = !stored.snapshot || stored.snapshot.reducer >= COMPLETION_REVISION;
  for (const logged of stored.tail) {
    apply({ policy, decisions: modes.decisions, deadLetters: modes.deadLetters, traces: false, completion }, state, logged, newOutput());
    state.version += 1;
  }
  if (state.version !== stored.version) throw new Error(`attend: invalid stored version for scope "${scope}"`);
  return state;
}

/** Reads the durable record, independently of any live writer's cached state. */
export async function readRecord(store: ScopeStore, scope: string, modes: Pick<ApplyConfig, 'decisions' | 'deadLetters'> = { decisions: 'lane', deadLetters: 'lane' }): Promise<ScopeRecord | null> {
  const stored = await store.load(scope);
  return stored === null ? null : structuredClone(toRecord(replay(scope, stored, modes)));
}
