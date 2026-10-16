import { apply, newOutput } from '../core/reducer.js';
import { fromRecord, initialState, toRecord } from '../core/state.js';
import { DEFAULT_POLICY, type ApplyConfig, type ScopeRecord, type ScopeState } from '../core/types.js';
import type { ScopeStore, StoredScope } from '../store/types.js';

/** Replays immutable stored data under the snapshot's policy, ignoring effects and appends. */
export function replay(scope: string, stored: StoredScope, modes: Pick<ApplyConfig, 'decisions' | 'deadLetters'>): ScopeState {
  const policy = stored.snapshot?.policy ?? DEFAULT_POLICY;
  const state = stored.snapshot ? fromRecord(stored.snapshot.record, policy) : initialState(scope, policy);
  for (const logged of stored.tail) {
    apply({ policy, ...modes, traces: false }, state, logged, newOutput());
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
