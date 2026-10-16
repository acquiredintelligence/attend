# Write a store adapter

A store implements `ScopeStore` (see [`src/store/types.ts`](../src/store/types.ts), where each member's guarantee is in its doc comment):

- `load(scope)` returns the latest snapshot and the inputs committed after it, or `null`.
- `commit(writes)` applies each scope's write only if its stored version equals `expectedVersion` (0 means no stored scope), and returns `'ok'` or `'conflict'` for each. The inputs, the snapshot, the due value and the decision rows of one write land together or not at all. Of N concurrent writes at one version, exactly one is `'ok'`.
- `due(now, limit, after?)` pages the scopes whose due value has passed. It reads and claims nothing.
- `remove(scope)` forgets the scope.
- `log` is the `DecisionLog`: the epoch, bounds, reads by position, `find`, `wait` and `trim`.

Run the conformance suites and the crash suites against your adapter with any test runner. Each returns named async cases. [`examples/adapter-suites.ts`](../examples/adapter-suites.ts) runs them against `memoryStore` with `node:test`:

```ts
import { test } from 'node:test';
import { memoryStore } from '@acquiredintelligence/attend';
import { crashSuite, multiScopeCrashSuite, scopeStoreConformance, decisionLogConformance } from '@acquiredintelligence/attend/testing';

for (const c of scopeStoreConformance(() => memoryStore())) test(c.name, c.run);
for (const c of crashSuite(() => memoryStore())) test(c.name, c.run);
for (const c of decisionLogConformance(() => memoryStore())) test(c.name, c.run);
for (const c of multiScopeCrashSuite(() => memoryStore())) test(c.name, c.run);
```

The factory must return a fresh, empty store for each case. For a database, give each case its own table prefix or key prefix. An `EventLog` adapter runs `logConformance(make, { canBlock })`, where `canBlock` says whether `read` waits for `blockMs`.
