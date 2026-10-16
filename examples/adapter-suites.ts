/**
 * The store conformance suite and the crash suite, run against `memoryStore`. Swap in your own
 * adapter's factory. Run with `pnpm build && node --test examples/adapter-suites.ts`.
 */
import { test } from 'node:test';
import { memoryStore } from '@acquiredintelligence/attend';
import { crashSuite, multiScopeCrashSuite, scopeStoreConformance, decisionLogConformance } from '@acquiredintelligence/attend/testing';

for (const c of scopeStoreConformance(() => memoryStore())) test(c.name, c.run);
for (const c of crashSuite(() => memoryStore())) test(c.name, c.run);
for (const c of decisionLogConformance(() => memoryStore())) test(c.name, c.run);
for (const c of multiScopeCrashSuite(() => memoryStore())) test(c.name, c.run);
