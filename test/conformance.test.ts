import { expect, test } from 'vitest';
import { decisionLogConformance, scopeStoreConformance } from '../src/testing/conformance.js';
import type { ScopeStore } from '../src/store/types.js';

function stubStore(durable = false): ScopeStore {
  return {
    durable,
    async load() { throw new Error('attend: load is not implemented'); },
    async commit() { throw new Error('attend: commit is not implemented'); },
    async due() { throw new Error('attend: due is not implemented'); },
    async remove() { throw new Error('attend: remove is not implemented'); },
    log: {
      async epoch() { throw new Error('attend: epoch is not implemented'); },
      async bounds() { throw new Error('attend: bounds is not implemented'); },
      async read() { throw new Error('attend: read is not implemented'); },
      async find() { throw new Error('attend: find is not implemented'); },
      async wait() { throw new Error('attend: wait is not implemented'); },
      async trim() { throw new Error('attend: trim is not implemented'); },
      compare() { throw new Error('attend: compare is not implemented'); },
    },
  };
}

const scopes = scopeStoreConformance(stubStore);
const decisions = decisionLogConformance(stubStore, { concurrent: async () => [stubStore(true), stubStore(true)] });

test('scope store case names match the contract', () => {
  expect(scopes.map((c) => c.name)).toEqual([
    'load of a missing scope is null',
    'a commit at version 0 creates the scope and load returns its inputs in order',
    'the stored version counts committed inputs',
    'a write at a stale version conflicts and changes nothing',
    'a write at version 0 on an existing scope conflicts',
    'N concurrent commits at one version give exactly one ok',
    'a conflicting write in a group leaves the other writes committed, with outcomes in order',
    'a group naming one scope twice rejects and applies nothing',
    'a snapshot replaces the stored one and trims the tail',
    'a snapshot and its tail round-trip with unicode, a NUL, a 250-entry window and a 1,000-id ring',
    'due returns scopes due by now, ascending by due then scope, paged after a key',
    'an omitted due keeps the stored value and a null due clears it',
    'remove forgets the scope, its due value and its version',
    'scopes that are prefixes of each other or hold punctuation stay separate',
  ]);
});

test('decision log case names match the contract', () => {
  expect(decisions.map((c) => c.name)).toEqual([
    'an empty log has no bounds and reads nothing',
    'rows appear only when their write commits',
    'rows read back in commit order with strictly increasing positions',
    'read returns rows strictly after a position and pages by limit',
    'checked reaches the head when a read is exhausted and the last row examined when it stops early',
    'a scoped read examines only that scope',
    'a prefix read matches only scopes that start with the prefix',
    'kinds, routes and sinceAt filter rows',
    'find returns a row by kind and id',
    'an append whose kind and id are already logged is skipped and its write still commits',
    'wait resolves on an append, at the timeout and on abort',
    'trim keeps the newest rows by count or age and never drops the head',
    'the epoch is stable on one instance and new for each non-durable instance',
    'two stores on one database share a durable epoch',
    'a reader never skips a row while two writers commit concurrently',
  ]);
});

for (const c of [...scopes, ...decisions]) {
  test(`${c.name} reaches the store port`, async () => {
    await expect(c.run()).rejects.toThrow(/is not implemented/);
  });
}
