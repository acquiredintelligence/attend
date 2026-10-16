import { expect } from 'vitest';
import { PermanentError } from '../../src/errors.js';
import { harness, recordingSink, scriptedJudge, type ConformanceCase, type HarnessConfig } from '../../src/testing.js';
import type { Event, JsonObject, StateStore } from '../../src/types.js';

const ID_A = '3ecbd4f2ef91c78f';

const close = (id: string, data: JsonObject): Event => ({ id, at: 0, name: 'close', data });

/** A runner over `store` whose waits see the store's real I/O, started at 1000 on scope `s`. */
function start(store: StateStore, config: Partial<HarnessConfig>) {
  const sink = recordingSink();
  const h = harness({ judge: scriptedJudge([{ route: 'note' }]), sink, kind: () => 'closing', ...config }, { store, scope: 's', start: 1000 });
  return { h, sink, inspect: () => h.attend.inspect('s') };
}

/**
 * No memory the caller can reach is shared with the stored record: events are copied at `push`,
 * `inspect` returns a copy, and the judge, the sink and the dead-letter handler each get their
 * own copy. Every store passes the same cases, so user code behaves the same whichever it runs on.
 */
export function aliasingCases(make: () => Promise<StateStore> | StateStore): ConformanceCase[] {
  const cases: [string, (store: StateStore) => Promise<void>][] = [
    [
      'a producer that reuses one data object across pushes stores each push as it was',
      async (store) => {
        const { h, inspect } = start(store, {});
        const pos = { x: 0 };
        for (let i = 0; i < 3; i += 1) {
          pos.x = i;
          await h.push([close(`e${i}`, pos)]);
        }
        expect((await inspect())?.buffer.entries.map((e) => e.data)).toEqual([{ x: 0 }, { x: 1 }, { x: 2 }]);
        await h.stop();
      },
    ],
    [
      'the stored record does not follow a change to the pushed event after push resolves',
      async (store) => {
        const { h, inspect } = start(store, {});
        const data = { user: { name: 'ann' }, tags: ['a'] };
        await h.push([close('a', data)]);
        data.user.name = 'mallory';
        data.tags.push('b');
        expect((await inspect())?.buffer.entries[0]?.data).toEqual({ user: { name: 'ann' }, tags: ['a'] });
        await h.stop();
      },
    ],
    [
      'the stored record does not follow a change to what inspect returned',
      async (store) => {
        const { h, inspect } = start(store, {});
        await h.push([close('a', Object.freeze({ user: { name: 'ann' } }))]);
        const seen = await inspect();
        const entry = seen?.buffer.entries[0];
        if (!seen || !entry) throw new Error('nothing stored');
        const user = entry.data['user'];
        if (typeof user !== 'object' || user === null || Array.isArray(user)) throw new Error('user is not an object');
        user['name'] = 'mallory';
        seen.buffer.gap = true;
        seen.seen.push('zzz');
        expect(await inspect()).toMatchObject({ seen: ['a'], buffer: { gap: false, entries: [{ id: 'a', data: { user: { name: 'ann' } } }] } });
        await h.stop();
      },
    ],
    [
      'a judge that changes its input is answered once and the stored record keeps what it had',
      async (store) => {
        let calls = 0;
        const { h, sink, inspect } = start(store, {
          judge: async (input) => {
            calls += 1;
            input.recent.push({ id: 'ghost', route: 'skip', at: 0 });
            const entry = input.window.events[0];
            if (entry) entry.data['handled'] = true;
            return { route: 'note' };
          },
        });
        await h.push([close('a', {})]);
        await h.advance(500);
        expect(calls).toBe(1);
        expect(h.traces.filter((t) => t.kind === 'retrying' || t.kind === 'deadLettered')).toEqual([]);
        expect(sink.delivered.map((d) => [d.id, d.window.events[0]?.data])).toEqual([[ID_A, {}]]);
        expect((await inspect())?.recent).toEqual([{ id: ID_A, route: 'note', at: 1500 }]);
        await h.stop();
      },
    ],
    [
      'a sink that changes its decision delivers once and the stored record keeps what it had',
      async (store) => {
        let stored: JsonObject | undefined;
        const { h, inspect } = start(store, {
          sink: {
            async deliver(decision) {
              const entry = decision.window.events[0];
              if (entry) entry.data['handled'] = true;
              decision.window.events.length = 0;
              const item = (await inspect())?.outbox.decisions[0]?.item;
              if (item?.kind === 'decision') stored = item.decision.window.events[0]?.data;
            },
          },
        });
        await h.push([close('a', {})]);
        await h.advance(500);
        expect(stored).toEqual({});
        expect(h.traces.filter((t) => t.kind === 'delivered' || t.kind === 'retrying' || t.kind === 'deadLettered')).toEqual([{ kind: 'delivered', scope: 's', deliveryId: ID_A }]);
        expect((await inspect())?.outbox).toEqual({ decisions: [], deadLetters: [] });
        await h.stop();
      },
    ],
    [
      'a dead-letter handler that changes its letter is called once and the stored record keeps what it had',
      async (store) => {
        let stored: string | undefined;
        let calls = 0;
        const { h, inspect } = start(store, {
          judge: async () => {
            throw new PermanentError('refused');
          },
          deadLetter: async (letter) => {
            calls += 1;
            letter.message = 'changed';
            const entry = letter.window.events[0];
            if (entry) entry.data['handled'] = true;
            const item = (await inspect())?.outbox.deadLetters[0]?.item;
            if (item?.kind === 'deadLetter') stored = item.letter.message;
          },
        });
        await h.push([close('a', {})]);
        await h.advance(500);
        expect(calls).toBe(1);
        expect(stored).toBe('refused');
        expect(h.traces.filter((t) => t.kind === 'retrying')).toEqual([]);
        expect((await inspect())?.outbox).toEqual({ decisions: [], deadLetters: [] });
        await h.stop();
      },
    ],
  ];
  return cases.map(([name, run]) => ({ name, run: async () => run(await make()) }));
}
