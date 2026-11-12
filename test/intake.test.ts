import { expect, test } from 'vitest';
import { createAttend } from '../src/index.js';
import { openIntake } from '../src/intake.js';
import type { SourceBatch } from '../src/types.js';
import { fakeClock, scriptedJudge } from '../src/testing.js';

const batch = (id: string, at = 0, quietForMs = 100): SourceBatch => ({ events: [{ id, at, observedAt: at, quietForMs, name: 'move', data: {} }] });

function gate() {
  const controller = new AbortController();
  const promise = new Promise<void>((resolve) => controller.signal.addEventListener('abort', () => resolve(), { once: true }));
  return { promise, open: () => controller.abort() };
}

test('preparation owns the timer and registered backlog', async () => {
  const clock = fakeClock();
  const judge = scriptedJudge([{ route: 'queue' }]);
  const attend = createAttend({ clock, kind: { move: 'continuous' }, judge, sink: 'log' });
  const intake = openIntake(attend, 's');
  const running = attend.run();
  try {
    await intake.submit(async () => batch('a'));
    await clock.advance(90);
    const release = gate();
    const filtering = intake.submit(async () => { await release.promise; return { events: [], cursor: '1' }; }, { source: 'feed' });
    await clock.advance(5);
    const queued = intake.submit(async () => batch('b', 95, 300));
    await clock.advance(195);
    expect(judge.calls).toHaveLength(0);
    release.open();
    await filtering;
    await queued;
    expect(judge.calls).toHaveLength(0);
    await clock.advance(105);
    expect(judge.calls.map((call) => call.window.settledAt)).toEqual([395]);
    expect(judge.calls[0]?.window.events.map((event) => event.id)).toEqual(['a', 'b']);
    expect((await attend.inspect('s'))?.cursors).toEqual({ feed: '1' });
  } finally {
    await intake.close();
    await attend.stop();
    await running;
  }
});

test('raw arrivals wait for judgment and delivery', async () => {
  const clock = fakeClock();
  const started = gate(), release = gate(), sinkStarted = gate(), sinkRelease = gate();
  const offers: { at: number; ids: string[] }[] = [];
  const attend = createAttend({ clock, kind: { move: 'continuous' },
    judge: async (input) => {
      offers.push({ at: clock.now(), ids: input.window.events.map((event) => event.id) });
      if (offers.length === 1) { started.open(); await release.promise; }
      return { route: 'queue' };
    },
    sink: async (decision) => { if (decision.window.events[0]?.id === 'a') { sinkStarted.open(); await sinkRelease.promise; } },
  });
  const intake = openIntake(attend, 's');
  const running = attend.run();
  const prepared: string[] = [];
  try {
    await intake.submit(async () => batch('a'));
    await clock.advance(100);
    await started.promise;
    await clock.advance(50);
    const pending = intake.submit(async () => { prepared.push('b'); return batch('b', 150); });
    release.open();
    await clock.advance(0);
    await sinkStarted.promise;
    await clock.advance(550);
    expect(prepared).toEqual([]);
    sinkRelease.open();
    await pending;
    expect(offers).toEqual([{ at: 100, ids: ['a'] }, { at: 700, ids: ['b'] }]);
  } finally {
    release.open(); sinkRelease.open();
    await intake.close();
    await attend.stop();
    await running;
  }
});

test('abort before callback and owned-scope refusal', async () => {
  const attend = createAttend({ clock: fakeClock(), kind: { move: 'continuous' }, judge: scriptedJudge([{ route: 'queue' }]), sink: 'log' });
  const intake = openIntake(attend, 's');
  const running = attend.run();
  let called = false;
  try {
    const controller = new AbortController();
    const pending = intake.submit(async () => { called = true; return batch('a'); }, { signal: controller.signal });
    controller.abort();
    await expect(pending).rejects.toBeDefined();
    expect(called).toBe(false);
    expect(await attend.inspect('s')).toBeNull();
    await expect(attend.push('s', { id: 'other', name: 'move' })).rejects.toThrow('intake');
  } finally {
    await intake.close();
    await attend.stop();
    await running;
  }
});
