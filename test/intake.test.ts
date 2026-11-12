import { expect, test } from 'vitest';
import { BackpressureError, createAttend, memoryStore } from '../src/index.js';
import { openIntake } from '../src/intake.js';
import type { Clock, Input, ScopeStore, SourceBatch } from '../src/types.js';
import { drain } from '../src/internal/clock.js';
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
    await expect(attend.end('s')).rejects.toThrow('intake');
    await expect(attend.flush('s')).rejects.toThrow('intake');
    await expect(attend.attach('s', { key: 'feed', async subscribe() { throw new Error('must not subscribe'); } })).rejects.toThrow('intake');
    expect(() => openIntake(attend, 's')).toThrow('already active');
  } finally {
    await intake.close();
    await attend.stop();
    await running;
  }
});

test('queued abort refunds its reservation without preparing', async () => {
  const attend = createAttend({ clock: fakeClock(), admission: { maxQueuedInputs: 2 }, kind: {}, judge: scriptedJudge([]), sink: 'log' });
  const intake = openIntake(attend, 's');
  expect(() => intake.submit(async () => ({ events: [] }))).toThrow('running');
  const running = attend.run(), started = gate(), release = gate();
  try {
    const first = intake.submit(async () => { started.open(); await release.promise; return { events: [] }; });
    await started.promise;
    const controller = new AbortController();
    let called = false;
    const queued = intake.submit(async () => { called = true; return { events: [], cursor: 'bad' }; }, { source: 'feed', signal: controller.signal });
    controller.abort();
    await expect(queued).rejects.toBeDefined();
    await attend.push('other', { id: 'c', name: 'output' });
    release.open(); await first;
    expect(called).toBe(false);
    expect((await attend.inspect('s'))?.cursors ?? {}).toEqual({});
    expect(() => openIntake(attend, 'other')).toThrow('already active');
  } finally { release.open(); await intake.close(); await attend.stop(); await running; }
});

class ManualClock implements Clock {
  time = 0;
  readonly timers: { at: number; callback: () => void; cancelled: boolean }[] = [];
  now() { return this.time; }
  at(at: number, callback: () => void) {
    const timer = { at, callback, cancelled: false };
    this.timers.push(timer);
    return () => { timer.cancelled = true; };
  }
  fire(at: number) {
    this.time = at;
    for (const timer of this.timers.filter((t) => !t.cancelled && t.at <= at).sort((a, b) => a.at - b.at)) {
      timer.cancelled = true;
      timer.callback();
    }
  }
}

function gatedStore() {
  const inner = memoryStore();
  const gates = new Map<Input['kind'], { entered: ReturnType<typeof gate>; release: ReturnType<typeof gate> }>();
  const store: ScopeStore = { ...inner, async commit(writes) {
    for (const write of writes) for (const logged of write.inputs) {
      const held = gates.get(logged.input.kind);
      if (held) { held.entered.open(); await held.release.promise; }
    }
    return inner.commit(writes);
  } };
  return { store,
    hold(kind: Input['kind']) {
      const held = { entered: gate(), release: gate() };
      gates.set(kind, held);
      return { entered: held.entered.promise, release: held.release.open };
    },
    releaseAll() { for (const held of gates.values()) held.release.open(); },
  };
}

test('a timer owns intake before its commit', async () => {
  const clock = new ManualClock();
  const firstResult = gate(), firstDelivery = gate();
  const calls: { at: number; ids: string[] }[] = [];
  const attend = createAttend({ clock, kind: { move: 'continuous' },
    judge: async (input) => {
      const ids = input.window.events.map((event) => event.id);
      calls.push({ at: clock.now(), ids });
      if (ids[0] === 'a') await firstResult.promise;
      return { route: 'queue' };
    },
    sink: async (decision) => { if (decision.window.events[0]?.id === 'a') await firstDelivery.promise; },
  });
  const intake = openIntake(attend, 's');
  const running = attend.run({ sweepMs: 10 ** 12 });
  const prepared: number[] = [];
  try {
    await intake.submit(async () => batch('a'));
    clock.fire(100);
    expect((await attend.inspect('s'))?.batch).toBeNull();
    const pending = intake.submit(async () => { prepared.push(clock.now()); return batch('b', 100); });
    await drain();
    expect(calls).toEqual([{ at: 100, ids: ['a'] }]);
    clock.fire(150); firstResult.open(); await drain();
    clock.fire(200); await drain();
    expect(prepared).toEqual([]);
    clock.fire(700); firstDelivery.open(); await pending;
    expect(prepared).toEqual([700]);
    expect(calls).toEqual([{ at: 100, ids: ['a'] }, { at: 700, ids: ['b'] }]);
  } finally {
    firstResult.open(); firstDelivery.open();
    await attend.stop(); await running;
  }
});

test.each([false, true])('close and receipt wait for release commit, timer=%s', async (timer) => {
  const clock = fakeClock();
  const held = gatedStore();
  const judge = scriptedJudge([{ route: 'queue' }]);
  const attend = createAttend({ clock, store: held.store, kind: { move: 'continuous' }, judge, sink: 'log' });
  const intake = openIntake(attend, 's');
  const running = attend.run({ sweepMs: 10 ** 12 });
  let acknowledged = false, closed = false;
  try {
    const released = held.hold('judgeReleased');
    let receipt: Promise<void> | undefined;
    let blocked: ReturnType<typeof held.hold>;
    if (timer) {
      await intake.submit(async () => batch('a'));
      blocked = held.hold('tick');
      await clock.advance(100);
    } else {
      blocked = held.hold('events');
      receipt = intake.submit(async () => batch('a', 0, 0));
      void receipt.then(() => { acknowledged = true; });
    }
    await blocked.entered;
    const closing = intake.close().then(() => { closed = true; });
    blocked.release();
    await released.entered;
    await drain();
    expect(closed).toBe(false);
    expect(acknowledged).toBe(false);
    expect(judge.calls).toHaveLength(0);
    released.release();
    await closing; await receipt;
    expect((await attend.inspect('s'))?.batch?.phase.phase).toBe('retrying');
  } finally {
    held.releaseAll(); await attend.stop(); await running;
  }
});

test('deferral commit belongs to the receipt but future retry does not', async () => {
  const held = gatedStore();
  const deferred = held.hold('judgeDeferred');
  const started = gate(), release = gate();
  const attend = createAttend({ clock: fakeClock(), store: held.store, kind: { move: 'continuous' }, judgeConcurrency: 1,
    judge: async () => { started.open(); await release.promise; return { route: 'skip' }; }, sink: 'log' });
  const first = openIntake(attend, 'first'), second = openIntake(attend, 'second');
  const running = attend.run({ sweepMs: 10 ** 12 });
  let done = false;
  try {
    const one = first.submit(async () => batch('a', 0, 0));
    await started.promise;
    const two = second.submit(async () => batch('b', 0, 0)).then(() => { done = true; });
    await deferred.entered; await drain();
    expect(done).toBe(false);
    deferred.release(); await two;
    expect((await attend.inspect('second'))?.batch?.phase.phase).toBe('retrying');
    release.open(); await one;
  } finally {
    held.releaseAll(); release.open(); await attend.stop(); await running;
  }
});

test('a normal result queued before close is fenced until application', async () => {
  const held = gatedStore();
  const delivered = held.hold('delivered');
  const clock = fakeClock();
  const releaseB = gate(), returnedB = gate();
  let deliveries = 0;
  const attend = createAttend({ clock, store: held.store, kind: { move: 'continuous' }, settle: { retryBackoffMs: [1000] },
    judge: async (input) => { if (input.window.events[0]?.id === 'b') { await releaseB.promise; returnedB.open(); } return { route: 'queue' }; },
    sink: async () => { deliveries += 1; if (deliveries === 1) throw new Error('controlled failure'); },
  });
  const intake = openIntake(attend, 's');
  const running = attend.run({ sweepMs: 10 ** 12 });
  try {
    await intake.submit(async () => batch('a', 0, 0));
    await intake.submit(async () => batch('b', 0, 1000));
    await clock.advance(1000);
    await delivered.entered;
    releaseB.open(); await returnedB.promise; await drain();
    const closed = intake.close();
    await clock.advance(10);
    delivered.release(); await closed;
    const record = await attend.inspect('s');
    expect(record?.batch?.phase.phase).toBe('retrying');
    expect(record?.outbox.decisions).toEqual([]);
    expect(record?.recent).toHaveLength(1);
  } finally {
    held.releaseAll(); releaseB.open(); await attend.stop(); await running;
  }
});

test('engine budget reserves preparation and refunds cancellation', async () => {
  const attend = createAttend({ clock: fakeClock(), kind: { move: 'continuous' }, judge: scriptedJudge([{ route: 'queue' }]), sink: 'log', admission: { maxQueuedInputs: 1 } });
  const first = openIntake(attend, 'first'), second = openIntake(attend, 'second');
  const running = attend.run();
  const release = gate(), controller = new AbortController();
  const prepare = async () => { await release.promise; return batch('a'); };
  try {
    const pending = first.submit(prepare, { signal: controller.signal });
    expect(() => second.submit(prepare)).toThrow(BackpressureError);
    await expect(attend.push('ordinary', { id: 'ordinary', name: 'move' })).rejects.toThrow(BackpressureError);
    controller.abort(); await expect(pending).rejects.toBeDefined();
    release.open(); await second.submit(prepare);
    await attend.push('ordinary', { id: 'ordinary', name: 'move' });
  } finally {
    release.open(); await attend.stop(); await running;
  }
});

test.each(['prepare', 'judge', 'sink'])('same-scope reentrancy during %s refuses without deadlock', async (during) => {
  const refused: string[] = [];
  const attempt = () => {
    expect(() => intake.submit(async () => batch('nested'))).toThrow('own callback');
    expect(() => intake.close()).toThrow('own callback');
    refused.push(during);
  };
  const attend = createAttend({ clock: fakeClock(), kind: { move: 'continuous' },
    judge: async () => { if (during === 'judge') attempt(); return { route: 'queue' }; },
    sink: async () => { if (during === 'sink') attempt(); },
  });
  const intake = openIntake(attend, 's');
  const running = attend.run();
  try {
    await intake.submit(async () => { if (during === 'prepare') attempt(); return batch('a', 0, 0); });
    expect(refused).toEqual([during]);
  } finally {
    await attend.stop(); await running;
  }
});

test('preparation failure keeps the cursor and permits another registration', async () => {
  const attend = createAttend({ clock: fakeClock(), kind: { move: 'continuous' }, judge: scriptedJudge([{ route: 'queue' }]), sink: 'log' });
  const intake = openIntake(attend, 's');
  const running = attend.run();
  try {
    await expect(intake.submit(async () => { throw new Error('controlled preparation failure'); }, { source: 'feed' })).rejects.toThrow('controlled preparation');
    expect(await attend.inspect('s')).toBeNull();
    await intake.submit(async () => ({ events: [], cursor: '2' }), { source: 'feed' });
    expect((await attend.inspect('s'))?.cursors).toEqual({ feed: '2' });
  } finally { await attend.stop(); await running; }
});

test('close during load refuses unapplied input', async () => {
  const inner = memoryStore(), entered = gate(), release = gate();
  const store: ScopeStore = { ...inner, async load(scope) { entered.open(); await release.promise; return inner.load(scope); } };
  const attend = createAttend({ store, clock: fakeClock(), kind: { move: 'continuous' }, judge: scriptedJudge([{ route: 'queue' }]), sink: 'log' });
  const intake = openIntake(attend, 's');
  const running = attend.run();
  try {
    const pending = intake.submit(async () => batch('a'));
    await entered.promise;
    const closed = intake.close();
    release.open(); await closed;
    await expect(pending).rejects.toThrow('closed');
    expect(await attend.inspect('s')).toBeNull();
  } finally { release.open(); await attend.stop(); await running; }
});

test('an applied prefix can commit after close', async () => {
  const held = gatedStore(), events = held.hold('events');
  const clock = fakeClock();
  const attend = createAttend({ store: held.store, clock, kind: { move: 'continuous' }, judge: scriptedJudge([{ route: 'queue' }]), sink: 'log' });
  const intake = openIntake(attend, 's');
  const running = attend.run();
  try {
    const pending = intake.submit(async () => batch('a'));
    await events.entered; await clock.advance(10);
    const closed = intake.close();
    await clock.advance(10); events.release();
    await pending; await closed;
    expect((await attend.inspect('s'))?.buffer.entries[0]?.seenAt).toBe(0);
  } finally { held.releaseAll(); await attend.stop(); await running; }
});

test('internal write failure rejects the operation without undoing accepted input', async () => {
  const inner = memoryStore();
  const store: ScopeStore = { ...inner, async commit(writes) {
    if (writes.some((write) => write.inputs.some((logged) => logged.input.kind === 'judged'))) throw new Error('controlled completion failure');
    return inner.commit(writes);
  } };
  const attend = createAttend({ store, clock: fakeClock(), kind: { move: 'continuous' }, judge: scriptedJudge([{ route: 'queue' }]), sink: 'log' });
  const intake = openIntake(attend, 's');
  const running = attend.run();
  try {
    await expect(intake.submit(async () => batch('a', 0, 0))).rejects.toThrow('controlled completion');
    const record = await attend.inspect('s');
    expect(record?.seen).toEqual(['a']); expect(record?.recent).toEqual([]);
  } finally { await attend.stop(); await running; }
});

test('retained-store recreation resumes a native release', async () => {
  const store = memoryStore(), clock = fakeClock(), entered = gate(), release = gate();
  const first = createAttend({ store, clock, kind: { move: 'continuous' }, sink: 'log',
    judge: async () => { entered.open(); await release.promise; return { route: 'queue' }; },
  });
  const intake = openIntake(first, 's');
  const running = first.run();
  const pending = intake.submit(async () => batch('a', 0, 0));
  await entered.promise;
  const closed = intake.close(); release.open();
  await closed; await pending; await first.stop(); await running;
  const judge = scriptedJudge([{ route: 'queue' }]);
  const second = createAttend({ store, clock, kind: { move: 'continuous' }, judge, sink: 'log' });
  openIntake(second, 's');
  const resumed = second.run();
  try {
    await clock.advance(0);
    expect(judge.calls[0]?.window.events.map((event) => event.id)).toEqual(['a']);
    expect((await second.inspect('s'))?.recent[0]?.route).toBe('queue');
  } finally { await second.stop(); await resumed; }
});

test('late preparation cannot admit after withdrawal', async () => {
  const attend = createAttend({ clock: fakeClock(), kind: { move: 'continuous' }, judge: scriptedJudge([{ route: 'queue' }]), sink: 'log' });
  const intake = openIntake(attend, 's'), started = gate(), release = gate(), returned = gate();
  const running = attend.run(), controller = new AbortController();
  try {
    const pending = intake.submit(async () => { started.open(); await release.promise; returned.open(); return batch('late'); }, { signal: controller.signal });
    await started.promise;
    controller.abort(); await expect(pending).rejects.toBeDefined();
    await intake.close(); release.open(); await returned.promise; await drain();
    expect(await attend.inspect('s')).toBeNull();
  } finally { release.open(); await attend.stop(); await running; }
});

test('another owned scope progresses during preparation', async () => {
  const attend = createAttend({ clock: fakeClock(), kind: { move: 'continuous' }, judge: scriptedJudge([{ route: 'queue' }]), sink: 'log' });
  const first = openIntake(attend, 'first'), second = openIntake(attend, 'second'), release = gate();
  const running = attend.run();
  let done = false;
  try {
    const pending = first.submit(async () => { await release.promise; return batch('a'); }).then(() => { done = true; });
    await second.submit(async () => batch('b', 0, 0));
    expect(done).toBe(false);
    expect((await attend.inspect('second'))?.recent[0]?.route).toBe('queue');
    release.open(); await pending;
  } finally { release.open(); await attend.stop(); await running; }
});

test.each([0, -1, NaN, Infinity, 1.5, Number.MAX_SAFE_INTEGER + 1])('capacity %s must be a positive portable integer', (capacity) => {
  const attend = createAttend({ clock: fakeClock(), kind: { move: 'continuous' }, judge: scriptedJudge([{ route: 'queue' }]), sink: 'log' });
  expect(() => openIntake(attend, 's', { capacity })).toThrow('capacity');
});
