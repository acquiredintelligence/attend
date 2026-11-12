import { expect, test } from 'vitest';
import { createAttend } from '../src/index.js';
import { openIntake } from '../src/intake.js';
import { fakeClock, scriptedJudge } from '../src/testing.js';

function gate() {
  const controller = new AbortController();
  const promise = new Promise<void>((resolve) => controller.signal.addEventListener('abort', () => resolve(), { once: true }));
  return { promise, open: () => controller.abort() };
}

test.each(['close', 'stop'])('%s joins cooperative preparation cleanup', async (via) => {
  const clock = fakeClock(), engine = createAttend({ clock, kind: {}, judge: scriptedJudge([]), sink: 'log' });
  const intake = openIntake(engine, 'scope'), running = engine.run();
  const started = gate(), cleaning = gate(), release = gate(), ended = gate();
  let closed = false, receiptDone = false;
  try {
    const receipt = intake.submit(async signal => {
      started.open();
      try { await new Promise<void>((resolve, reject) => signal.addEventListener('abort', () => reject(signal.reason), { once: true })); }
      finally { cleaning.open(); await release.promise; ended.open(); }
      return { events: [] };
    });
    void receipt.then(() => { receiptDone = true; }, () => { receiptDone = true; });
    await started.promise;
    const closing = (via === 'close' ? intake.close() : engine.stop()).then(() => { closed = true; });
    await cleaning.promise; await clock.advance(0);
    expect(receiptDone).toBe(true);
    expect(closed).toBe(false);
    release.open(); await ended.promise; await closing;
    await expect(receipt).rejects.toThrow('closed');
    expect(await engine.inspect('scope')).toBeNull();
  } finally { release.open(); await engine.stop(); await running; }
});

test('close joins previously withdrawn work and preserves prompt receipt/capacity release', async () => {
  const clock = fakeClock(), engine = createAttend({ clock, kind: {}, judge: scriptedJudge([]), sink: 'log', admission: { maxQueuedInputs: 1 } });
  const intake = openIntake(engine, 'scope'), running = engine.run();
  const started = gate(), cleaning = gate(), release = gate();
  const controller = new AbortController();
  let closed = false;
  try {
    const receipt = intake.submit(async signal => {
      started.open();
      try { await new Promise<void>((resolve, reject) => signal.addEventListener('abort', () => reject(signal.reason), { once: true })); }
      finally { cleaning.open(); await release.promise; }
      return { events: [], cursor: 'late' };
    }, { source: 'feed', signal: controller.signal });
    await started.promise; controller.abort();
    await expect(receipt).rejects.toBeDefined();
    await cleaning.promise;
    await intake.submit(async () => ({ events: [], cursor: 'kept' }), { source: 'feed' });
    const shared = intake.close();
    expect(intake.close()).toBe(shared);
    const closing = shared.then(() => { closed = true; });
    await clock.advance(0); expect(closed).toBe(false);
    release.open(); await closing;
    expect((await engine.inspect('scope'))?.cursors).toEqual({ feed: 'kept' });
  } finally { release.open(); await engine.stop(); await running; }
});

test('close joins a withdrawn callback that later rejects', async () => {
  const clock = fakeClock(), engine = createAttend({ clock, kind: {}, judge: scriptedJudge([]), sink: 'log' });
  const intake = openIntake(engine, 'scope'), running = engine.run();
  const started = gate(), release = gate();
  let closed = false;
  try {
    const receipt = intake.submit(async () => { started.open(); await release.promise; throw new Error('late failure'); });
    await started.promise;
    const closing = intake.close().then(() => { closed = true; });
    await clock.advance(0); expect(closed).toBe(false);
    release.open(); await closing;
    await expect(receipt).rejects.toThrow('closed');
    expect(await engine.inspect('scope')).toBeNull();
  } finally { release.open(); await engine.stop(); await running; }
});
