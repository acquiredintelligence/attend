/**
 * Deterministic tests for an attend setup with a fake clock, a scripted judge and a recording
 * sink. Run with `pnpm build && node --test examples/testing.ts`.
 */
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createAttend, memoryStore, type DeadLetter, type Event, type Kind } from 'attend';
import { fakeClock, recordingSink, scriptedJudge, type ScriptedAnswer } from 'attend/testing';

const kind = (event: Event): Kind => {
  if (event.name === 'user.message') return 'flush';
  if (event.name === 'build.finished') return 'closing';
  return 'consequence';
};

function setup(script: ScriptedAnswer[]) {
  const clock = fakeClock();
  const judge = scriptedJudge(script);
  const sink = recordingSink();
  const letters: DeadLetter[] = [];
  const attend = createAttend({
    store: memoryStore(),
    kind,
    judge,
    sink,
    clock,
    deadLetter: (letter) => {
      letters.push(letter);
    },
  });
  const controller = new AbortController();
  const running = attend.run({ signal: controller.signal });
  const stop = async () => {
    controller.abort();
    await running;
  };
  return { clock, judge, sink, letters, attend, stop };
}

test('a finished build is judged after the closing grace, with its log lines', async () => {
  const { clock, judge, sink, letters, attend, stop } = setup([{ route: 'queue' }]);
  await attend.push('repo', [
    { id: 'log-1', at: 0, name: 'build.log', data: { line: 'pnpm test' } },
    { id: 'done-1', at: 0, name: 'build.finished', data: { ok: false } },
  ]);
  await clock.advance(499);
  assert.equal(judge.calls.length, 0);
  await clock.advance(1);
  assert.deepEqual(judge.calls[0]?.window.events.map((e) => e.id), ['log-1', 'done-1']);
  assert.deepEqual(sink.delivered.map((d) => d.route), ['queue']);
  assert.deepEqual(letters, []);
  await stop();
});

test('a waiting window merges with the user message and is judged again without wait', async () => {
  const { clock, judge, sink, attend, stop } = setup([{ route: 'wait' }, { route: 'interrupt' }]);
  await attend.push('repo', [{ id: 'done-1', at: 0, name: 'build.finished', data: { ok: false } }]);
  await clock.advance(500);
  assert.equal(sink.delivered.length, 0);
  await attend.push('repo', [{ id: 'msg-1', at: 0, name: 'user.message', data: { text: 'can you look?' } }]);
  await clock.advance(0);
  assert.deepEqual(judge.calls[1]?.routes.map((r) => r.route), ['interrupt', 'queue', 'note', 'ask', 'skip']);
  assert.deepEqual(
    sink.delivered.map((d) => [d.route, d.window.events.map((e) => e.id)]),
    [['interrupt', ['done-1', 'msg-1']]],
  );
  await stop();
});

test('a sink that keeps failing dead-letters the decision after the retry budget', async () => {
  const { clock, sink, letters, attend, stop } = setup([{ route: 'queue' }]);
  sink.failNext(4);
  await attend.push('repo', [{ id: 'done-1', at: 0, name: 'build.finished', data: { ok: false } }]);
  await clock.advance(500 + 1000 + 4000 + 16_000);
  assert.deepEqual(sink.delivered, []);
  assert.deepEqual(letters.map((l) => [l.stage, l.attempts]), [['sink', 4]]);
  await stop();
});
