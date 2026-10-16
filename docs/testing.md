# Test with a fake clock

`@acquiredintelligence/attend/testing` makes settling deterministic. Inject `fakeClock()`, answer with `scriptedJudge`, and assert on `recordingSink`. This excerpt is from [`examples/testing.ts`](../examples/testing.ts), which runs with `node --test`:

```ts
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createAttend, memoryStore, type DeadLetter, type Event, type Kind } from '@acquiredintelligence/attend';
import { fakeClock, recordingSink, scriptedJudge, type ScriptedAnswer } from '@acquiredintelligence/attend/testing';

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
```

- `clock.advance(ms)` fires every timer due on the way and lets each one's work finish before it returns.
- `scriptedJudge` answers in order, repeats its last answer, and records every input in `calls`. An answer can also be `{ fail: 'message' }`, `{ fail: 'message', permanent: true }` or `{ hang: true }`.
- `recordingSink` records `delivered` and `attempts`. `failNext(n)` makes the next `n` deliveries throw.
- Pass the same clock to `mcpEventsSource` and `logSource` through their `clock` option, so their polling runs on fake time too.
- `harness` runs attend on a fake clock over a store that it can crash at any commit. Use it for restart tests.
