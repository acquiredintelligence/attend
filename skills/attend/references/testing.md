# Testing

`@acquiredintelligence/attend/testing` makes settling deterministic and runs with any test runner. These examples use `node:test`.

## Set up a scope on a fake clock

```ts
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createAttend, DEFAULT_POLICY, memoryStore, type DeadLetter, type Event, type Kind } from '@acquiredintelligence/attend';
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
  const attend = createAttend({ store: memoryStore(), kind, judge, sink, clock, deadLetter: (letter) => void letters.push(letter) });
  const controller = new AbortController();
  const running = attend.run({ signal: controller.signal });
  const stop = async () => {
    controller.abort();
    await running;
  };
  return { clock, judge, sink, letters, attend, stop };
}

test('a closing event settles exactly at the grace', async () => {
  const { clock, judge, sink, attend, stop } = setup([{ route: 'queue' }]);
  await attend.push('repo', [{ id: 'done-1', at: 0, name: 'build.finished', data: {} }]);
  await clock.advance(DEFAULT_POLICY.closingGraceMs - 1);
  assert.equal(judge.calls.length, 0);
  await clock.advance(1);
  assert.deepEqual(sink.delivered.map((d) => d.route), ['queue']);
  await stop();
});

test('a flush settles at once and is never offered wait', async () => {
  const { clock, judge, attend, stop } = setup([{ route: 'interrupt' }]);
  await attend.push('repo', [{ id: 'msg-1', at: 0, name: 'user.message', data: {} }]);
  await clock.advance(0);
  assert.deepEqual(judge.calls[0]?.routes.map((r) => r.route), ['interrupt', 'queue', 'note', 'ask', 'skip']);
  await stop();
});
```

- Start `run()` before you push. Timers arm only while the runner runs.
- `clock.advance(ms)` fires every timer due on the way, in order, and lets each one's work finish. A flush is due at once, so `advance(0)` fires it.
- Take deadlines from `DEFAULT_POLICY`, or from your own `settle` values, and assert that nothing happens one millisecond early.
- Pass the same clock to `mcpEventsSource` and `logSource` through their `clock` option.

## Script the judge

`scriptedJudge(script)` answers in order, repeats its last answer, and records every `JudgeInput` in `calls`. An answer is one of:

- a judgment, such as `{ route: 'wait' }` or `{ route: 'note', predicates: { asked: 0.9 } }`;
- `{ fail: 'message' }`, which throws a retryable error;
- `{ fail: 'message', permanent: true }`, which throws `PermanentError`;
- `{ hang: true }`, which never answers and rejects when attend aborts it at `judgeTimeoutMs`.

Assert on `calls[i].window.events`, `calls[i].window.reason` and `calls[i].routes`.

## Record the sink

`recordingSink()` collects `delivered` decisions and counts `attempts` per decision id. `failNext(n)` makes the next `n` deliveries throw, and `failNext(n, true)` makes them throw `PermanentError`. A decision gets `1 + retryBackoffMs.length` attempts before it becomes a dead letter.

## Restarts and crashes

`harness(config, options)` runs attend on one scope with a fake clock over a store it can crash at any commit. `config` is the `createAttend` config without `store`, `clock` and `deadLetter`. The harness records dead letters in `h.deadLetters` and traces in `h.traces`.

```ts
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { DEFAULT_POLICY, type Event, type Kind } from '@acquiredintelligence/attend';
import { CrashError, harness, recordingSink, scriptedJudge } from '@acquiredintelligence/attend/testing';

const kind = (event: Event): Kind => (event.name === 'build.finished' ? 'closing' : 'consequence');

test('a decision stranded by a crash is delivered once after the restart', async () => {
  const { deliveryTimeoutMs, retryBackoffMs: [firstRetry = 0] } = DEFAULT_POLICY;
  const sink = recordingSink();
  const h = harness({ judge: scriptedJudge([{ route: 'queue' }]), sink, kind });
  await h.push([{ id: 'done-1', at: 0, name: 'build.finished', data: {} }]);
  h.crashAt(2, 'after');
  await assert.rejects(h.advance(DEFAULT_POLICY.closingGraceMs), CrashError);
  await h.restart();
  assert.equal(sink.delivered.length, 0);
  await h.advance(deliveryTimeoutMs + firstRetry);
  assert.deepEqual(sink.delivered.map((d) => d.route), ['queue']);
  await h.stop();
});
```

- `h.push` and `h.advance` wait until the runner is idle, and `h.flush` also lets the runner's tick settle the flush. All three throw `CrashError` when a scheduled crash strikes.
- `h.crashAt(n, 'before' | 'after')` crashes at the nth commit from now. `before` writes nothing. `after` writes the record but runs none of its effects.
- `h.crash()` kills the runner now. `h.restart()` starts a fresh runner over the same store and lets it recover.
- The judge, the sink and `context` must resolve without real time, or `h.idle()` cannot see them finish.

## Adapter suites

To test your own `ScopeStore` or `EventLog`, register the suites with your runner:

```ts
import { test } from 'node:test';
import { memoryStore } from '@acquiredintelligence/attend';
import { crashSuite, multiScopeCrashSuite, scopeStoreConformance, decisionLogConformance } from '@acquiredintelligence/attend/testing';

for (const c of scopeStoreConformance(() => memoryStore())) test(c.name, c.run);
for (const c of crashSuite(() => memoryStore())) test(c.name, c.run);
for (const c of decisionLogConformance(() => memoryStore())) test(c.name, c.run);
for (const c of multiScopeCrashSuite(() => memoryStore())) test(c.name, c.run);
```

Replace the memory adapters with factories for yours. Each factory call must return a fresh, empty adapter: for a database, use a new table or key prefix per call. Set `canBlock` to whether `read` waits for `blockMs`.
