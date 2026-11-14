# Deliver decisions to your agent

A sink delivers decisions to the agent. attend calls `sink.deliver(decision, signal)` at least once per decision, one decision at a time per scope, oldest first. After a crash it can deliver the same decision again with the same id, so make the sink idempotent on `decision.id`, which is unique across scopes. Throw to retry, or throw a `PermanentError` to dead-letter at once.

For a host-prepared result, return [`Judgment.data`](judges.md#carry-a-result-to-the-sink) from the custom judge and read `decision.data` here. The outbox owns that JSON value through retries and recovery, so the sink does not need a separate pending-result cache. The sink receives a detached copy. The default notice remains based on event lines and ignores this field.

A sink can be one function, `(decision, signal) => …`, that gets every delivered decision. `routes()` builds a sink from one handler per delivered route instead, so a route you forgot fails to typecheck. A sink never sees `skip` or `wait`. This excerpt from [`examples/postgres-worker.ts`](../examples/postgres-worker.ts) writes every decision to an inbox table that the agent's runtime reads:

```ts
async function enqueue(decision: Decision): Promise<void> {
  await pool.query('INSERT INTO agent_inbox (scope, decision_id, route, notice) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING', [
    decision.scope,
    decision.id,
    decision.route,
    defaultNotice(decision),
  ]);
}

const attend = createAttend({
  store: postgresStore(pool),
  kind: (event) => (event.name === 'message.created' ? 'flush' : event.name === 'job.completed' ? 'closing' : 'consequence'),
  judge: async ({ window }) => ({ route: window.reason === 'flush' ? 'interrupt' : 'queue' }),
  sink: routes({ interrupt: enqueue, queue: enqueue, note: enqueue, ask: enqueue }),
  deadLetter: async (letter) => {
    await pool.query('INSERT INTO agent_dead_letters (scope, id, letter) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING', [letter.scope, letter.id, JSON.stringify(letter)]);
  },
});
```

A dead letter is a window that no judge or sink could take, with the stage that gave up, the error message and the attempt count. Without a `deadLetter` handler, attend writes each one to the decision log as a final row, where `attend.decisions()` and MCP subscribers read it. Give `deadLetter` a handler to act on them yourself. attend retries the handler with capped backoff until it succeeds, so write the letter to durable storage and keep the handler simple. Dead letters travel in their own lane, so a failing handler never holds back decisions; once the lane holds `maxDeadLetters` of them, `push` and a source's `emit` reject with `OutboxFullError` until it drains, and nothing is dropped.

## Write your own folded notice

`foldEvents(values, { repeats, continues })` returns consecutive groups with `first`, `last`, `count` and `mode`. It accepts your own typed values. `mode` is `single`, `repeat` or `steps`. In Python, import `fold_events` and `EventFold` from `aqi_attend` and pass the comparisons as keyword arguments.

Both comparisons receive the first value of the run and the next value. A run holds while every value repeats the first, or while every value continues the same act. Once a comparison fails within a run, it stays false until the next run. When both hold, the mode is `repeat`. A run such as 1, 2, 1 has mode `steps`, since its middle value did not repeat the first.

Include the stream's identity in both comparisons when equal values on different streams must stay apart. This example keeps two files separate:

```ts
import { foldEvents } from '@acquiredintelligence/attend';

const updates = [
  { file: 'a.txt', line: 1 },
  { file: 'a.txt', line: 4 },
  { file: 'b.txt', line: 4 },
];
const groups = foldEvents(updates, {
  repeats: (first, next) => first.file === next.file && first.line === next.line,
  continues: (first, next) => first.file === next.file,
});
// The first group covers a.txt, lines 1 to 4, count 2. The second covers b.txt.
```

The function preserves order and does not filter or truncate. Filter values before calling it, then choose how many groups to show and sum omitted groups' `count` for the number of hidden events. Endpoint values are references to the inputs; the function never mutates them. Write comparisons as pure functions. `fold: false`, or Python's `fold=False`, returns one group per value without calling either comparison.

`defaultNotice` uses this same function after filtering context entries. Its existing wording and line-limit behavior are unchanged.

## The notice

`defaultNotice(decision)` is the text for the agent: the window's events oldest first, then one line on the route. An event's line is `Event.text` when it has one, and its name and JSON data otherwise. The agent's own `context` events are left out.

Repeats fold, so a busy window stays short:

- A run of events that read the same becomes one line with its count.
- A run of `continuous` events under one name is the steps of one act. It becomes its first and last step with the count.

```text
job.failed {"job":"sync"} (26 times)
dial.turned {"value":0.1} → {"value":0.8} (14 steps)
can you look?
Attend to this now, even mid-task.
```

Only neighbours fold. Events of another kind with one name and different data stay separate lines, because attend cannot tell a new value for one thing from two different things. Steps of two acts that interleave stay separate too. The notice shows the newest `maxLines` lines (default 20) and counts the events behind the lines it left out.

Folding changes this text and nothing else. `decision.window.events` holds every event, and the judge saw every one. Pass `{ fold: false }` for one line per event, or write your own text from `decision.window.events`. In Python the call is `default_notice(decision, max_lines=20, fold=True)`.
