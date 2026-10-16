# Deliver decisions to your agent

A sink delivers decisions to the agent. attend calls `sink.deliver(decision, signal)` at least once per decision, one decision at a time per scope, oldest first. After a crash it can deliver the same decision again with the same id, so make the sink idempotent on `decision.id`, which is unique across scopes. Throw to retry, or throw a `PermanentError` to dead-letter at once.

`routes()` builds a sink from one handler per delivered route. A sink never sees `skip` or `wait`. This excerpt from [`examples/postgres-worker.ts`](../examples/postgres-worker.ts) writes every decision to an inbox table that the agent's runtime reads:

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

`deadLetter` is required. It receives a window that no judge or sink could take, with the stage that gave up, the error message and the attempt count. attend retries a dead letter with capped backoff until the handler succeeds, so write it to durable storage and keep the handler simple. Dead letters travel in their own lane, so a failing handler never holds back decisions; once the lane holds `maxDeadLetters` of them, `push` and a source's `emit` reject with `OutboxFullError` until it drains, and nothing is dropped.
