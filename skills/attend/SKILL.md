---
name: attend
description: Wire the attend TypeScript library into an app so that events from MCP Events, webhooks or an app bus wake an agent only when they should. Use when adding attend to a project, assigning event kinds, choosing a store, choosing or writing a judge, writing a sink, attaching an MCP Events source, testing settle timing with a fake clock, or debugging why an agent woke, stayed silent or got a dead letter.
---

# attend

attend sits between an event stream and an agent. Each **scope** (one conversation, repo, inbox or device) has one persisted record. `kind(event)` gives every event a settle role at ingest. The scope's **window** settles when the act in it is finished. A **judge** picks a **route**, and a **sink** carries the route out. The runner retries failures and persists every step, so a restart loses nothing.

Work through the steps in order. Each step ends on a check. Finish the check before you start the next step.

## 1. Map sources to scopes

List every place events come from and the scope each event belongs to. Pick the feed for each source:

| Source | Feed |
|---|---|
| A webhook handler or an in-process bus | `await attend.push(scope, events)` |
| An MCP Events subscription | `attend.attach(scope, mcpEventsSource(client, { name, arguments }))`. See [references/mcp.md](references/mcp.md). |
| Producers in other processes with a shared log | `attend.attach(scope, logSource(log, scope))` over `sqliteLog`, `postgresLog`, `redisLog` or `memoryLog` |

Build each `Event` as `{ id, at, name, data, text? }`:

- `id` is the dedup key within the scope. Derive it from the source: the webhook delivery id, the MCP `eventId`, the bus message id. A retried delivery must carry the same id.
- `at` is the source's time in ms. Settle timers use attend's clock, not `at`.
- `text` is one plain line for the agent to read. `defaultNotice` prints it, and falls back to `name` plus the JSON of `data`.

**Check.** Every event the app produces has a scope key, a feed and a stable id.

## 2. Assign kinds

Ask these questions of each event name, in order. The first yes decides the kind.

1. Did the agent itself produce it: its reply, its tool call, its own status? Use `context`. It joins the window as context for the judge and never settles one.
2. Is a person addressing the agent and waiting for it: a message to the agent, an @mention of it, a final voice transcript? Use `flush`. The window settles at once, and `wait` is not offered to the judge.
3. Is it one step of something that keeps sending while the user keeps going: typing, scrolling, dragging, a partial transcript, a hand pose? Use `continuous`. The window settles once no continuous event has arrived for `streamGapMs`.
4. Is the user lingering on something without acting: a hover, a focus, a page view? Use `passive`. The window settles `passiveQuietMs` after the last one.
5. Does it end something: the user finishing an act (submit, save, send, merge, close), or a run reaching an outcome the agent should hear about even when nobody acted (a failed build, a finished deploy, a fired alert)? Use `closing`. The window settles `closingGraceMs` after it, so the output it causes can join the window.
6. Otherwise it reports what followed: a log line, progress, a status change, a tool result. Use `consequence`. It joins the open window and never settles one on its own.

The order matters. `context` comes first so the agent's own output never settles a window. `flush` comes before `closing` so a direct message settles at once, not after the grace.

| Event names | Kind |
|---|---|
| `message.created` from the user to the agent, `mention.created`, `transcript.final` | `flush` |
| `transcript.partial` | `continuous` |
| `text.changed`, `cursor.moved`, `scroll.moved`, `hand.moved` | `continuous` |
| `page.viewed`, `item.hovered`, `panel.focused` | `passive` |
| `form.submitted`, `file.saved`, `pr.merged`, `issue.closed`, a reply the user sent to someone else | `closing` |
| `build.failed`, `deploy.finished`, `alert.fired`, `job.completed` | `closing` if the agent should hear it with no user act, else `consequence` |
| `ci.log`, `job.progress`, `status.changed`, `tool.result`, `email.delivered` | `consequence` |
| `agent.message`, `agent.tool_call`, `agent.status` | `context` |

Write `kind` as a pure function from the event to a `Kind` string, most often a lookup table keyed by `name` with `consequence` as the fallback. attend stores the kind on the event at ingest. `closing`, `flush`, `continuous` and `passive` are acts: things that happen in the scope and can call for the agent, whether the user did them or an outside party did.

**Check.** `kind` returns a kind for every event name from step 1, and each scope has at least one event that settles a window: `flush`, `closing`, `continuous` or `passive`. A scope fed only consequences never wakes the agent. [references/kinds.md](references/kinds.md) has the exact settle rules.

## 3. Pick a store

| Situation | Store |
|---|---|
| Tests, demos, a browser app, a short-lived process | `memoryStore()` from `attend` |
| One host with a disk: a single server, a desktop app, an edge node | `sqliteStore(db)` from `attend/sqlite` |
| Several replicas, or Postgres already in the stack | `postgresStore(pool)` from `attend/postgres`, with `migrate(pool)` at deploy |
| Redis already in the stack | `redisStore(send)` from `attend/redis`, with a hash-tagged prefix on Cluster |

**Check.** The store survives the process restarts that the app expects. [references/stores.md](references/stores.md) has the setup code and the caveats for each store.

## 4. Pick or write a judge

A judge is `(input: JudgeInput, signal: AbortSignal) => Promise<Judgment>`, and it returns `{ route, probabilities?, predicates? }`.

| When | Judge |
|---|---|
| A rule decides, such as "a direct message always interrupts" | A plain function |
| The decision needs judgment about what the events mean | `llmJudge` from `attend/llm`, with a `complete` call to your model |
| The app already uses the OpenAI Decisions API | `decisionsJudge` from `attend/decisions` |
| Rules cover some cases and a model covers the rest | A function that answers the rule cases and calls the model judge for the rest |

Set `guidance` to one or two sentences on who the agent is and what its user cares about. Set `criteria` only for routes whose default criterion is wrong for the app. Use `context(scope, signal)` to pass live facts, such as whether the agent is mid-turn.

**Check.** The judge answers only with routes in `input.routes`, stops when `signal` aborts, and throws `PermanentError` on a refusal. [references/judges.md](references/judges.md) has the Claude, OpenAI and AI SDK wiring.

## 5. Write the sink and the dead-letter handler

Build the sink with `routes({ interrupt, queue, note, ask })`. attend decides the route, and each handler carries it out in the host:

- `interrupt`: deliver now, even mid-turn.
- `queue`: deliver when the agent's current turn ends.
- `note`: add context for the next turn. The agent does not respond to it.
- `ask`: show the user a prompt to call the agent in. The agent stays quiet unless the user accepts.

Make every handler idempotent on `decision.id`, which is unique across scopes. attend delivers at least once, and after a crash it redelivers the same decision with the same id. Build the agent's text with `defaultNotice(decision)` or from `decision.window.events`. The judge writes no text.

Write `deadLetter` to durable storage keyed on `letter.id`. attend retries it until it succeeds and never drops it.

**Check.** Running any handler twice with the same decision changes nothing the second time. [references/sinks.md](references/sinks.md) has a full wiring template and the host mapping for each route.

## 6. Run it

```ts
import { createAttend, memoryStore, routes, type Event, type Kind } from 'attend';

const kind = (event: Event): Kind => (event.name === 'message.created' ? 'flush' : 'consequence');

const attend = createAttend({
  store: memoryStore(),
  kind,
  judge: async () => ({ route: 'queue' }),
  sink: routes({ interrupt: async () => {}, queue: async () => {}, note: async () => {}, ask: async () => {} }),
  deadLetter: async () => {},
  guidance: 'A coding agent for the acme/shop repo. Its user cares about failing tests on main.',
  observe: (trace) => console.debug('attend', trace.kind, trace.scope),
});

const controller = new AbortController();
await attend.run({ signal: controller.signal });
```

Replace the stub judge, handlers and dead-letter handler with the ones from steps 4 and 5.

- `run()` judges and delivers. `push`, `attach` and `flush` only persist. Start `run()` in at least one long-lived process. Several processes may run it over one shared store.
- Give every process that feeds a scope the same `kind`. The process that ingests an event is the one that kinds it.
- A scope can take several sources; the record keeps one cursor per `Source.key`. Attach a given source to a scope from one process only, and never twice in one process: the second `attach` throws.
- `observe` receives every trace: `settled`, `judged`, `waiting`, `retrying`, `deadLettered` and more. Log them while you wire attend in.

**Check.** An end-to-end run on real events delivers at least one decision to the sink, and the traces show `settled`, `judging`, `judged` and `delivered` for it.

## 7. Test with a fake clock

Inject `fakeClock()` from `attend/testing` as `clock`, answer with `scriptedJudge([...])`, and assert on `recordingSink()`. Advance the clock by the exact deadline, such as `DEFAULT_POLICY.closingGraceMs`, and assert that nothing happens one millisecond before it.

**Check.** The tests cover every kind the app uses, one `wait` that merges with the next window, and one judge or sink failure that ends in a dead letter. [references/testing.md](references/testing.md) has the setup and the crash harness.

## Pitfalls by symptom

Turn on `observe` first. Its traces name each step a window took. Then match the symptom and check its causes in order.

| Symptom | Causes |
|---|---|
| The agent never hears anything | No process runs `run()`. The scope gets only `consequence` and `context` events. The producer that pushed the events uses a different `kind`. |
| The agent hears the same thing twice | The sink is not idempotent on `decision.id`. The source sends a new event id when it retries. |
| A direct message reaches the agent late | `kind` maps it to `closing` or `consequence` instead of `flush`. The judge chose `queue`. A judge call was in flight, and the message waited for the next window. |
| Dead letters with the message `route not offered: wait` | The judge answered `wait` when `input.routes` did not offer it: after a flush, after `attend.flush(scope)`, after `maxWaits` waits, or at the wait horizon. |
| Dead letters with `attend: judge timed out` or `judge lease expired` | The judge ignores `signal` or takes longer than `judgeTimeoutMs`. |
| Dead letters with `attend: delivery timed out` or `delivery lease expired` | The sink ignores `signal` or takes longer than `deliveryTimeoutMs`. |
| `push` rejects with `OutboxFullError` | Its `lane` says which handler is failing: `decisions` means the sink, and the scope holds `maxOutbox` undelivered decisions; `deadLetters` means the dead-letter handler, and it holds `maxDeadLetters`. Fix the handler, then retry the push. `attach` backs off on its own. |
| `push` or `flush` rejects with `ConflictError` | Eight compare-and-swaps in a row lost to other writers on one scope. Retry; every input is idempotent. If it persists, too many producers write one scope directly: put a log in front of it and `attach` `logSource`. |
| `attach` rejects with `attend/mcp: the Client closed` | The MCP Client's transport closed. Reconnect the Client, or make a new one, and attach again. |
| `attach` throws `already attached to scope` | The process attached the same `Source.key` to the same scope twice. Attach each source once per scope per process. |
| `onError` reports `unrouted` | The server sends notifications under subscription ids attend did not open, or the app replaced the `notifications/events/*` handlers attend installed on the Client. |
| `onError` reports `idle` or `overflow` | The stream went quiet for `idleTimeoutMs`, or more than `maxBuffered` notifications waited on a slow store. attend reopened from the committed cursor; nothing is lost when the server can replay from a cursor. |
| `run()` rejects with `attend: already running` | The process called `run()` twice on one instance. Create one `Attend` per process and share it. |
| A test hangs or settles at the wrong time | `run()` started after the push, or `mcpEventsSource` or `logSource` runs on the real clock. Pass the fake clock as their `clock` option. |
| Redis Cluster replies `CROSSSLOT` | The prefix has no hash tag. Use one such as `attend:{app}:`. |
| Postgres reports that `attend_records` does not exist | `migrate()` has not run against this database and schema. |
