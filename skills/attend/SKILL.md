---
name: attend
description: Wire the attend TypeScript library into an app so that its events wake an agent only when they should. Use when adding attend to a project, deciding which app events should reach an agent and with what reaction (interrupt, queue, note, ask, wait, skip), writing the one small attention module, choosing a store or a judge, running attend serve, tuning the act threshold from a replay, or debugging why an agent woke, stayed silent or got a dead letter.
---

# attend

attend sits between an app's firehose of events and an agent. Each **scope** (one conversation, repo, inbox or device) collects its events into a **window**. The window settles when the act in it is finished, a **judge** picks a **route** (`interrupt`, `queue`, `note`, `ask`, `skip` or `wait`), and the app carries the route out. Everything is persisted, so a restart loses nothing.

Your job is to wire this into the user's app in four steps, with a tiny footprint:

1. Identify the events the app emits and could pipe out.
2. Pick which of them are useful signals.
3. Decide which deserve which reaction.
4. Wire it end to end.

## How to behave

- **Discover first.** Read the code for everything you can answer yourself: the event sources, the agent runtime, the language and the conventions, which store the app already has, which API keys are in its environment (check names, never print values).
- **Stay thin.** The integration is **one small module** in the codebase's own style and location, plus **a few call sites** at event chokepoints the app already has (its bus, its webhook handler, its analytics wrapper, its store dispatch). Add no new layer, framework or directory tree, and do not scatter edits. If the app has a place for such modules, use it.
- **Tell, then ask once.** Before you edit anything, give the user the FYI below. It is an explanation, not a question. Then ask **one batch** of simple questions, each with a recommended default, in a single message. Never ask a series. If the user says "go" or nothing contradicts a default, take the defaults.
- **Never print or write an API key.** Read keys from the environment.

## 1. Identify the events

Find every place the app already emits something an agent could react to, and note the scope each belongs to (the conversation, repo, inbox or device it is about):

| Look in | For |
|---|---|
| Analytics and telemetry calls (`track`, `capture`, `log.event`) | Named user actions |
| WebSocket and SSE handlers, message buses, queues | Messages, presence, progress |
| Store actions and reducers, DOM and UI handlers | State changes, clicks, edits |
| Webhook handlers and CI or deploy hooks | Builds, deploys, alerts, PRs |
| Logs and job runners | Failures, completions |
| MCP servers the app already talks to | Upstream events (`attend.attach(scope, mcpEventsSource(...))`, see [references/mcp.md](references/mcp.md)) |

For each event decide its `id` (derive it from the source: the delivery id, the message id; a retry must carry the same id), its `at` (the source's time in ms), and `text` (one plain line the agent can read). Note anything that must be redacted before it leaves the process.

**Check.** You have a list of event names, where each is emitted, its scope key and a stable id.

## 2. Pick the signals

Most events are noise. Keep an event only if an agent could do something useful with it, alone or with the events around it. Drop high-rate chatter (cursor moves, heartbeats) unless the agent should react to the user lingering; keep it as `continuous` or `passive` if so. Prefer fewer, richer events. The agent's own output is worth piping in as `context`, so the judge sees what the agent already said.

**Check.** The list is short enough to read in one glance, and every kept event has a reason.

## 3. Decide the reactions

Give each kept event a **kind**, which tells attend when the window settles. Ask these in order; the first yes decides.

1. The agent itself produced it (a reply, a tool call, a status)? `context`. It never settles a window.
2. A person is addressing the agent and waiting (a message to it, an @mention, a final transcript)? `flush`. The window settles at once and `wait` is not offered.
3. One step of something the user keeps doing (typing, scrolling, dragging, a partial transcript)? `continuous`. Settles after `streamGapMs` of quiet.
4. The user lingering without acting (hover, focus, page view)? `passive`. Settles `passiveQuietMs` after the last one.
5. It ends something, or a run reached an outcome the agent should hear even when nobody acted (submit, save, merge, failed build, fired alert)? `closing`. Settles `closingGraceMs` later so its consequences can join.
6. Otherwise it reports what followed (log line, progress, tool result)? `consequence`. It joins the open window and never settles one.

Then map each **route** to what the agent runtime can really do. The judge picks the route; the app decides how to run it:

| Route | The agent | Typical mapping |
|---|---|---|
| `interrupt` | Hears it now, even mid-turn | Steer or cancel the current turn with the notice |
| `queue` | Hears it when the current turn ends | Enqueue as the next user-side message |
| `note` | Gets it as context, does not respond | Append to the next turn's context |
| `ask` | Stays quiet; the user is offered a prompt | Show a banner or button that calls the agent in |
| `skip`, `wait` | Nothing yet | No handler; attend keeps them |

Write down, in a sentence each, when you expect `interrupt` and when you expect silence in this app. That goes into the judge's `guidance`. [references/kinds.md](references/kinds.md) has the exact settle rules and [references/sinks.md](references/sinks.md) the host mapping.

**Check.** Every kept event has a kind, each scope has at least one event that settles a window (`flush`, `closing`, `continuous` or `passive`; a scope fed only consequences never wakes the agent), and every route has a handler or a reason not to.

## The FYI, then the question batch

After steps 1 to 3 and before you edit anything, send the user this, in your own words and with their real names filled in. Keep it to a few short lines:

> **How attend will work in your app**
> - **Events:** `<event names>` from `<files or sources>`, one scope per `<conversation/repo/...>`.
> - **Settling:** attend groups them into a window per scope and settles it when `<the closing event / a user message>` arrives, or after a quiet gap, so the agent hears one notice per episode, not one per event.
> - **Judge:** `<Jev / the Decisions API / your LLM / a rule>` reads each settled window and picks a route.
> - **Reactions:** interrupt `<does X>`, queue `<does Y>`, note `<does Z>`, ask `<does W>`.
> - **Where:** one new file, `<path>`, and `<N>` call sites in `<files>`.

Then, in the same message, one batch (keep only the questions you could not answer from the code):

> **Quick questions (reply "go" to accept the defaults):**
> 1. Judge: `<Jev, with your TypeSafe key in TYPESAFE_API_KEY>` (recommended: fast, built for this) or `<your own LLM>` or `<a rule>`?
> 2. Run it inside the app (recommended: real time) or as `attend serve` beside it?
> 3. How many interruptions an hour are acceptable? (default: 6)
> 4. Store: `<memory>` for now or `<the database you already have>`? (default: `<what the app has>`)
> 5. May I replay a sample of your recorded events to tune the threshold? (default: yes, from `<where>`)

## 4. Wire it end to end

### The one module

Put it where the app keeps its integrations, named for what it is. It owns the kinds table, the judge, the sink and the run loop, and exports two things the call sites use: `hear` and `startAttention`.

```ts
import { createAttend, defaultNotice, memoryStore, routes, type Decision, type Event, type Kind } from 'attend';
import { jevJudge } from 'attend/jev';

declare const agent: {
  steer(scope: string, text: string): Promise<void>;
  enqueue(scope: string, text: string): Promise<void>;
  addContext(scope: string, text: string): Promise<void>;
  offer(scope: string, text: string): Promise<void>;
};

const KIND_BY_NAME: Record<string, Kind> = {
  'message.created': 'flush',
  'build.failed': 'closing',
  'deploy.finished': 'closing',
  'agent.reply': 'context',
};
const kind = (event: Event): Kind => KIND_BY_NAME[event.name] ?? 'consequence';

const deliver = (act: (scope: string, text: string) => Promise<void>) => (decision: Decision) => act(decision.scope, defaultNotice(decision));

const attend = createAttend({
  store: memoryStore(),
  kind,
  judge: jevJudge({ apiKey: process.env.TYPESAFE_API_KEY ?? '' }),
  sink: routes({ interrupt: deliver(agent.steer), queue: deliver(agent.enqueue), note: deliver(agent.addContext), ask: deliver(agent.offer) }),
  deadLetter: (letter) => console.error('attend dead letter', letter.id, letter.message),
  guidance: 'A coding agent for the acme/shop repo. Its user cares about failing builds on main and about direct questions.',
});

export const hear = (scope: string, event: Event) => attend.push(scope, [event]);
export const startAttention = (signal: AbortSignal) => attend.run({ signal });
```

Adapt it to the codebase: its language idioms, its logger, its config for the key, its real agent calls. Make each handler idempotent on `decision.id` (attend delivers at least once and redelivers the same decision after a crash). Pass `deadLetter` something durable if the app has storage. The judge writes no text: the notice comes from `defaultNotice(decision)` or from `decision.window.events`.

### The call sites

At each chokepoint from step 1, add one line that hands the existing event to `hear` with the scope and the stable id. Do not change what the chokepoint does. Start `startAttention(signal)` once where the app boots, and abort the signal where it shuts down.

### Choose the judge and the store

| Situation | Choice |
|---|---|
| The user has a TypeSafe key | `jevJudge` from `attend/jev`. A fast classifier with real probabilities. The speed default for real time. |
| The app uses the OpenAI Decisions API | `decisionsJudge` from `attend/decisions` |
| The app has an LLM client and the user prefers it | `llmJudge` from `attend/llm` with a `complete` call; slower, see [references/judges.md](references/judges.md) |
| A rule decides ("a direct message always interrupts") | A plain function |
| Tests, a demo, a short-lived process | `memoryStore()` |
| One host with a disk | `sqliteStore(db)` from `attend/sqlite` |
| Several replicas, or Postgres already there | `postgresStore(pool)` from `attend/postgres` after `migrate(pool)` |
| Redis already there | `redisStore(send)` from `attend/redis` |

[references/stores.md](references/stores.md) has the setup for each store. If the app is not TypeScript, or the agent is another process, run `npx attend serve` beside it instead, post NDJSON to `/v1/ingest` from the existing chokepoints, and read decisions over MCP Events (`events/stream`, `events/poll` or the `attend_wait` tool). See [references/mcp.md](references/mcp.md).

### Prove it with a fake clock

Inject `fakeClock()` from `attend/testing` as `clock`, answer with `scriptedJudge([...])`, and assert on `recordingSink()`. Advance the clock to the exact deadline (such as `DEFAULT_POLICY.closingGraceMs`) and assert nothing happens one millisecond before it. Cover every kind the app uses, one `wait` that merges with the next window, and one failure that ends in a dead letter. [references/testing.md](references/testing.md) has the setup.

### Tune the act threshold

By default the judge's route stands. Where the judge returns real probabilities (`jevJudge`, `decisionsJudge`), set `actThreshold` so the agent acts only when `p(interrupt) + p(ask)` reaches it. Choose the value from the app's own events against the user's interruptions-per-hour budget (question 3), not by guessing: replay a recording with the threshold off, collect each judgment, and let `suggestThreshold` pick the smallest threshold that fits.

```ts
import { createAttend, memoryStore, type Event, type Judge, type Judgment } from 'attend';
import { jevJudge } from 'attend/jev';
import { fakeClock, suggestThreshold } from 'attend/testing';

export async function tune(recorded: { scope: string; event: Event }[], maxActsPerHour: number) {
  const judgments: Judgment[] = [];
  const base = jevJudge({ apiKey: process.env.TYPESAFE_API_KEY ?? '' });
  const judge: Judge = async (input, signal) => {
    const judgment = await base(input, signal);
    judgments.push(judgment);
    return judgment;
  };
  const clock = fakeClock();
  const attend = createAttend({
    store: memoryStore(),
    clock,
    judge,
    kind: (event) => (event.name === 'message.created' ? 'flush' : 'consequence'),
    sink: 'log',
    deadLetter: 'log',
  });
  const stop = new AbortController();
  const running = attend.run({ signal: stop.signal });
  const first = recorded[0]?.event.at ?? 0;
  for (const { scope, event } of recorded) {
    await clock.advance(Math.max(0, event.at - first - clock.now()));
    await attend.push(scope, [event]);
  }
  await clock.advance(120_000);
  stop.abort();
  await running;
  const last = recorded.at(-1)?.event.at ?? first;
  return suggestThreshold(judgments, { durationMs: Math.max(1, last - first), maxActsPerHour });
}
```

Write the returned `threshold` into the module as `actThreshold`. If it is `null`, no threshold fits the budget: tell the user and keep the judge's route. `llmJudge`'s self-reported probabilities are near one-hot, so a threshold changes little there.

**Check.** An end-to-end run on real events delivers at least one decision to a handler, and `observe` traces show `settled`, `judged` and `delivered` for it. Keep `observe` on while you wire it in.

## Run notes

- `run()` judges and delivers. `push`, `attach` and `flush` only persist. Start `run()` in at least one long-lived process; several may run over one shared store.
- Give every process that feeds a scope the same `kind`. The process that ingests an event is the one that kinds it.
- A scope can take several sources. Attach a given source to a scope from one process only, and never twice in one process: the second `attach` throws.
- `attend.end(scope)` tells attend the producer knows the unit is over (the turn ended): it settles at the next tick and still offers `wait`. `attend.flush(scope)` forces a settle without `wait`.
- `observe` receives every trace (`settled`, `judged`, `routeThresholded`, `retrying`, `deadLettered` and more).

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
| `push` or `flush` rejects with `ConflictError` | Eight versioned writes in a row lost to other writers on one scope. Retry; every input is idempotent. If it persists, too many producers write one scope directly: put a log in front of it and `attach` `logSource`. |
| `attach` rejects with `attend/mcp: the Client closed` | The MCP Client's transport closed. Reconnect the Client, or make a new one, and attach again. |
| `attach` throws `already attached to scope` | The process attached the same `Source.key` to the same scope twice. Attach each source once per scope per process. |
| `onError` reports `unrouted` | The server sends notifications under subscription ids attend did not open, or the app replaced the `notifications/events/*` handlers attend installed on the Client. |
| `onError` reports `idle` or `overflow` | The stream went quiet for `idleTimeoutMs`, or more than `maxBuffered` notifications waited on a slow store. attend reopened from the committed cursor; nothing is lost when the server can replay from a cursor. |
| `run()` rejects with `attend: already running` | The process called `run()` twice on one instance. Create one `Attend` per process and share it. |
| A test hangs or settles at the wrong time | `run()` started after the push, or `mcpEventsSource` or `logSource` runs on the real clock. Pass the fake clock as their `clock` option. |
| Redis Cluster replies `CROSSSLOT` | The prefix has no hash tag. Use one such as `attend:{app}:`. |
| Postgres reports that `attend_records` does not exist | `migrate()` has not run against this database and schema. |
