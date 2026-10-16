# attend

[![ci](https://github.com/acquiredintelligence/attend/actions/workflows/ci.yml/badge.svg)](https://github.com/acquiredintelligence/attend/actions/workflows/ci.yml)

Decide when an event should wake your agent.

An app emits a firehose of events: log lines, messages, clicks, alerts. An agent that wakes on every one is noisy, and one behind a debounce is deaf to the message that mattered. attend sits between the two. It settles each scope's events into a window, asks a judge what the agent should do about the window, and hands the decision to your app, which keeps the final say in how that route runs.

```text
  app events (the firehose)
        |   push() in-process, or NDJSON over HTTP to attend serve
        v
  +----------------------------------------------------------+
  |  attend                                                  |
  |   window    one per scope; events pile up                |
  |   settle    the act in it is finished, or the window     |
  |             is old, or full, or you flushed it           |
  |   judge     reads the settled window, picks a route:     |
  |             interrupt | queue | note | ask | skip | wait |
  |   log       every step and decision is persisted         |
  +----------------------------------------------------------+
        |  ~4 to 8 decisions a minute, not thousands of events
        v
  decisions out
        |-- in-process: your sink's handler for the route
        '-- attend serve: MCP Events poll, stream and tools, for any MCP client
        v
  your agent reacts (barge in, take it next turn, add context, offer the user a prompt);
  your app decides how each route is carried out
```

The judge picks the route and the app executes it: `interrupt` means "the agent should hear this now", and your handler decides whether that cancels the turn, steers it or shows a banner. Raw events never leave attend. Only settled, judged decisions do.

```sh
npm install attend
```

attend needs Node 22.13 or later and ships ESM only. The core has no runtime dependencies and imports nothing from Node.

## Two ways to run it

One engine, run either way.

| | Embedded | `attend serve` |
|---|---|---|
| What | `createAttend` in your process | The same engine as a process of its own: a sidecar next to your app |
| Events in | `attend.push(scope, events)` | `POST /v1/ingest` with NDJSON, from any language |
| Decisions out | Your `sink`, called in-process | MCP Events: `events/poll`, `events/stream`, and the `attend_wait` and `attend_window` tools |
| Use it for | **Real-time agents.** The default. Flush to delivery stays in-process, with no network hop. | Apps that are not TypeScript, several agents on one firehose, or a team that wants one shared attention service |

Raw events never cross MCP, so the MCP hop carries a few decisions a minute and adds about a millisecond to a judge call that takes 140 ms or more.

### attend serve

```sh
npm install attend @modelcontextprotocol/server zod
npx attend serve --store sqlite:attend.db --judge jev --kinds kinds.json
# attend serving http://127.0.0.1:8787  (ingest POST http://127.0.0.1:8787/v1/ingest, MCP http://127.0.0.1:8787/mcp)
```

One command, no image. The MCP server package is an optional peer of `attend`, so the embedded engine stays dependency-free; `attend serve` exits with an error naming it when it is missing. Every flag has an environment variable, and a flag wins.

| Flag | Env | Default | |
|---|---|---|---|
| `--store` | `ATTEND_STORE` | `memory` | `memory`, `sqlite:<file>`, `postgres:<url>` or `redis:<url>`. Postgres and Redis need the `pg` or `redis` package installed. |
| `--judge` | `ATTEND_JUDGE` | `rule` | `rule` interrupts on a flush and queues otherwise, with no network. `jev` reads `TYPESAFE_API_KEY`. `decisions` reads `OPENAI_API_KEY` and needs `--judge-model`. |
| `--kinds` | `ATTEND_KINDS` | none | Event name to kind, as inline JSON or a file: `{"user.said":"flush","build.failed":"closing"}` |
| `--default-kind` | `ATTEND_DEFAULT_KIND` | `consequence` | Kind for names `--kinds` does not list |
| `--host`, `--port`, `--socket` | `ATTEND_HOST`, `ATTEND_PORT`, `ATTEND_SOCKET` | `127.0.0.1`, `8787` | Port 0 picks a free one. A socket path replaces host and port. |
| `--act-threshold` | `ATTEND_ACT_THRESHOLD` | off | See [the act threshold](#the-act-threshold) |
| `--token` | `ATTEND_TOKEN` | none | Require `Authorization: Bearer <token>` on ingest and MCP |

Send events from anything that can POST:

```sh
curl -H 'content-type: application/x-ndjson' --data-binary @events.ndjson http://127.0.0.1:8787/v1/ingest
# {"scope":"ops/db","id":"a1","at":1760570000123,"name":"alert.fired","data":{},"text":"replica lag 40s on db-2"}
# {"scope":"ops/db","end":true}
```

A line is an event, `{"scope", "flush": true}` or `{"scope", "end": true}`. The reply is `{accepted, duplicates, rejected: [{line, scope, error}]}` once every accepted line has committed, and a `429` with `Retry-After` when attend is over its limit. `POST /v1/ingest/stream` keeps one NDJSON request open and answers with acks and credits.

An agent hears the decisions by subscribing to `attention.decision` on `/mcp`. Hosts that do not subscribe call the `attend_wait` tool in a loop. In TypeScript, `decisionFeed` from `attend/mcp` keeps the cursor for you. This is [`examples/serve.ts`](examples/serve.ts), which runs the server and a subscriber in one process:

```ts
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';
import type { Transport } from '@modelcontextprotocol/sdk/shared/transport.js';
import { createAttend, memoryStore } from 'attend';
import { decisionFeed } from 'attend/mcp';
import { serve } from 'attend/server';

const store = memoryStore();
const attend = createAttend({
  store,
  kind: (event) => (event.name === 'page.acked' ? 'flush' : event.name === 'alert.fired' ? 'closing' : 'consequence'),
  judge: async ({ window }) => ({ route: window.reason === 'flush' ? 'interrupt' : 'note' }),
  sink: 'log',
  deadLetter: 'log',
});

const server = serve(attend, store, { listen: { port: 0 } });
await server.ready;
const url = server.url();
console.log(`attend serving ${url}`);

const client = new Client({ name: 'pager-agent', version: '1.0.0' });
// The SDK's class types sessionId as optional, which exactOptionalPropertyTypes rejects.
await client.connect(new StreamableHTTPClientTransport(new URL(`${url}/mcp`)) as Transport);
const stop = new AbortController();
const feed = decisionFeed(client, { cursor: null, signal: stop.signal })[Symbol.asyncIterator]();
```

`serve(attend, store, options)` is the same thing as a library call: it runs `attend.run`, the events server over `store.log` and the ingest routes on one HTTP listener. Give it `principal` to authorize by scope prefix, and close it with `await server.close()`.

## Quickstart

```ts
import { createAttend, defaultNotice, memoryStore, routes, type Decision } from 'attend';

const tell = (decision: Decision) => console.log(`[${decision.route}]\n${defaultNotice(decision)}`);

const attend = createAttend({
  store: memoryStore(),
  kind: (event) => (event.name === 'user.message' ? 'flush' : 'consequence'),
  judge: async ({ window }) => ({ route: window.reason === 'flush' ? 'interrupt' : 'note' }),
  sink: routes({ interrupt: tell, queue: tell, note: tell, ask: tell }),
  deadLetter: (letter) => console.error('dead letter', letter.id, letter.message),
});

void attend.run({ signal: AbortSignal.timeout(1000) });
await attend.push('session-1', [
  { id: 'e1', at: Date.now(), name: 'build.failed', data: {}, text: 'Build 4521 failed: 1 test' },
  { id: 'e2', at: Date.now(), name: 'user.message', data: {}, text: 'can you look?' },
]);
```

The build failure alone never wakes the agent. The user's message settles the window at once, and the agent hears both lines in one notice:

```text
[interrupt]
Build 4521 failed: 1 test
can you look?
Attend to this now, even mid-task.
```

Before production, replace the function judge with [`jevJudge` or `llmJudge`](#judges) and `memoryStore` with a [database store](#storage).

## Demo

`pnpm demo` starts a real MCP Events server in the same process, streams a short CI story into attend, and prints what the agent hears. It needs no API key and no database. The source is [`examples/demo.ts`](examples/demo.ts); the script builds the package and runs it with `node --experimental-strip-types`, so Node 22.13 or later runs it as TypeScript.

```text
attend demo: a CI server streams MCP Events, and attend decides what the agent hears.

 0.0s  event    ci.started {"run":4521,"branch":"main"}
 0.1s  event    ci.log {"line":"pnpm install"}
 0.2s  event    ci.log {"line":"pnpm test"}
 0.3s  event    ci.log {"line":"211 passed, 1 failed"}
 0.4s  event    ci.test_failed {"test":"checkout applies the coupon"}
 0.6s  settle   5 events (closing)
 0.6s  judge    wait (hold the window and judge it again)
 1.0s  event    comment.created {"author":"sam","body":"can you look?"}
 1.0s  settle   6 events (flush), merged with the held window
 1.0s  judge    interrupt
 1.0s  deliver  interrupt
                ci.started {"run":4521,"branch":"main"}
                ci.log {"line":"pnpm install"}
                ci.log {"line":"pnpm test"}
                ci.log {"line":"211 passed, 1 failed"}
                ci.test_failed {"test":"checkout applies the coupon"}
                comment.created {"author":"sam","body":"can you look?"}
                Attend to this now, even mid-task.
 1.8s  event    deploy.finished {"env":"staging","sha":"9f3c2e1"}
 2.0s  settle   1 event (closing)
 2.0s  judge    note
 2.0s  deliver  note
                deploy.finished {"env":"staging","sha":"9f3c2e1"}
                Background only. No reply is needed.

7 events in, 2 deliveries out. Waking on every event would have run the agent 7 times.
```

attend held the log lines until the failed test closed the window. The judge chose `wait` for the failure, so the user's comment merged into the held window, and one interrupt carried all six events to the agent. The deploy arrived as a `note`.

## How it works

```text
  MCP Events, webhooks, push()
        |
        v
  kind(event)        closing | flush | continuous | passive | consequence | context
        |
        v
  window             one per scope, persisted with the source's cursor
        |            settles when its earliest deadline passes
        v
  judge(window)      interrupt | queue | note | ask | skip | wait
        |                                                      |
        v                                                      +-- held, judged again later
  sink.deliver(decision)    at least once, idempotent on the decision id
```

1. A source hands events to a scope. A scope is whatever one agent attends to: a conversation, a repo or a device. Each scope has one persisted state: a cursor per attached source, the open window, the judge slot and the outbox.
2. `kind(event)` gives each event a settle role when it arrives. attend stores the kind on the event, so a later change to `kind` never reinterprets an open window.
3. The window settles at the earliest deadline among its events. A window that holds only `consequence` and `context` events never settles.
4. The judge sees the settled window and picks a route. One judge call runs per scope at a time. Events that arrive during the call wait in the next window.
5. The decision is appended to the store's decision log in the same write as the state change, so a restart loses nothing. In-process, the sink receives it; over `attend serve`, subscribers read it from the log. It carries the route, the window, a probability for each route and the answers to any predicates. The judge never writes text. `defaultNotice(decision)` builds the agent's notice from the events.

### Kinds

| Kind | Use it for | Settles the window |
|---|---|---|
| `closing` | The end of an act: a submit, a save, a merged pull request, a finished build | `closingGraceMs` after the last closing event, so the output it causes can join the window |
| `flush` | Someone addresses the agent directly: a message to it, an @mention | At once. `wait` is not offered. |
| `continuous` | One step of an ongoing act: typing, scrolling, dragging, a partial transcript | Once no continuous event has arrived for `streamGapMs` |
| `passive` | A dwell or a look: a hover, a focus, a page view | `passiveQuietMs` after the last passive event |
| `consequence` | System output: a log line, a status change, a tool result | Never. It joins the open window. |
| `context` | The agent's own output | Never. It joins the open window as context for the judge, and `defaultNotice` leaves it out. |

`closing`, `flush`, `continuous` and `passive` events are acts: things that happen in the scope and can call for the agent, whether the user did them or an outside party did. A window also settles when its first act is `maxWindowMs` old (reason `age`), when it fills its share of the judge's context window (reason `size`, see [the window budget](#the-window-budget)), when you call `attend.end(scope)` because the producer knows the unit is over (reason `closing`, `wait` still offered), or when you call `attend.flush(scope)` (reason `forced`, with `wait` not offered). When a window passes `maxEvents`, attend drops its oldest consequence and context events and counts them in `window.dropped`. If the window is still too big, it settles with reason `overflow`. attend never drops an act. Consequence and context events that wait longer than `maxWindowMs` for an act are dropped and counted the same way.

A system event that should wake the agent on its own, such as a failed deploy or a fired alert, needs an act kind. `closing` is the usual choice.

### Routes

| Route | The agent | Reaches the sink |
|---|---|---|
| `interrupt` | Hears it now, even mid-turn. | Yes |
| `queue` | Hears it when its current turn ends. | Yes |
| `note` | Gets it as context for its next turn and does not respond to it. | Yes |
| `ask` | Stays quiet. Your app offers the user a prompt to call the agent in. | Yes |
| `skip` | Hears nothing. | No. The verdict joins the scope's recent verdicts. |
| `wait` | Hears nothing yet. attend holds the window and judges it again with the next window that settles, or after `waitHorizonMs` together with everything that arrived meanwhile, with `wait` withheld. | No. A window waits at most `maxWaits` times. |

attend picks the route, and your sink carries it out. `routes()` makes you write a handler for each of the four delivered routes.

## MCP Events

`mcpEventsSource` from `attend/mcp` turns one MCP Events subscription into a source. Install the peer dependencies with `npm install @modelcontextprotocol/sdk zod`; zod 3.25 or later and zod 4 both work. This excerpt is from [`examples/mcp-events.ts`](examples/mcp-events.ts), with `attend` set up as in the quickstart and `controller` an `AbortController`:

```ts
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import { mcpEventsSource } from 'attend/mcp';

const client = new Client({ name: 'my-agent', version: '1.0.0' });
await client.connect(new StdioClientTransport({ command: 'node', args: ['./ci-events-server.js'] }));

const source = mcpEventsSource(client, {
  name: 'repo.activity',
  arguments: { repo: 'acme/shop' },
  onError: (error) => console.warn('mcp events', error),
});

await Promise.all([
  attend.run({ signal: controller.signal }),
  attend.attach('repo:acme/shop', source, { signal: controller.signal }),
]);
```

The source covers these parts of the draft:

- In the default `auto` mode it reads `events/list`, streams with `events/stream` when the event lists `push` delivery, and polls with `events/poll` otherwise. A `-32026` (Unsupported) reply to a stream falls back to polling.
- Every batch carries the server's cursor, and attend commits it in the same write as the events, under the source's key (`mcp:<name>:<arguments>`). After a restart, `attach` resumes from the committed cursor.
- `eventId` becomes `Event.id`, so the scope drops redelivered events.
- `truncated: true` sets `window.gap`. The judge sees it, and `defaultNotice` says that events were lost.
- When a poll result sets `hasMore`, the source polls again at once. Otherwise it waits `nextPollMs`, or `pollMs` (default 10000) when the server sends none.
- A terminated stream reopens from the committed cursor. So does a stream with more than `maxBuffered` (default 1000) notifications waiting on a slow store, which is reported as `overflow`, and a stream that sends nothing, heartbeats included, for `idleTimeoutMs` (default 300000), which is reported as `idle`.
- A `-32025` (ResourceExhausted) reply or a request timeout is thrown as a plain error, so `attach` backs off through `retryBackoffMs` and resubscribes from the committed cursor. Any other error ends the subscription: `attach` rejects with a `PermanentError` whose `cause` is the SDK error.
- When the Client's transport closes, `attach` rejects with a `PermanentError` that says the Client closed. Reconnect the Client and attach again.
- `onError` receives each `notifications/events/error`, each payload that fails validation, each `overflow`, each `idle`, and each `unrouted` notification, one that names a subscription no stream here opened. None of them ends the subscription.
- Several streams can share one `Client`. attend installs one handler for each of the five `notifications/events/*` methods on the Client, records the JSON-RPC id of each `events/stream` request and routes notifications by `_meta["io.modelcontextprotocol/subscriptionId"]`. Do not replace those handlers.

MCP events carry no one-line summary, so `Event.text` stays unset and `defaultNotice` prints each event's name and JSON data.

## Judges

**The speed default is a system-one classifier.** `jevJudge` (TypeSafe's Jev, with your own TypeSafe key) and `decisionsJudge` (the OpenAI Decisions API) answer a fixed multiple-choice question with probabilities in roughly 140 ms or more, which is what a real-time agent can afford on every settled window. `llmJudge` calls a general model you already use; it is slower, and its self-reported probabilities are close to one-hot. Use it when you want reasoning over speed.

A judge is a function, `(input: JudgeInput, signal: AbortSignal) => Promise<Judgment>`. It gets the settled `window`, the `routes` on offer with your criteria, your `guidance`, the `predicates` to answer, live `facts` from `context()` and the scope's `recent` verdicts. It returns `{ route, probabilities?, predicates? }`.

- Throw to retry with backoff. Throw a `PermanentError` to dead-letter the window at once.
- A route that was not offered counts as a retryable failure. Check `input.routes` before you answer `wait`.
- Stop when `signal` aborts. attend aborts the call after `judgeTimeoutMs`.

### llmJudge

`llmJudge` from `attend/llm` works with any model that answers in JSON matching a schema. You pass that one call as `complete`. attend writes the prompt and the schema and validates the reply. With Claude:

```ts
import Anthropic from '@anthropic-ai/sdk';
import { PermanentError } from 'attend';
import { llmJudge } from 'attend/llm';

const anthropic = new Anthropic();

export const judge = llmJudge({
  complete: async ({ system, prompt, images, schema, signal }) => {
    const res = await anthropic.messages.create(
      {
        model: 'claude-opus-5-5',
        max_tokens: 1024,
        system,
        messages: [
          {
            role: 'user',
            content: [
              { type: 'text', text: prompt },
              ...images.map((image) => ({ type: 'image' as const, source: { type: 'base64' as const, media_type: image.mimeType, data: image.data } })),
            ],
          },
        ],
        output_config: { effort: 'low', format: { type: 'json_schema', schema } },
      },
      { signal },
    );
    if (res.stop_reason === 'refusal') throw new PermanentError('judge refused');
    const text = res.content.find((b) => b.type === 'text');
    return JSON.parse(text?.type === 'text' ? text.text : 'null');
  },
});
```

[`skills/attend/references/judges.md`](skills/attend/references/judges.md) has the same judge on the OpenAI Responses API and the Vercel AI SDK. The default prompt is `DEFAULT_SYSTEM`, and each route's default criterion is in `DEFAULT_CRITERIA`. Override them with `system` and `criteria`.

### jevJudge

`jevJudge` from `attend/jev` calls TypeSafe's Jev, a classifier built for this kind of question, with your own TypeSafe key. It asks one `choice` question for the route and one `noul` question per predicate, and sends the state as JSON. The model defaults to `jev-latest`; `jev-preview`, `jev-1.14.0` and `jev-1.13.0` are the others. Pass `endpoint` to go through OpenRouter or LiteLLM, and `longContext: true` for 262k tokens of context at some cost in speed and accuracy. Its probabilities are real, so [the act threshold](#the-act-threshold) works with it.

```ts
import { jevJudge } from 'attend/jev';

export const judge = jevJudge({ apiKey: process.env.TYPESAFE_API_KEY ?? '' });
```

### decisionsJudge

`decisionsJudge` from `attend/decisions` calls the OpenAI Decisions API. It asks one `choice` question for the route and one `predicate` question per predicate.

```ts
import { decisionsJudge } from 'attend/decisions';

export const judge = decisionsJudge({ apiKey: process.env.OPENAI_API_KEY ?? '', model: '<model id>' });
```

Both hosted judges retry network errors and 408, 429 and 5xx responses, and dead-letter the window at once on any other 4xx, on a billing or quota 429, and when the model refuses the route question. Every error from a response carries `status`, `requestId` and `retryAfterMs`. Both return the classifier's `confidence` in the route on `decision.confidence`.

### The window budget

A window must never overload the judge that reads it. A judge declares its input budget (`jevJudge` declares 32k tokens, or 262k with `longContext`; `llmJudge` and `decisionsJudge` take `contextTokens`), and attend turns it into a size limit on the open window: `(contextTokens - reservedTokens) * windowShare * bytesPerToken`, with `windowShare` defaulting to 0.5. attend keeps a running size estimate for each window as events arrive. A window that reaches its share settles early with reason `size`, and a judge that still gets more than fits drops sheddable events (consequence and context, oldest first) when it renders and never an act or a flush. As a backstop, a provider's context-length rejection triggers exactly one retry with the window halved. A judge that declares no budget leaves the limit off. Set `settle.maxWindowBytes` yourself to override.

### The act threshold

By default the judge's route stands. A judge that returns real probabilities (`jevJudge`, `decisionsJudge`) also ranks how much the agent should act, and only your app knows whether an interruption or a missed moment costs more. Set `actThreshold` in (0, 1] on `createAttend`, and the route becomes an acting route (`interrupt` or `ask`, whichever is likelier and offered) exactly when `p(interrupt) + p(ask)` is at least the threshold, and a quieter route (`skip`, `note`, `queue` or `wait`, by probability) otherwise. It is off by default. A judgment without probabilities is never changed, and a changed route emits a `routeThresholded` trace. The threshold is applied before the decision is logged, so replays stay deterministic.

Pick it from your own traffic instead of guessing. Replay a recorded stream with the threshold off, record each judgment, and `suggestThreshold` from `attend/testing` returns the smallest threshold whose acts fit an interruptions-per-hour budget, with the rate at every candidate:

```ts
import { suggestThreshold } from 'attend/testing';
import type { Judge, Judgment } from 'attend';

export const judgments: Judgment[] = [];
export const recording = (judge: Judge): Judge => async (input, signal) => {
  const judgment = await judge(input, signal);
  judgments.push(judgment);
  return judgment;
};

// After replaying an hour of recorded events through createAttend({ judge: recording(judge), ... }):
export const { threshold, candidates } = suggestThreshold(judgments, { durationMs: 3_600_000, maxActsPerHour: 6 });
```

`threshold` is null when no threshold fits the budget. `llmJudge`'s probabilities are near one-hot, so a threshold changes little there.

### Images

Give `createAttend` a `capture(scope, signal)` that returns a `JudgeImage` (base64 `data` and a `mimeType`) or a list of them, such as a screenshot of what the user is looking at, and every judge call carries them as `input.images`. A capture that throws or takes longer than `captureTimeoutMs` (default 500) never fails the call: the judge runs without images and an observer sees a `captureFailed` trace. `jevJudge` sends PNG and JPEG within the provider's limits (280 to 1568 px per side, 16 MiB per file) and drops the rest, reporting each through `onImageError`; `decisionsJudge` sends up to 128 of any format. Either one retries once without images when the provider rejects them. `llmJudge` hands them to `complete` as `images`, as the snippet above shows.

### Your own judge

Any function with the judge signature works, and judges compose as functions. This one answers a direct message without calling a model:

```ts
import type { Judge } from 'attend';

export const withShortcut =
  (judge: Judge): Judge =>
  async (input, signal) =>
    input.window.reason === 'flush' ? { route: 'interrupt' } : judge(input, signal);
```

To ask the judge yes-or-no questions beside the route, pass `predicates` to `createAttend`, such as `{ asked: 'Did the user ask the agent for this?' }`. Each answer arrives on `decision.predicates` as a probability.

## Sinks

A sink delivers decisions to the agent. attend calls `sink.deliver(decision, signal)` at least once per decision, one decision at a time per scope, oldest first. After a crash it can deliver the same decision again with the same id, so make the sink idempotent on `decision.id`, which is unique across scopes. Throw to retry, or throw a `PermanentError` to dead-letter at once.

`routes()` builds a sink from one handler per delivered route. A sink never sees `skip` or `wait`. This excerpt from [`examples/postgres-worker.ts`](examples/postgres-worker.ts) writes every decision to an inbox table that the agent's runtime reads:

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

## Storage

One `ScopeStore` holds each scope's latest snapshot, the inputs committed since it, the due index and the decision log. attend replays the short tail of inputs over the snapshot to recover a scope, so a restart loses nothing.

| Store | Create it | Guarantees | Caveats |
|---|---|---|---|
| Memory | `memoryStore()` from `attend` | Atomic within one process. | State is lost when the process exits. Use it for tests, demos and browser apps. |
| SQLite | `sqliteStore(db)` from `attend/sqlite`, with `db` a `node:sqlite` `DatabaseSync`, a `bun:sqlite` `Database`, or any object with the same `exec` and `prepare`. `sqliteStore({ file, worker: true })` runs the database on a worker thread. | One transaction per group of writes. Durable on a file. Creates its tables on construction. | Given a handle, calls block the event loop for their duration; the worker mode does not. Processes that share the file must run on one host. |
| Postgres | `postgresStore(pool)` from `attend/postgres`, after `await migrate(pool)` | One statement commits a group of scopes' writes together, and decisions wake subscribers in other processes through `LISTEN` and `NOTIFY` when you pass a `listener`. Durable. Safe for many replicas. | Run `migrate()` at deploy with a role that can create tables. The store needs only `SELECT`, `INSERT`, `UPDATE` and `DELETE`. The table prefix must be lowercase. PostgreSQL 13 or later. |
| Redis | `redisStore((args) => client.sendCommand(args))` from `attend/redis` | One script validates and writes a group of scopes' writes together, and keeps the decision log in a stream. Redis 6.2 or later. | Durable only as far as your Redis persistence settings. On Redis Cluster, a group's keys must hash to one slot. Put a hash tag in the prefix, such as `redisStore(send, { prefix: 'attend:{app}:' })`. |

Each store also takes `retain` (decision rows kept by count or age), so the log does not grow without bound. Every store passes the same conformance and crash suites.

Several processes can share one store. Any of them can `push`, and any number can `run` or serve the log: a write names the stored version it read, a writer that loses reloads and applies its input again, and every input is idempotent. Give every process the same `kind` function, because the process that ingests an event is the one that kinds it. A scope can take several sources at once; the record keeps one cursor per `Source.key`. Attach a given source to a scope from one process only, since two processes running the same source would overwrite each other's cursor, and a second `attend.attach` of the same key in one process throws.

Each adapter also has an `EventLog`: `memoryLog`, `sqliteLog`, `postgresLog` and `redisLog`. When producers live in other processes and you want a durable shared log in front of attend, append to the log and attach `logSource(log, scope)` instead of calling `push`.

## Guarantees

- **One snapshot and a short tail per scope.** The snapshot holds the open window, the judge slot and the outbox. A write appends the inputs that changed state, and a snapshot replaces the tail once it grows past the snapshot's own size. A source's cursor commits in the same write as the events it covers.
- **Versioned writes.** Each write names the version it read. A writer that loses waits a few jittered milliseconds, reloads and applies its input again. After 8 losses in a row it gives up: `push` and `flush` reject with `ConflictError`, and work in flight recovers when its lease expires.
- **Effects only after a committed write.** attend calls the judge and the sink only for state it has persisted. `push` and `flush` persist and return; the runner settles, judges and delivers. If a process crashes after a write and before its effect, the work stays in the store, and a runner retries it when its lease expires.
- **The decision log is the delivery for subscribers.** The decision row commits with the state change that made it. Subscribers read it by cursor, so a reconnect resumes where it stopped; a cursor that fell behind retention is reported as `truncated`, never silently skipped.
- **At-least-once delivery with stable ids.** A decision's id is a hash of the scope and its window's event ids, so a redelivery carries the same id and no two scopes share one. Deliveries in one scope run one at a time, oldest first.
- **Dead letters are never dropped.** The judge and the sink each get `retryBackoffMs.length` retries. A window that still fails becomes a dead letter, which attend retries with capped backoff until `deadLetter` accepts it. A lane that fills to its cap (`maxOutbox` decisions, `maxDeadLetters` dead letters) makes `push` and a source's `emit` reject until it drains.
- **Crash-tested at every commit boundary.** `crashSuite` runs a scripted session once, then once more for each commit with a crash just before it and again just after it. Every run must deliver the same decision ids and dead-letter ids. CI runs it against the memory, SQLite, Postgres and Redis stores.

## Testing with a fake clock

`attend/testing` makes settling deterministic. Inject `fakeClock()`, answer with `scriptedJudge`, and assert on `recordingSink`. This excerpt is from [`examples/testing.ts`](examples/testing.ts), which runs with `node --test`:

```ts
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
```

- `clock.advance(ms)` fires every timer due on the way and lets each one's work finish before it returns.
- `scriptedJudge` answers in order, repeats its last answer, and records every input in `calls`. An answer can also be `{ fail: 'message' }`, `{ fail: 'message', permanent: true }` or `{ hang: true }`.
- `recordingSink` records `delivered` and `attempts`. `failNext(n)` makes the next `n` deliveries throw.
- Pass the same clock to `mcpEventsSource` and `logSource` through their `clock` option, so their polling runs on fake time too.
- `harness` runs attend on a fake clock over a store that it can crash at any commit. Use it for restart tests.

## Writing an adapter

A store implements `ScopeStore` (see [`src/store/types.ts`](src/store/types.ts), where each member's guarantee is in its doc comment):

- `load(scope)` returns the latest snapshot and the inputs committed after it, or `null`.
- `commit(writes)` applies each scope's write only if its stored version equals `expectedVersion` (0 means no stored scope), and returns `'ok'` or `'conflict'` for each. The inputs, the snapshot, the due value and the decision rows of one write land together or not at all. Of N concurrent writes at one version, exactly one is `'ok'`.
- `due(now, limit, after?)` pages the scopes whose due value has passed. It reads and claims nothing.
- `remove(scope)` forgets the scope.
- `log` is the `DecisionLog`: the epoch, bounds, reads by position, `find`, `wait` and `trim`.

Run the conformance suites and the crash suites against your adapter with any test runner. Each returns named async cases. [`examples/adapter-suites.ts`](examples/adapter-suites.ts) runs them against `memoryStore` with `node:test`:

```ts
import { test } from 'node:test';
import { memoryStore } from 'attend';
import { crashSuite, multiScopeCrashSuite, scopeStoreConformance, decisionLogConformance } from 'attend/testing';

for (const c of scopeStoreConformance(() => memoryStore())) test(c.name, c.run);
for (const c of crashSuite(() => memoryStore())) test(c.name, c.run);
for (const c of decisionLogConformance(() => memoryStore())) test(c.name, c.run);
for (const c of multiScopeCrashSuite(() => memoryStore())) test(c.name, c.run);
```

The factory must return a fresh, empty store for each case. For a database, give each case its own table prefix or key prefix. An `EventLog` adapter runs `logConformance(make, { canBlock })`, where `canBlock` says whether `read` waits for `blockMs`.

## Defaults

These are round starting points, not tuned values. Override any of them with `settle` in `createAttend`. Times are in milliseconds.

| Setting | Default | What it does |
|---|---|---|
| `closingGraceMs` | 500 | How long after a closing event the window settles |
| `streamGapMs` | 1000 | How long every stream must be quiet before a continuous gesture settles |
| `passiveQuietMs` | 4000 | How long after a passive event the window settles |
| `waitHorizonMs` | 10000 | How long a waiting window waits for the next window before attend judges it alone |
| `maxWaits` | 2 | How many times one window may wait |
| `maxEvents` | 250 | Events one window holds. Past it, attend drops consequence and context events, then settles. |
| `maxWindowMs` | 60000 | Age of the first act at which the window settles |
| `windowShare` (a `createAttend` option) | 0.5 | Share of the judge's context budget a window may fill before it settles with reason `size` |
| `actThreshold` (a `createAttend` option) | off | Act when p(interrupt) + p(ask) reaches it |
| `retryBackoffMs` | `[1000, 4000, 16000]` | Backoff before each retry of a judge call, a delivery or a source. Its length is the retry budget for judges and sinks; a source repeats the last rung. |
| `judgeTimeoutMs` | 15000 | Aborts a judge call. The judge's lease is twice this. |
| `deliveryTimeoutMs` | 10000 | Lease on one delivery. An expired lease counts as a failed attempt. |
| `recentCount` | 8 | Recent verdicts the judge sees |
| `seenCount` | 500 | Event ids each scope remembers for dedup |
| `maxOutbox` | 1000 | Undelivered decisions per scope before `push` and a source's `emit` reject with `OutboxFullError` |
| `maxDeadLetters` | 1000 | Undelivered dead letters per scope before `push` and a source's `emit` reject with `OutboxFullError` |
| `deadLetterBackoffMaxMs` | 60000 | Cap on the backoff between dead-letter attempts |

`run()` sweeps the store for due scopes every `sweepMs` (1000), up to `sweepLimit` (100) scopes per sweep.

## Agent skill

The repo ships an [Agent Skill](skills/attend/SKILL.md) that leads your coding agent through wiring attend into your app. It works out which events the app emits, which of them are signals, and which deserve which reaction, then wires it with one small module and a few call sites at your existing event chokepoints. It tells you in a few lines what it will do, asks one batch of questions with a recommended default for each, and tunes the act threshold from a replay of your own events. To install it in Claude Code:

```text
/plugin marketplace add acquiredintelligence/attend
/plugin install attend@attend
```

For Codex or Cursor, copy `skills/attend` into your project's `.agents/skills/`. The npm package includes the skill at `node_modules/attend/skills/attend`. The third-party Vercel Labs CLI also installs it with `npx skills add acquiredintelligence/attend`. That CLI is not part of this project.

## Status

attend is v0, and its API can change before 1.0. MCP Events is a working-group draft, so its wire shapes can change too. `attend/mcp` follows the current draft.

## License

MIT. Built by the team behind [Zettel](https://zettel.study).
