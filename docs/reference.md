# Reference

## Kinds

| Kind | Use it for | Settles the window |
|---|---|---|
| `closing` | The end of an act: a submit, a save, a merged pull request, a finished build | `closingGraceMs` after the last closing event, so the output it causes can join the window |
| `flush` | Someone addresses the agent directly: a message to it, an @mention | At once. `wait` is not offered. |
| `continuous` | One step of an ongoing act: typing, scrolling, dragging, a partial transcript | Once no continuous event has arrived for `streamGapMs` |
| `passive` | A dwell or a look: a hover, a focus, a page view | `passiveQuietMs` after the last passive event |
| `consequence` | System output: a log line, a status change, a tool result | Never. It joins the open window. |
| `context` | The agent's own output | Never. It joins the open window as context for the judge, and `defaultNotice` leaves it out. |

`closing`, `flush`, `continuous` and `passive` events are acts: things that happen in the scope and can call for the agent, whether the user did them or an outside party did. A window also settles when its first act is `maxWindowMs` old (reason `age`), when it fills its share of the judge's context window (reason `size`, see [the window budget](judges.md#the-window-budget)), when you call `attend.end(scope)` because the producer knows the unit is over (reason `closing`, `wait` still offered), or when you call `attend.flush(scope)` (reason `forced`, with `wait` not offered). When a window passes `maxEvents`, attend drops its oldest consequence and context events and counts them in `window.dropped`. If the window is still too big, it settles with reason `overflow`. attend never drops an act. Consequence and context events that wait longer than `maxWindowMs` for an act are dropped and counted the same way.

A system event that should wake the agent on its own, such as a failed deploy or a fired alert, needs an act kind. `closing` is the usual choice.

## Routes

| Route | The agent | Reaches the sink |
|---|---|---|
| `interrupt` | Hears it now, even mid-turn. | Yes |
| `queue` | Hears it when its current turn ends. | Yes |
| `note` | Gets it as context for its next turn and does not respond to it. | Yes |
| `ask` | Stays quiet. Your app offers the user a prompt to call the agent in. | Yes |
| `skip` | Hears nothing. | No. The verdict joins the scope's recent verdicts. |
| `wait` | Hears nothing yet. attend holds the window and judges it again with the next window that settles, or after `waitHorizonMs` together with everything that arrived meanwhile, with `wait` withheld. | No. A window waits at most `maxWaits` times. |

attend picks the route, and your sink carries it out. `routes()` makes you write a handler for each of the four delivered routes.

## Guarantees

- **One snapshot and a short tail per scope.** The snapshot holds the open window, the judge slot and the outbox. A write appends the inputs that changed state, and a snapshot replaces the tail once it grows past the snapshot's own size. A source's cursor commits in the same write as the events it covers.
- **Versioned writes.** Each write names the version it read. A writer that loses waits a few jittered milliseconds, reloads and applies its input again. After 8 losses in a row it gives up: `push` and `flush` reject with `ConflictError`, and work in flight recovers when its lease expires.
- **Effects only after a committed write.** attend calls the judge and the sink only for state it has persisted. `push` and `flush` persist and return; the runner settles, judges and delivers. If a process crashes after a write and before its effect, the work stays in the store, and a runner retries it when its lease expires.
- **The decision log is the delivery for subscribers.** The decision row commits with the state change that made it. Subscribers read it by cursor, so a reconnect resumes where it stopped; a cursor that fell behind retention is reported as `truncated`, never silently skipped.
- **At-least-once delivery with stable ids.** A decision's id is a hash of the scope and its window's event ids, so a redelivery carries the same id and no two scopes share one. Deliveries in one scope run one at a time, oldest first.
- **Dead letters are never dropped.** The judge and the sink each get `retryBackoffMs.length` retries. A window that still fails becomes a dead letter, which attend retries with capped backoff until `deadLetter` accepts it. A lane that fills to its cap (`maxOutbox` decisions, `maxDeadLetters` dead letters) makes `push` and a source's `emit` reject until it drains.
- **Crash-tested at every commit boundary.** `crashSuite` runs a scripted session once, then once more for each commit with a crash just before it and again just after it. Every run must deliver the same decision ids and dead-letter ids. CI runs it against the memory, SQLite, Postgres and Redis stores.

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
