# Reference

## Kinds

| Kind | Use it for | Settles the window |
|---|---|---|
| `closing` | The end of an act: a submit, a save, a merged pull request, a finished build | `closingGraceMs` after the last closing event, so the output it causes can join the window |
| `flush` | Someone addresses the agent directly: a message to it, an @mention | At once. `wait` is not offered. |
| `continuous` | One step of an ongoing act: typing, scrolling, dragging, a partial transcript | Once no continuous event has arrived for `streamGapMs` |
| `passive` | A dwell or a look: a hover, a focus, a page view | `passiveQuietMs` after the last passive event |
| `consequence` | System output: a log line, a status change, a tool result | Never. It joins the open window. |
| `context` | The agent's own output | Never. It joins the open window as context for the judge, and [`defaultNotice`](sinks.md#the-notice) leaves it out. |

`closing`, `flush`, `continuous` and `passive` events are acts: things that happen in the scope and can call for the agent, whether the user did them or an outside party did. A window also settles when its first act is `maxWindowMs` old (reason `age`), when it fills its share of the judge's context window (reason `size`, see [the window budget](judges.md#the-window-budget)), when you call `attend.end(scope)` because the producer knows the unit is over (reason `closing`, `wait` still offered), or when you call `attend.flush(scope)` (reason `forced`, with `wait` not offered). When a window passes `maxEvents`, attend drops its oldest consequence and context events and counts them in `window.dropped`. If the window is still too big, it settles with reason `overflow`. attend never drops an act. Consequence and context events that wait longer than `maxWindowMs` for an act are dropped and counted the same way.

A system event that should wake the agent on its own, such as a failed deploy or a fired alert, needs an act kind. `closing` is the usual choice.

## Observation time

An event can supply `observedAt` when its source observed it before delivering it to attend. It is a finite timestamp in the same millisecond coordinates as the engine's `Clock`. `push` and `attach` reject future values before admitting the batch. Convert a source clock to the engine's time coordinates before supplying it; attend does not clamp the value.

| Field | Meaning |
|---|---|
| `at` | Informational time reported by the source. It does not set deadlines. |
| `observedAt` | Optional source observation time for quiet periods, the act age bound and background age pruning. |
| `seenAt` | Time the engine applies the input. The engine sets it and retains it even when `observedAt` is present. |

When a kind includes an explicit observation time, its quiet period starts at the latest effective observation for that kind. The age bound uses the earliest effective act time once an act supplies `observedAt`. An omitted field uses that entry's `seenAt`. Kinds whose entries omit `observedAt` retain their existing handling of backwards engine clocks. Entry order and duplicate handling do not change.

```ts
import { createAttend } from '@acquiredintelligence/attend';
import { fakeClock } from '@acquiredintelligence/attend/testing';

const clock = fakeClock(1000);
const attend = createAttend({
  clock,
  kind: { change: 'continuous' },
  judge: async () => ({ route: 'note' }),
  sink: 'log',
});
await attend.push('repo', { id: 'a', name: 'change', at: 0, observedAt: 100 });
console.log((await attend.inspect('repo'))?.dueAt); // 1100
```

An already-due observation settles through the normal runner at processing time. It never reopens a resolved window or backdates `settledAt`. A bulk push remains one batch. Flush, overflow and byte limits keep processing-time semantics. Waits, retries, leases, timeouts and decision timestamps also keep engine time. A stale background observation can be pruned as soon as it arrives, with the usual dropped count and trace.

Both runtimes retain `observedAt` through snapshots, logged inputs, windows and decision records. See [storage](storage.md) for version coordination when sharing a store.

## Continuous quiet duration

A continuous event can supply a finite, nonnegative `quietForMs`. Zero makes its quiet deadline immediate. `push` and `attach` refuse invalid values or a duration on any other kind before admitting the batch or committing its cursor.

Once a continuous entry supplies a duration, that group's deadline is the maximum of each entry's observation time plus its duration. An omitted duration uses `streamGapMs`; an omitted observation uses ingestion time. A late observation can therefore be due on arrival, but settlement still happens at processing time. Entry order does not change.

```ts
import { createAttend } from '@acquiredintelligence/attend';
import { fakeClock } from '@acquiredintelligence/attend/testing';

const clock = fakeClock(100);
const attend = createAttend({
  clock,
  kind: { dial: 'continuous', confirmation: 'continuous' },
  judge: async () => ({ route: 'note' }),
  sink: 'log',
});
await attend.push('device', { name: 'dial', quietForMs: 40 });
await clock.advance(20);
await attend.push('device', { name: 'confirmation', quietForMs: 90 });
console.log((await attend.inspect('device'))?.dueAt); // 210
```

Closing and passive deadlines can still settle the window earlier. Flush, age and bounds keep their existing precedence. After a wait, only fresh entries set a new quiet deadline; held entries retain their durations as data. Waits, retries and leases keep engine time.

When every continuous entry omits `quietForMs`, existing timing remains unchanged, including backwards engine clocks. Explicit durations persist through snapshots, tails and decisions. Replay uses the snapshot policy for omitted durations. Changing a live engine's policy recalculates those default durations; it never changes an explicit duration. [Storage](storage.md) describes version coordination.

## Routes

| Route | The agent | Reaches the sink |
|---|---|---|
| `interrupt` | Hears it now, even mid-turn. | Yes |
| `queue` | Hears it when its current turn ends. | Yes |
| `note` | Gets it as context for its next turn and does not respond to it. | Yes |
| `ask` | Stays quiet. Your app offers the user a prompt to call the agent in. | Yes |
| `skip` | Hears nothing. | No. The verdict joins the scope's recent verdicts. |
| `wait` | Hears nothing yet. attend holds the window and judges it again with the next window that settles, or after `waitHorizonMs` with the fresh input. Separate mode withholds wait at that horizon; [accumulating continuation](#accumulating-continuation) can reoffer it within its limits. | No. A window waits at most `maxWaits` times. |

attend picks the route, and your sink carries it out. `routes()` makes you write a handler for each of the four delivered routes.

## Serial intake

Use serial intake when asynchronous preparation belongs to a scope's event order. It registers raw work before preparation starts, then runs one preparation and its immediate effects at a time. Ordinary `push` keeps its existing behavior.

In Node, import `openIntake` from `@acquiredintelligence/attend/intake`. This optional entry uses Node's async context; the default package entry does not import it. Open the handle before this engine has touched the scope through input, attachment or a runner sweep. `inspect` only reads the store and does not claim a scope. Start `run()` before submitting; it starts synchronously when called.

```ts
import { createAttend } from '@acquiredintelligence/attend';
import { openIntake } from '@acquiredintelligence/attend/intake';

const attend = createAttend({
  kind: { message: 'flush' },
  judge: async () => ({ route: 'note' }),
  sink: 'log',
});
const intake = openIntake(attend, 'channel', { capacity: 100 });
const running = attend.run();
try {
  await intake.submit(async (signal) => {
    signal.throwIfAborted();
    return {
      events: [{ id: 'a', name: 'message', at: Date.now(), data: {} }],
      cursor: '1',
    };
  }, { source: 'feed' });
} finally {
  await intake.close();
  await attend.stop();
  await running;
}
```

`submit(prepare, { source?, signal? })` registers synchronously and returns a promise. Preparation returns a native `SourceBatch`; provide `source` to commit its cursor with the events. An empty batch can advance the cursor without settling a window. A preparation error commits nothing. Keep preparation free of writes or other external effects, and honor its abort signal.

A timer claims the scope before its write starts. During preparation or immediate effects, timer wakes coalesce. Registered raw work gets the next turn before an idle deferred timer, which checks the current deadline again. An admitted nonempty batch uses the existing reducer tick after admission. No stored window policy changes.

The promise covers the input write and the immediate chain of judgments, deliveries and their completion, release or deferral writes. A failed immediate write rejects it. A persisted wait or retry with a future deadline releases the turn; the promise does not wait for that future attempt or an open window's quiet deadline. A cursor can therefore already be committed when a later effect write fails. A caller that retries must supply stable event ids and make external delivery idempotent.

An opted-in scope refuses ordinary `push`, `attach`, `flush` and `end`, including after close. Same-engine, same-scope `submit` or `close` from preparation, context, judge or delivery rejects with `IntakeError`; awaiting it would wait on itself. Other scopes progress independently. Do not create cycles by awaiting each other's intakes from callbacks.

`capacity` is a positive safe integer, default 20,000. It bounds queued and active submissions on this handle. Registered preparation also shares `admission.maxQueuedInputs` with ordinary traffic in this engine. The reservation transfers into normal input admission without double charging. These are counts per engine, not byte limits or limits across multiple engines. Full queues reject with `BackpressureError`; they do not silently drop work.

Abort before application prevents event and cursor admission. An input already applied may still commit; abort does not roll it back. A withdrawn submission can reject promptly and release its admission slot while its callback finishes cancellation cleanup. Later submissions can proceed. `close()` fences new work synchronously, aborts preparation and active effect signals, and joins every preparation awaitable the intake started, including earlier withdrawn work, plus owned immediate effects. Late preparation results are discarded. Normal effect results still awaiting application become native releases after close. Close cannot undo an external delivery, and a callback or store that never settles can keep it pending. Callbacks must await their own dependency cleanup; arbitrary detached child work is not joined. Repeated close calls join the same cleanup.

The handle is terminal after close or engine shutdown. Recreate the engine and open a new handle before running to resume committed state. Raw registrations and callbacks are not durable. Snapshots and tails contain only the existing reducer inputs; another runtime can read them without an intake. This provides local ordering in one engine, not ownership across processes sharing a store.

## Accumulating continuation

Set `settle.continuation` to `accumulate` when waits and provider retries should retain one unresolved window. Omission or `separate` keeps the existing behavior. Serial intake and this option are independent: intake orders preparation and immediate work; continuation determines what retained state does afterward.

```ts
import { createAttend } from '@acquiredintelligence/attend';

const attend = createAttend({
  kind: { reading: 'continuous' },
  judge: async () => ({ route: 'queue' }),
  sink: 'log',
  settle: { continuation: 'accumulate' },
});
```

After a `wait`, fresh entries alone drive quiet. At the horizon, attend reoffers the whole unresolved window, including fresh entries that are not quiet yet. It can offer `wait` again until `maxWaits` or a whole-window bound requires resolution. A provider failure retains the wait count; a successful `wait` retains the provider failure count. The latter selects the next retry backoff.

During retry, its deadline is a strict not-before time. Fresh quiet, direct address, `flush()`, `end()`, age, count and bytes do not start a judgment earlier. At retry time, every entry still retained after pruning joins the held window. Gap and dropped-entry metadata join its next offer even when no fresh entry survives, and are then cleared from the fresh buffer. A timer that owns serial intake before a later raw registration can retry without that later entry. `flush()` still removes wait and supplies reason `forced`. `end()` applies to fresh open input, keeps reason `closing` and permits wait within the limits; with only an already-settled held window it remains a no-op.

Eligibility uses the complete offered payload after the usual background shedding. Acts are never dropped. Count above `maxEvents`, rendered bytes at or above `maxWindowBytes`, and age at or above `maxWindowMs` withhold wait. Age starts at the earliest unresolved act's effective observation time. These checks also apply at initial offers, exact quiet/age ties and after absorbing an older observation. A bound makes a waiting window due; during backoff it takes effect at retry time. It does not interrupt an already running judge. A bound can withhold wait even when another deadline supplied the window's reason. Count and rendered bytes are not hard memory-allocation limits.

Wait count, provider failures and the native attempt token are separate. Release and deferral refund the attempt token, so it is not a count of physical callback starts. A non-wait judgment or terminal failure ends the held window. Exhaustion keeps the existing dead-letter record and fields; `attempts` does not become a provider-failure count. Delivery retries do not reopen judgment. Engine or intake close retains committed state for recovery and does not reset its counters.

Continuation is saved with the snapshot. A live writer cannot change between modes while a held batch or fresh entries remain. Empty scopes can change mode while retaining recent decisions and outbox work. In particular, an old separate waiting snapshot has no historic failure count to recover; switching it to accumulate is refused. `inspect` reads using the saved policy. See [storage](storage.md) for version coordination.

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
