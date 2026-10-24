# Shell changes from the frozen shell

## Commit boundaries

Inputs applied before a write starts share a commit. Writes across scopes share a sorted group. Inputs that change nothing are not logged. `owned shell` tests pin grouped writes, version counts, and failure isolation.

Rewritten assertions in `test/attend.test.ts`:

| Case | Old assertion | New assertion and reason |
| --- | --- | --- |
| A flush between two pushes | Three puts | One commit with `events`, `flush`, `events`, version 3. All inputs arrive before the commit starts. |
| Pushes queued behind a write | Two puts | Two commits, with the queued events merged into the second input. |
| Source batches emitted without awaiting | One put | One commit carrying the merged events, final cursor, and sticky gap. |
| Writer that never wins | Eight puts | Eight commits returning conflicts. |
| Merged write failure | No stored record | The same null `inspect` result after the failed commit. |

All stored-record assertions in `test/attend.test.ts` and `test/mcp.test.ts` now call `attend.inspect` instead of `store.get`. Their expected records are unchanged. Their store doubles wrap `load` and `commit` instead of `get` and `put`.

In `test/ingest.test.ts`, the slow-write occupants change from an empty push to the real event `held`. Expected classifier calls and stored entries include that event. Pipelined-source assertions include it in both the initial write and the resumed record. Write counts now count commits. The wrapper's write observations read the committed input arguments, and its read observations replay `load` results. All other expected values are unchanged.

## Versions

Version counts committed inputs, including multiple inputs in one write. A duplicate or empty push adds no version. `version counts committed inputs, including multi-input writes and excluding no-ops` pins the count.

An untouched scope stays absent after an empty punctuation. The differential compares that absent record as the initial empty record, with version excluded. `end settles at the next tick as closing with wait, and an empty end writes nothing` pins absence of a write.

## Overdue deadlines

While running, an input arriving after an exact deadline first applies a logged tick. `an overdue deadline logs tick before events and closes the earlier window first` pins input order and window membership. Fake clocks fire deadlines before returning, so the differential sees no overdue change.

## No-op completion

A no-op shares an open write's outcome, or resolves immediately when no write is open. `a duplicate waits for the open write to become durable` pins the durability boundary.

## Conflicts

The shell reloads and re-applies the whole conflicting write's raw inputs before later queued inputs. The write's inputs share the conflict budget and abandonment. Existing writer-conflict cases and the multi-scope crash suite pin recovery and mixed outcomes.

## Event validation

Non-string `id`, `name`, and present `text` reject with `TypeError` without writing. The boundary cases and the arbitrary Unicode copy-size property pin this check.

## Window budget

A declared judge budget supplies `maxWindowBytes` unless explicitly configured. Budgeted windows can settle with reason `size`. The three `window budget` cases pin the derived value, explicit override, and absent budget. Invalid shares, thresholds, and nonpositive budgets reject at construction.

## Log delivery

`sink: 'log'` and `deadLetter: 'log'` append final rows without a delivery lane. `log modes append final rows and leave both delivery lanes empty` pins the modes. Decision-subscription tests pin copying, cursors, filters, gaps, and uninterrupted pagination.

## Delivery order

Delivered copies are sliced: up to 64 entries copy synchronously, and a larger copy yields between slices. Under a fake clock that fires deadlines before returning, a window of more than 64 entries is therefore delivered at a different point, relative to later work, than in the frozen shell. Dead letters for different scopes that fall due at the same fake instant can also reach the handler in a different order. Order within a scope is unchanged except for windows above 64 entries under a fake clock. The shell differential compares letters per scope and generates windows of at most 6 events, so it pins neither case.

## Policy snapshots

A load under a changed policy forces a snapshot on the next input, even when that input is a no-op. `a changed policy snapshots even a no-op tick and recomputes the exact deadline` pins the extra write.

## Trace scope in docs

`skills/attend/SKILL.md` no longer reads `trace.scope`: backpressure traces are process-wide and carry no scope.

# Python port

`python/` ports the TypeScript package to Python 3.12 or later. The fixtures in `conformance/` pin what the two must agree on. This section lists every place the Python port differs and why.

## Scheduling within one scope

A Python coroutine runs on without yielding until it reaches real I/O, while every JavaScript `await` yields. When a judge call and a delivery for one scope are in flight at once, their results can commit in the other order. With any real store that order is a race in TypeScript too. Each lane keeps its order, and the records, decisions, dead letters and log rows are equal. The Python scenario runner therefore compares traces per lane: the scope's own lane, its decisions lane and its dead-letter lane, each in order. 2 of the 53 scenarios differ only in that cross-lane order.

## Cancellation

- `aqi_attend.signal` provides `AbortController` and `AbortSignal`, and adds `wait()` and `race()`. A judge, sink or dead-letter handler gets a signal and must honour it, as in TypeScript. attend never cancels such a call while its lease runs.
- A `capture` still running at `capture_timeout_ms` is cancelled. In TypeScript its promise is left pending.
- When `run()` ends, any judge call or delivery still pending after its lease has expired is cancelled. In TypeScript it runs on, and a late result can be submitted after `run()` resolved. Either way the expired lease has already handed the work back.
- Cancelling the task that runs `run()` stops it the way `stop()` does.

## Intake

- `push`, `flush` and `end` return asyncio futures and take their input when called, as the TypeScript promises do, so concurrent pushes merge and group exactly as there. Each caller gets a future of its own, so cancelling one never cancels another push merged into the same input. The input is still applied.
- `run()` starts when called, as in TypeScript, and returns the coroutine to await or run as a task, so `stop()` straight after `create_task(attend.run())` stops it.
- Event data must hold JSON values: dicts with string keys, lists or tuples, strings, ints, floats, booleans and None. NaN and the infinities become null and -0.0 becomes 0, as JSON writes them. Any other value raises `TypeError`, where TypeScript would call its `toJSON` (a `Date`) or drop it (`undefined`, a function).
- The memory store's tail accounting recomputes each event's rendered size, where TypeScript caches it in a `WeakMap`. The value is the same.

## The open window

TypeScript keeps a dead-prefix offset (`start`) and compacts it lazily, to avoid V8's `shift`. Python deletes removed entries in place. The entries, summaries, sizes and hashes are the same, and the reducer fixtures pin them.

## Names

- The Python API is snake_case: `create_attend` keyword arguments and `settle` keys such as `closing_grace_ms`. `max_queued_inputs` and `max_lag_ms` replace `admission`, `cache_idle_ms` replaces `cache.idleMs`, and `decisions(signal=..., cursor=..., scope=...)` takes keywords in place of `(filter, options)`. `suggest_threshold(judgments, duration_ms=..., max_acts_per_hour=...)` takes keywords too, and error fields are `retry_after_ms` and `request_id`.
- Every value that crosses a store or a judge keeps the TypeScript field names: events, windows, decisions, dead letters, records, traces, logged inputs and the policy. That keeps the persisted layout, so one store serves both runtimes (`python/tests/test_interop.py`), and it lets the fixtures compare values directly.
- Error, trace and dead-letter messages keep the TypeScript text, including the judges' `jevJudge:` and `decisionsJudge:` prefixes, so a dead letter reads the same whichever runtime wrote it.

## Stores

- Redis runs on redis-py's asyncio client with the Lua scripts of `src/store/redis.ts` copied verbatim; `python/tests/test_redis.py` fails if they drift. `blocking` takes a client in place of `blockingSend`. redis-py runs each concurrent command on a pooled connection, and its default pool raises once 100 are in use, where node-redis pipelines everything on one connection. Use a `BlockingConnectionPool`.
- Postgres runs on asyncpg. `listener` is a dedicated `asyncpg.Connection` in place of a `listen` function. Statements cast every parameter whose type Postgres cannot infer, and they select explicit columns, reading `xid8` only as text, because asyncpg has no `xid8` codec. The tables, columns, migration and JSON are the same.
- There is no SQLite store, and no `redisLog`, `postgresLog` or `sqliteLog` event log.

## Judges

- The hosted judges use httpx. `client`, an `httpx.AsyncClient`, replaces the `fetch` option. On abort the request is cancelled and the signal's reason is raised.
- Errors from a response are `ProviderError`, or `PermanentProviderError` for a permanent one. Each carries `status`, `request_id` and `retry_after_ms`.

## Not ported

`attend serve`, the MCP Events server and `mcpEventsSource`, the SQLite store, the event-log adapters for Redis and Postgres, `freezingStore`, `eventsWireConformance`, `logConformance`, the frozen oracle and its differential tests, and every bench but latency.
