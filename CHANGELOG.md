# Changelog

What changed in each release, for people who use attend. The npm package `@acquiredintelligence/attend` and the Python package `aqi-attend` are released together under one version number.

## 0.3.0 (2026-11-14)

This is the first release of the Python package. The npm package gains folded notices and five opt-in features, and `attend serve` delivers decisions sooner on Redis and Postgres.

### Read this before you upgrade from 0.2.0

- **The default notice reads differently.** `defaultNotice` now folds repeated events into one line. If your agent or your tests depend on one line per event, pass `{ fold: false }`.
- **Upgrade every process that shares a store before you turn on a new option.** The stored format moves from reducer revision 1 to 5. 0.3.0 reads what 0.2.0 wrote and writes a fresh snapshot the first time it touches a scope. Nothing stops a 0.2.0 process from reading or writing a scope that 0.3.0 has written, and it will apply its old rules to it. Once events carry `observedAt` or `quietForMs`, or a scope uses `continuation: 'accumulate'`, going back to 0.2.0 changes how that stored state behaves.
- **`observedAt` and `quietForMs` are now event fields.** If your events already carried top-level fields with those names, attend now reads them. It rejects the batch when `observedAt` is in the future or not a finite number, and when `quietForMs` is invalid or sits on an event that is not `continuous`.
- **`attend serve` holds one more connection** to Redis or Postgres. See "Fixed" below.

### Added

- **A Python package.** `pip install aqi-attend`, then `import aqi_attend`. It is an asyncio port with the same semantics and the same decision ids as the TypeScript package, and either one can read and continue a scope the other wrote in a shared Redis or Postgres. It needs Python 3.12 or later and has no required dependencies; the `redis`, `postgres` and `http` extras add the store and judge clients. Start with the [Python guide](python/README.md).
- **Folded notices.** `defaultNotice` (`default_notice` in Python) turns a run of events that read the same into one line with its count, and a run of `continuous` events under one name into its first and last step with the count. Twenty-six identical failures are now one line instead of twenty lines and a count. `maxLines` counts folded lines. Only the text changes: the window, the decision id and what the judge sees are the same. `attend serve` sends the folded text in each decision's `notice` field. See [the notice](docs/sinks.md#the-notice). (#41)
- **`foldEvents`** (`fold_events` in Python) gives you the same groups for a notice you write yourself: `first`, `last`, `count` and `mode` for each run, using comparisons you supply. See [write your own folded notice](docs/sinks.md#write-your-own-folded-notice). (#42)
- **Observation time.** An event can carry `observedAt`, the time its source saw it. attend then measures quiet periods and window age from that time instead of from when the event arrived, which matters for a source that buffers or delivers late. Leave it out and timing is as before. See [observation time](docs/reference.md#observation-time). (#43)
- **A quiet duration for each continuous event.** A `continuous` event can carry `quietForMs`, so events that share a window can wait different lengths of quiet before the window settles. Events without it use `streamGapMs` as before. See [continuous quiet duration](docs/reference.md#continuous-quiet-duration). (#44)
- **Serial intake.** When events need asynchronous preparation before they can be pushed, `openIntake(attend, scope)` from the new `@acquiredintelligence/attend/intake` entry (`attend.open_intake(scope)` in Python) runs one preparation and its immediate judgments and deliveries at a time, in the order the work was registered. A quiet timer or an earlier delivery can no longer overtake work that is still being prepared. Closing the intake waits for every preparation it started to finish cleaning up. The entry needs Node; the default entry is unchanged and still imports nothing from Node. See [serial intake](docs/reference.md#serial-intake). (#45, #47)
- **Accumulating continuation.** With `settle: { continuation: 'accumulate' }`, a window the judge answered with `wait`, or one whose judge call failed, stays one window: events that arrive meanwhile join it, the judge can be offered `wait` again within `maxWaits`, and a retry never starts before its backoff ends. The default, `separate`, is the 0.2.0 behavior. A scope keeps the mode it was written with, and attend refuses to change the mode while the scope still holds unresolved events. See [accumulating continuation](docs/reference.md#accumulating-continuation). (#46)
- **Separate limits for counted and background events.** With accumulating continuation, `settle.eventLimits: { counted, background }` (`event_limits` in Python) settles the window once it holds `counted` acts, and separately keeps at most `background` context and consequence events, dropping the oldest first. It takes the place of `maxEvents`, and setting both is rejected. See [independent event limits](docs/reference.md#independent-event-limits). (#48)

### Fixed

- **`attend serve` on Redis delivered every decision up to 250 ms late.** Subscribers on `events/stream`, `events/poll` and `attend_wait` polled the decision log instead of blocking on it. `attend serve --store redis:` now opens a second connection for a blocking read, so a waiting subscriber wakes as soon as the decision is written. If you build the Redis store yourself for `serve()` or for `attend.decisions`, pass `blockingSend` to get the same; [choose a store](docs/storage.md) shows how. (#36)
- **`attend serve` on Postgres delivered decisions on the next poll, up to 100 ms late.** `attend serve --store postgres:` now holds one more connection that listens for new decisions. If you build the Postgres store yourself, pass a `listener`; [choose a store](docs/storage.md) shows how. (#38)
- **Restarting with changed `settle` options could apply finished input a second time.** On startup the TypeScript engine replayed a scope's stored inputs under the new options instead of the policy saved with the scope. It now replays under the saved policy. (#48)

## 0.2.0 (2026-10-16)

A leaner `createAttend`, and a guide for putting attend in front of a Mastra agent.

## 0.1.0 (2026-10-16)

The first release on npm.
