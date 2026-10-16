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

Delivered copies are sliced: up to 64 entries copy synchronously, and a larger copy yields between slices. Under a fake clock that fires deadlines before returning, a window of more than 64 entries is therefore delivered at a different point, relative to later work, than in the frozen shell. Dead letters for different scopes that fall due at the same fake instant can also reach the handler in a different order. Order within a scope is unchanged. The shell differential compares letters per scope and generates windows of at most 6 events, so it pins neither case.

## Policy snapshots

A load under a changed policy forces a snapshot on the next input, even when that input is a no-op. `a changed policy snapshots even a no-op tick and recomputes the exact deadline` pins the extra write.

## Trace scope in docs

`skills/attend/SKILL.md` no longer reads `trace.scope`: backpressure traces are process-wide and carry no scope.
