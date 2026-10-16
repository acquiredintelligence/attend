# Settle rules

How a scope's open window settles, exactly. All times are on attend's clock at ingest (`entry.seenAt`), never the source's `event.at`. Defaults are in `DEFAULT_POLICY`, exported from `attend`.

## Deadlines

Every event in the open window can set a deadline. The window settles at the earliest one, with that deadline's reason.

| Deadline | At | Reason |
|---|---|---|
| A `flush` event | Its arrival | `flush` |
| The last `closing` event | Its arrival plus `closingGraceMs` | `closing` |
| The last `continuous` event, on any stream | Its arrival plus `streamGapMs` | `stream_quiet` |
| The last `passive` event | Its arrival plus `passiveQuietMs` | `passive_quiet` |
| The first act (`closing`, `continuous` or `passive`) | Its arrival plus `maxWindowMs` | `age` |
| `attend.flush(scope)` | The call | `forced` |
| More than `maxEvents` events left after shedding | The last arrival | `overflow` |

- A later closing, continuous or passive event moves its own deadline later. A steady stream of continuous events holds the window open until the streams go quiet, or until the `age` deadline.
- A window with no flush and no act has no deadline. It never settles.
- After a `flush` or `forced` settle, the judge is not offered `wait`.

## Shedding

- When the window holds more than `maxEvents` events, attend drops its oldest `consequence` and `context` events, adds them to `window.dropped`, and traces `dropped` with `why: 'overflow'`. If only acts and flushes are left and the window is still too big, it settles with reason `overflow`.
- `consequence` and `context` events that arrived before the first act and are older than `maxWindowMs` are dropped the same way, with `why: 'age'`.
- attend never drops an act or a flush.

## One judge call at a time

Each scope has one judge slot. While a judge call runs, new events collect in the open window, and that window cannot settle. When the judge answers, a window whose deadline has already passed settles at once.

## Wait and merge

- `wait` moves the settled window into a held state until `waitHorizonMs` from the answer.
- If the open window settles before the horizon, the held window and the new one merge, held events first. The merged window has a new id, because the id is a hash of all its event ids, and it keeps the held window's `waits` count. The judge is offered `wait` again only while `waits` is below `maxWaits` and the reason is neither `flush` nor `forced`.
- At the horizon, attend judges the held window alone, with `wait` withheld.
- `attend.flush(scope)` on a scope that holds only a waiting window judges it at once, with `wait` withheld.

## What the judge sees

`input.window` carries:

- `id`, which becomes the decision id;
- `events`, oldest first, each with its `kind`, its `stream` when continuous, and `seenAt`;
- `reason`, the settle reason from the table above;
- `waits`, how many times this window was held;
- `dropped`, how many consequence and context events were shed;
- `gap`, true when a source reported lost events, such as MCP `truncated: true`;
- `settledAt`.

## Kinds by agent

These are starting points. Run the question sequence in `SKILL.md` on your own event names.

- **Coding agent on a repo.** A comment that names the agent is `flush`. A merged pull request, a failed build and a finished deploy are `closing`. CI log lines and status changes are `consequence`. The agent's own commits and messages are `context`.
- **Voice agent.** A final transcript is `flush`. Partial transcripts are `continuous` on stream `speech`. Tool results are `consequence`. The agent's own speech is `context`.
- **Inbox agent.** A message to the agent's address is `flush`. A new email that the agent should triage with no user act is `closing`. The user opening an email is `passive`, and the user sending a reply is `closing`. Delivery receipts are `consequence`.
- **On-call agent.** A page or a fired alert is `closing`. Metric samples and log lines are `consequence`. An incident-channel message that mentions the agent is `flush`.
- **Gesture app.** Hand and pointer motion are `continuous`, one stream per hand or pointer. A gaze dwell is `passive`. Grabbing or releasing an object is `closing`. Physics and render results are `consequence`.
