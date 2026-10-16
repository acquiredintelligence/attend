# How attend works

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

The kinds, the routes and the guarantees are listed in [the reference](reference.md).
