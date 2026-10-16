# attend serve reference

## Flags

Every flag has an environment variable, and a flag wins.

| Flag | Env | Default | |
|---|---|---|---|
| `--store` | `ATTEND_STORE` | `memory` | `memory`, `sqlite:<file>`, `postgres:<url>` or `redis:<url>`. Postgres and Redis need the `pg` or `redis` package installed. |
| `--judge` | `ATTEND_JUDGE` | `rule` | `rule` interrupts on a flush and queues otherwise, with no network. `jev` reads `TYPESAFE_API_KEY`. `decisions` reads `OPENAI_API_KEY` and needs `--judge-model`. |
| `--kinds` | `ATTEND_KINDS` | none | Event name to kind, as inline JSON or a file: `{"user.said":"flush","build.failed":"closing"}` |
| `--default-kind` | `ATTEND_DEFAULT_KIND` | `consequence` | Kind for names `--kinds` does not list |
| `--host`, `--port`, `--socket` | `ATTEND_HOST`, `ATTEND_PORT`, `ATTEND_SOCKET` | `127.0.0.1`, `8787` | Port 0 picks a free one. A socket path replaces host and port. |
| `--act-threshold` | `ATTEND_ACT_THRESHOLD` | off | See [the act threshold](judges.md#the-act-threshold) |
| `--token` | `ATTEND_TOKEN` | none | Require `Authorization: Bearer <token>` on ingest and MCP |

## Ingest

A line is an event, `{"scope", "flush": true}` or `{"scope", "end": true}`. The reply is `{accepted, duplicates, rejected: [{line, scope, error}]}` once every accepted line has committed, and a `429` with `Retry-After` when attend is over its limit. `POST /v1/ingest/stream` keeps one NDJSON request open and answers with acks and credits.
