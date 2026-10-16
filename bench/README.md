# Benchmarks

Every number must be read with its `runner` block and `adapterVersion`. Shared-machine and unpinned runs do not verify the ingest latency objective. The scripts load a built directory by file URL. Run `pnpm build` first; stale builds are rejected.

| Command | Measures |
| --- | --- |
| `pnpm bench:selfcheck` | Protocol, accounting, schema and gate correctness |
| `pnpm bench:openloop --dist dist` | Intended-time ingest latency, throughput, CPU, scheduler lag and a separate no-op floor |
| `pnpm bench:latency --dist dist --samples 200` | Flush-to-judge and judge-to-sink segments, idle or with `--load 50000` |
| `pnpm bench:stores --dist dist --store sqlite` | Group commits, commit latency, bytes and CPU per event |
| `pnpm bench:alloc --dist dist --crosscheck` | Isolated reducer and end-to-end allocation cells, checked against heap sampling |
| `pnpm bench:suite --dist dist --out-dir bench/reports` | All measurements; `--quick` shortens runs |
| `pnpm bench:validate bench/reports/*.jsonl` | Strict report validation |
| `pnpm bench:gates bench/reports/*.jsonl` | The five SLO gates |
| `pnpm test:oracle` | Frozen reducer and engine behavior |

Reports are JSONL, validated against [lib/schema.mjs](lib/schema.mjs). Use `--out file.jsonl` to append them. No-op runs use the same intended schedule as the engine. Histograms count latency from intended send time, including missed credit and generator stalls. `--mix tape --tapes file.json` accepts `{ "tapes": [{ "name": "sample", "events": [{ "t": 0, "kind": "continuous" }] }] }`; supply tapes locally.

Measurement starts when the target parses the first line after warm-up, so the generator never pauses at the boundary. Durable bytes are the lengths of the strings each write binds: every string parameter of a write statement or command on the state port (the record JSON and its keys, never a Redis script body), and the serialized inputs, snapshot and decision rows on the scope port. Bench events are ASCII, so the lengths are bytes. Durable CPU is the attend process's own; database server CPU is not included.

SQLite modes report journal mode and synchronous separately. Postgres reports `synchronous_commit`; Redis reports both AOF enablement and `appendfsync`. `ATTEND_POSTGRES_URL` and `ATTEND_REDIS_URL` select external services. The suite skips either service if its URL is absent. Each durable run creates and removes its own tables, keys or database file.

The open-loop gates count only runs over at least 1000 scopes with 60 measured seconds. The throughput gate requires three passing runs at 50000 for each fixed-rate mix, flat first-to-last p50, no failures, and at most one core. Its knee is the highest passing rate without the CPU condition. Ingest latency requires three pinned runs per mix and rate at 10000, 30000 and 50000, with both engine p99 and floor-adjusted p99 at most 100 microseconds.

Allocation requires both complete W x S grids from the same build, within 20% of row and column medians, and at most 1536 bytes per end-to-end event at S=500. Durable reports require a report from each of SQLite, Postgres and Redis, more than one event per commit at rates of at least 1000 and at most four times the event bytes. Reaction p99 must be at most 2 ms idle on memory, 10 ms under load, or twice commit p99 plus 2 ms on durable stores. The subscriber segment remains unverified until it is measured. Missing evidence yields `not verified`; a failed check yields `not met` and exit code 1.

On Linux, use `taskset -c <generator cores>` around the command and `--target-cpus <different cores>` for the target. Both affinity sets must be proper subsets of the online CPUs and disjoint to report a pinned open-loop run. The workflow takes these lists from `ATTEND_BENCH_GENERATOR_CPUS` and `ATTEND_BENCH_TARGET_CPUS`. `ATTEND_PERF_RUNS_ON` is the dedicated runner label array in JSON. Outside Linux, CPU pinning is unavailable.
