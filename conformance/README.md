# Conformance fixtures

attend has two implementations, TypeScript in `src/` and Python in `python/`. Both run the JSON fixtures in this directory and must reproduce every recorded value. The TypeScript source is the spec: `pnpm conformance:update` rebuilds every fixture from `src`, and `pnpm test` fails when `src` and the fixtures disagree.

| Fixture | What it pins | TypeScript source |
|---|---|---|
| `step-101.json`, `step-202.json`, `step-303.json` | The pure reducer. Each plan is a list of logged inputs from a seeded random plan, stepped from `initialRecord(scope)` with its config. Every step records its effects in full and a digest of the record after it, and every 16th step and the last also record the full record. | `test/conformance/fixtures.ts` |
| `ids.json` | Window ids (FNV-1a 64 over `len:scope` and each `len:id`) for pinned and random strings, lone surrogates included, and UTF-8 lengths. | `test/conformance/fixtures.ts` |
| `threshold.json` | `thresholdRoute`, `validate` and `suggestThreshold`. NaN inputs are written as `null`. | `test/conformance/fixtures.ts` |
| `notice.json` | `defaultNotice` text. | `test/conformance/fixtures.ts` |
| `scenarios.json` | The runtime: timed pushes, flushes and ends per scope through `createAttend` on a fake clock over the memory store, with scripted judge, sink and dead-letter answers. Each records every judge input, every delivery and dead-letter attempt, every trace by scope, each call's result, inspections, the final records and the decision log. | `test/conformance/scenario.ts`, `test/conformance/scenarios.ts` |
| `judges.json` | `jevJudge`, `decisionsJudge` and `llmJudge` against scripted replies: every request byte for byte, the budget, and how the call ended. | `test/conformance/judges.ts` |

## Comparing values

- Values are compared as parsed JSON, so `1` and `1.0` are equal.
- A record digest is the first 16 hex characters of the SHA-256 of the record's stable JSON: `JSON.stringify` with every object's keys sorted by UTF-16 code units. A port must format numbers and escape strings exactly as `JSON.stringify` does to reproduce it.
- Judge request bodies are compared as exact text, with each random UUID replaced by `<uuid0>`, `<uuid1>` and so on, in order of appearance. Header names are lowercased.

## Running a scenario

A scenario starts a fake clock at `start` (default 1000), creates attend over a fresh memory store with the scenario's `kinds` map, `settle` policy and `config`, and runs it with `sweepMs` (default 1000). It then performs its steps in order and lets the runtime go idle after each one:

- `push`: push the events to the scope and await it. Events default to `at: 0` and `data: {}`.
- `advance`: advance the fake clock, firing each due timer in order and letting its work finish.
- `flush` and `end`: call them on the scope and await them.
- `release`: settle a held judge answer or handler attempt, with `answer` or with `fail` and `permanent`.
- `inspect`: record `inspect(scope)`.

After the last step it stops the runner, records `inspect` for every scope a step named, sorted, and reads the decision log from the start.

The judge answers from a script per scope, with `*` for every other scope. `answers` are used in order, and the last one repeats. With `rules`, the first rule whose conditions all hold answers: `has` and `lacks` test event ids in the window, `offers` tests the routes on offer, `first` matches only the scope's first call, and `once` matches once per window id. An answer is a judgment, `fail` (with `permanent`), `hang` (rejects with `aborted` when its signal aborts), `ignore` (never settles), `hold` (settles at a `release` step) or `malformed` (resolves with a value that is not a judgment). Sinks and dead-letter handlers use `attempts` in order, then `ok`, or `rules` matched on the decision's route.

`backpressure` traces carry no scope and are not compared.
