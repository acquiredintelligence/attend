# Sinks

A sink is `{ deliver(decision, signal) }`. attend calls it at least once per decision, one decision at a time per scope, oldest first. Build it with `routes()`, which takes one handler per delivered route and fails to compile when one is missing.

## The decision

| Field | Holds |
|---|---|
| `id` | The window id: a hash of the window's event ids. The same window redelivered has the same id. A merged window has a new one. |
| `scope` | The scope key. Ids are unique within a scope only. |
| `route` | `interrupt`, `queue`, `note` or `ask`. A sink never sees `skip` or `wait`. |
| `probabilities` | One number per route, summing to 1. Routes that were not offered are 0. |
| `predicates` | The judge's answer to each predicate, by name |
| `window` | The settled window, with `events` oldest first |
| `at` | attend's clock when the judge answered |

## Wiring template

Map each route to an operation in the host. The key `${scope}/${id}` makes each operation idempotent.

```ts
import { createAttend, defaultNotice, routes, type DeadLetter, type Decision, type Event, type Judge, type Kinded, type StateStore } from 'attend';

export interface Host {
  /** Deliver now, even mid-turn: steer or cancel the current turn. */
  interrupt(key: string, text: string): Promise<void>;
  /** Deliver as the next user turn once the current turn ends. */
  enqueue(key: string, text: string): Promise<void>;
  /** Add to the next turn's context without starting a turn. */
  addContext(key: string, text: string): Promise<void>;
  /** Show the user a prompt that calls the agent in if they accept. */
  offer(key: string, text: string): Promise<void>;
  saveDeadLetter(key: string, letter: DeadLetter): Promise<void>;
}

const keyOf = (d: { scope: string; id: string }) => `${d.scope}/${d.id}`;

export function wire(host: Host, store: StateStore, judge: Judge, kind: (event: Event) => Kinded) {
  return createAttend({
    store,
    judge,
    kind,
    sink: routes({
      interrupt: (d: Decision) => host.interrupt(keyOf(d), defaultNotice(d)),
      queue: (d: Decision) => host.enqueue(keyOf(d), defaultNotice(d)),
      note: (d: Decision) => host.addContext(keyOf(d), defaultNotice(d)),
      ask: (d: Decision) => host.offer(keyOf(d), defaultNotice(d)),
    }),
    deadLetter: (letter) => host.saveDeadLetter(keyOf(letter), letter),
  });
}
```

Each `Host` method records `key` and returns early when it has seen the key before: a unique index on an inbox table, a set of delivered keys in the agent's runtime, or an id on the message the host sends.

## Routes in common hosts

| Host | `interrupt` | `queue` | `note` | `ask` |
|---|---|---|---|---|
| Chat host | Steer the running turn, or start one now | Append a user turn after the current one | Add a system note to the next turn | Show a "Ask the agent?" chip |
| Coding agent | Stop the current step and read the notice | Add to the task queue | Add to the session context | Post a suggestion the user can accept |
| Voice agent | Barge in at the next pause | Speak when the user finishes | Hold as context and stay silent | Play a short sound or show a hint |
| Server-side agent | Push to the agent's high-priority queue | Push to its normal queue | Write to its memory store | Notify the user's client |

## Failures

- Throw to retry. attend retries through `retryBackoffMs`, then turns the decision into a dead letter.
- Throw `PermanentError` from `attend` to dead-letter at once, for a decision the host can never take.
- Each delivery holds a lease of `deliveryTimeoutMs`. attend aborts `signal` when it expires and counts it as a failed attempt. Stop when `signal` aborts.
- One stuck delivery holds back later decisions in the same scope, because delivery is in order. Dead letters go through their own lane, so a failing `deadLetter` never holds back decisions.

## Dead letters

`deadLetter(letter, signal)` is required. A `DeadLetter` has `stage` (`judge` or `sink`), `window`, `decision` (null when the judge gave up), `message`, `permanent` and `attempts`. attend retries the handler with capped backoff until it succeeds and never drops a letter. Write the letter to durable storage, keyed on `scope` and `id`, and alert on it from there.
