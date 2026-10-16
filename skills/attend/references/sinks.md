# Sinks

A sink is `{ deliver(decision, signal) }`. attend calls it at least once per decision, one decision at a time per scope, oldest first. Build it with `routes()`, which takes one handler per delivered route and fails to compile when one is missing.

## The decision

| Field | Holds |
|---|---|
| `id` | The window id: a hash of the scope and the window's event ids, so it is unique across scopes. The same window redelivered has the same id. A merged window has a new one. |
| `scope` | The scope key |
| `route` | `interrupt`, `queue`, `note` or `ask`. A sink never sees `skip` or `wait`. |
| `probabilities` | One number per route, summing to 1. Routes that were not offered are 0. |
| `predicates` | The judge's answer to each predicate, by name |
| `window` | The settled window, with `events` oldest first |
| `at` | attend's clock when the judge answered |

## Wiring template

Map each route to an operation in the host. The decision id makes each operation idempotent.

```ts
import { createAttend, defaultNotice, routes, type DeadLetter, type Decision, type Event, type Judge, type Kind, type ScopeStore } from 'attend';

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

export function wire(host: Host, store: ScopeStore, judge: Judge, kind: (event: Event) => Kind) {
  return createAttend({
    store,
    judge,
    kind,
    sink: routes({
      interrupt: (d: Decision) => host.interrupt(d.id, defaultNotice(d)),
      queue: (d: Decision) => host.enqueue(d.id, defaultNotice(d)),
      note: (d: Decision) => host.addContext(d.id, defaultNotice(d)),
      ask: (d: Decision) => host.offer(d.id, defaultNotice(d)),
    }),
    deadLetter: (letter) => host.saveDeadLetter(letter.id, letter),
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
- Nothing is dropped. A scope whose sink stays down fills its decision lane to `maxOutbox`, and one whose dead-letter handler stays down fills its dead-letter lane to `maxDeadLetters`. Either cap makes `push` and a source's `emit` reject with `OutboxFullError` until the lane drains, and `attach` backs off and resubscribes on its own.

## Dead letters

`deadLetter(letter, signal)` is required. A `DeadLetter` has `stage` (`judge` or `sink`), `window`, `decision` (null when the judge gave up), `message`, `permanent` and `attempts`. attend retries the handler with capped backoff until it succeeds and never drops a letter. Write the letter to durable storage, keyed on its `id`, and alert on it from there.
