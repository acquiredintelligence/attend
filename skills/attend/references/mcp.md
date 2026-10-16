# MCP Events sources

`mcpEventsSource(client, options)` from `attend/mcp` turns one MCP Events subscription into a `Source`. It needs the peer dependencies `@modelcontextprotocol/sdk` and `zod` (3.25 or later, or 4).

## Wire it

```ts
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import { createAttend, memoryStore, routes } from 'attend';
import { mcpEventsSource } from 'attend/mcp';

const client = new Client({ name: 'my-agent', version: '1.0.0' });
await client.connect(new StdioClientTransport({ command: 'node', args: ['./ci-events-server.js'] }));

const attend = createAttend({
  store: memoryStore(),
  kind: (event) => (event.name === 'comment.created' ? 'flush' : 'consequence'),
  judge: async () => ({ route: 'queue' }),
  sink: routes({ interrupt: async () => {}, queue: async () => {}, note: async () => {}, ask: async () => {} }),
  deadLetter: async () => {},
});

const controller = new AbortController();
const source = mcpEventsSource(client, { name: 'repo.activity', arguments: { repo: 'acme/shop' } });
await Promise.all([attend.run({ signal: controller.signal }), attend.attach('repo:acme/shop', source, { signal: controller.signal })]);
```

- One subscription feeds one scope. Pick the scope from the subscription's `arguments`, such as one scope per repo. A scope can take several sources; each keeps its own cursor under its `Source.key`, which for this source is `mcp:<name>:<arguments as sorted JSON>`.
- Several subscriptions can share one `Client`. attend installs one notification handler for each of the five `notifications/events/*` methods the first time it uses a Client, records the JSON-RPC id of each `events/stream` request, and routes by `_meta["io.modelcontextprotocol/subscriptionId"]`. Do not replace those handlers.
- Attach a given subscription to a scope from one process. Every batch commits the server's cursor with its events, and `attach` resumes from the committed cursor after a restart.

## Options

| Option | Default | Does |
|---|---|---|
| `name` | required | The event name from `events/list` |
| `arguments` | none | The subscription's arguments |
| `mode` | `'auto'` | `'auto'` streams when `events/list` lists `push` delivery for the event and polls otherwise. `'stream'` and `'poll'` force one. |
| `maxAgeMs` | none | Passed through. Bounds replay when resuming from a cursor. |
| `maxEvents` | none | Passed through as the poll page size |
| `pollMs` | 10000 | Poll interval when the server sends no `nextPollMs` |
| `maxBuffered` | 1000 | Stream notifications that may wait while an earlier one persists. Past it, the source drops what it holds, reports `overflow`, and reopens from the committed cursor so the server resends them. |
| `idleTimeoutMs` | 300000 | How long a stream may go without any notification, heartbeats included. Past it, the source reports `idle` and reopens from the committed cursor. |
| `clock` | the system clock | Pass the test's fake clock here too |
| `onError` | ignore | Receives `{ kind: 'stream' }` for `notifications/events/error`, `{ kind: 'malformed' }` for a payload that failed validation, `{ kind: 'overflow' }`, `{ kind: 'idle' }`, and `{ kind: 'unrouted' }` for a notification under a subscription id no stream here opened |

## What the source does with the protocol

- `eventId` becomes `Event.id`, the occurrence's `name` becomes `Event.name`, `timestamp` becomes `Event.at`, and `data` becomes `Event.data`. `Event.text` stays unset, so `defaultNotice` prints the name and the JSON data.
- `truncated: true` on a poll result or on `notifications/events/active` sets `window.gap`.
- A poll result with `hasMore` polls again at once. Otherwise the source waits `nextPollMs`, or `pollMs`.
- A malformed occurrence goes to `onError` and is skipped. Its cursor still advances.
- A `notifications/events/terminated`, a stream request that returns, an overflow or an idle stream reopens the stream from the committed cursor after one second.

## Errors

`events/list`, `events/poll` and `events/stream` errors all follow this table.

| Error | The source |
|---|---|
| `-32025` ResourceExhausted, or a request timeout | Throws it as a plain error. `attach` backs off through `retryBackoffMs` (the last rung repeated), traces `retrying` with stage `source`, and resubscribes from the committed cursor. |
| `-32026` Unsupported on `events/stream` | Switches to `events/poll` in `auto` mode. In `stream` mode it ends the subscription with a `PermanentError`. |
| `-32000` ConnectionClosed | The Client's transport closed. `attach` rejects with a `PermanentError` that says to reconnect the Client and attach again. |
| `-32023` NotFound, `-32024` Forbidden, `-32027`, any other code | Ends the subscription. `attach` rejects with a `PermanentError` whose `cause` is the SDK error. |

A rejected `attach` means the server refused the subscription or the Client closed. Reconnecting the Client and attaching again fixes the second; retrying the first gets the same answer, so log it and alert.

## Filter events before they reach the scope

`kind` cannot drop an event. To drop noise such as heartbeats, wrap the source and keep its key:

```ts
import type { Event, Source } from 'attend';

export const only = (source: Source, keep: (event: Event) => boolean): Source => ({
  key: source.key,
  subscribe: (cursor, emit, signal) => source.subscribe(cursor, (batch) => emit({ ...batch, events: batch.events.filter(keep) }), signal),
});
```

The batch keeps its cursor, so dropped events never come back.

## Test against an in-memory server

A poll-only server over `InMemoryTransport` keeps a source test deterministic on a fake clock:

```ts
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import type { ServerCapabilities } from '@modelcontextprotocol/sdk/types.js';
import { z } from 'zod/v4';

export type Occurrence = { eventId: string; name: string; timestamp: string; data: Record<string, string | number | boolean> };

const ListEvents = z.object({ method: z.literal('events/list') });
const PollEvents = z.object({ method: z.literal('events/poll'), params: z.object({ name: z.string(), cursor: z.string().nullable() }) });

/** A client connected to a server that serves `occurrences` from one poll event. Push to the array to publish more. */
export async function pollServerClient(event: string, occurrences: Occurrence[]): Promise<Client> {
  const capabilities: ServerCapabilities & { events: { listChanged: boolean } } = { events: { listChanged: true } };
  const server = new Server({ name: 'test-events', version: '0.0.0' }, { capabilities });
  server.setRequestHandler(ListEvents, () => ({ events: [{ name: event, delivery: ['poll'], inputSchema: { type: 'object' }, payloadSchema: { type: 'object' } }] }));
  server.setRequestHandler(PollEvents, (request) => {
    const from = Number(request.params.cursor ?? 0);
    return { events: occurrences.slice(from), cursor: String(occurrences.length), truncated: false, hasMore: false, nextPollMs: 1000 };
  });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  const client = new Client({ name: 'test-agent', version: '0.0.0' });
  await client.connect(clientTransport);
  return client;
}
```

Attach `mcpEventsSource(client, { name, clock })` with the same fake clock as `createAttend`. The client and the server talk through promise callbacks, so call `drain()` from `attend/testing` about four times after you attach, and again after each poll interval you advance, before you assert. Then advance past the settle deadline.

## Typing note

With `exactOptionalPropertyTypes` on, SDK 1.32's `StreamableHTTPClientTransport` does not typecheck as a `Transport`, because its `sessionId` getter can return `undefined`. `StdioClientTransport` and `InMemoryTransport` typecheck.

## Serving decisions: attend serve

`npx attend serve` runs attend as a sidecar. Events go in as NDJSON on `POST /v1/ingest`, and judged decisions come out on `/mcp` as MCP Events. Raw events never cross MCP.

```sh
npm install attend @modelcontextprotocol/server zod
npx attend serve --store sqlite:attend.db --judge jev --kinds kinds.json --act-threshold 0.4
```

`kinds.json` maps event names to kinds, such as `{"message.created":"flush","build.failed":"closing"}`. Flags and their environment variables are in the README. Subscribe to `attention.decision` with `events/stream`, poll it with `events/poll`, or call the `attend_wait` tool in a loop from a host that cannot subscribe. In TypeScript, `decisionFeed` from `attend/mcp` keeps the cursor. Use `serve(attend, store, { listen })` from `attend/server` to run the same thing inside your own process.
