# Feed attend from an MCP Events server

`mcpEventsSource` from `@acquiredintelligence/attend/mcp` turns one MCP Events subscription into a source. Install the peer dependencies with `npm install @modelcontextprotocol/sdk zod`; zod 3.25 or later and zod 4 both work. This excerpt is from [`examples/mcp-events.ts`](../examples/mcp-events.ts), with `attend` set up as in [the README's first sample](../README.md) and `controller` an `AbortController`:

```ts
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import { mcpEventsSource } from '@acquiredintelligence/attend/mcp';

const client = new Client({ name: 'my-agent', version: '1.0.0' });
await client.connect(new StdioClientTransport({ command: 'node', args: ['./ci-events-server.js'] }));

const source = mcpEventsSource(client, {
  name: 'repo.activity',
  arguments: { repo: 'acme/shop' },
  onError: (error) => console.warn('mcp events', error),
});

await Promise.all([
  attend.run({ signal: controller.signal }),
  attend.attach('repo:acme/shop', source, { signal: controller.signal }),
]);
```

The source covers these parts of the draft:

- In the default `auto` mode it reads `events/list`, streams with `events/stream` when the event lists `push` delivery, and polls with `events/poll` otherwise. A `-32026` (Unsupported) reply to a stream falls back to polling.
- Every batch carries the server's cursor, and attend commits it in the same write as the events, under the source's key (`mcp:<name>:<arguments>`). After a restart, `attach` resumes from the committed cursor.
- `eventId` becomes `Event.id`, so the scope drops redelivered events.
- `truncated: true` sets `window.gap`. The judge sees it, and `defaultNotice` says that events were lost.
- When a poll result sets `hasMore`, the source polls again at once. Otherwise it waits `nextPollMs`, or `pollMs` (default 10000) when the server sends none.
- A terminated stream reopens from the committed cursor. So does a stream with more than `maxBuffered` (default 1000) notifications waiting on a slow store, which is reported as `overflow`, and a stream that sends nothing, heartbeats included, for `idleTimeoutMs` (default 300000), which is reported as `idle`.
- A `-32025` (ResourceExhausted) reply or a request timeout is thrown as a plain error, so `attach` backs off through `retryBackoffMs` and resubscribes from the committed cursor. Any other error ends the subscription: `attach` rejects with a `PermanentError` whose `cause` is the SDK error.
- When the Client's transport closes, `attach` rejects with a `PermanentError` that says the Client closed. Reconnect the Client and attach again.
- `onError` receives each `notifications/events/error`, each payload that fails validation, each `overflow`, each `idle`, and each `unrouted` notification, one that names a subscription no stream here opened. None of them ends the subscription.
- Several streams can share one `Client`. attend installs one handler for each of the five `notifications/events/*` methods on the Client, records the JSON-RPC id of each `events/stream` request and routes notifications by `_meta["io.modelcontextprotocol/subscriptionId"]`. Do not replace those handlers.

MCP events carry no one-line summary, so `Event.text` stays unset and `defaultNotice` prints each event's name and JSON data.
