# Run attend as a sidecar with attend serve

This guide runs the engine as its own process, sends it events over HTTP, and subscribes an agent to its decisions over MCP Events. For every flag and the ingest wire format, see [the attend serve reference](serve-reference.md).

## Start the server

```sh
npm install @acquiredintelligence/attend @modelcontextprotocol/server zod
npx attend serve --store sqlite:attend.db --judge jev --kinds kinds.json
# attend serving http://127.0.0.1:8787  (ingest POST http://127.0.0.1:8787/v1/ingest, MCP http://127.0.0.1:8787/mcp)
```

One command, no image. The MCP server package is an optional peer of `@acquiredintelligence/attend`, so the embedded engine stays dependency-free; `attend serve` exits with an error naming it when it is missing. Every flag has an environment variable, and a flag wins.

## Send events

Send events from anything that can POST:

```sh
curl -H 'content-type: application/x-ndjson' --data-binary @events.ndjson http://127.0.0.1:8787/v1/ingest
# {"scope":"ops/db","id":"a1","at":1760570000123,"name":"alert.fired","data":{},"text":"replica lag 40s on db-2"}
# {"scope":"ops/db","end":true}
```

Each line is an event, a flush or an end. [The wire format](serve-reference.md#ingest) lists the replies.

## Subscribe an agent

An agent hears the decisions by subscribing to `attention.decision` on `/mcp`. Hosts that do not subscribe call the `attend_wait` tool in a loop. In TypeScript, `decisionFeed` from `@acquiredintelligence/attend/mcp` keeps the cursor for you. This is [`examples/serve.ts`](../examples/serve.ts), which runs the server and a subscriber in one process:

```ts
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';
import type { Transport } from '@modelcontextprotocol/sdk/shared/transport.js';
import { createAttend, memoryStore } from '@acquiredintelligence/attend';
import { decisionFeed } from '@acquiredintelligence/attend/mcp';
import { serve } from '@acquiredintelligence/attend/server';

const store = memoryStore();
const attend = createAttend({
  store,
  kind: (event) => (event.name === 'page.acked' ? 'flush' : event.name === 'alert.fired' ? 'closing' : 'consequence'),
  judge: async ({ window }) => ({ route: window.reason === 'flush' ? 'interrupt' : 'note' }),
  sink: 'log',
  deadLetter: 'log',
});

const server = serve(attend, store, { listen: { port: 0 } });
await server.ready;
const url = server.url();
console.log(`attend serving ${url}`);

const client = new Client({ name: 'pager-agent', version: '1.0.0' });
// The SDK's class types sessionId as optional, which exactOptionalPropertyTypes rejects.
await client.connect(new StreamableHTTPClientTransport(new URL(`${url}/mcp`)) as Transport);
const stop = new AbortController();
const feed = decisionFeed(client, { cursor: null, signal: stop.signal })[Symbol.asyncIterator]();
```

`serve(attend, store, options)` is the same thing as a library call: it runs `attend.run`, the events server over `store.log` and the ingest routes on one HTTP listener. Give it `principal` to authorize by scope prefix, and close it with `await server.close()`.

Subscribers wait on the store's decision log. `attend serve --store postgres:<url>` and `--store redis:<url>` each open a second connection for that wait: one that runs `LISTEN` on Postgres, and one for `XREAD BLOCK` on Redis. When you build a Postgres or Redis store for `serve()` yourself, give it one too, with `postgresStore(pool, { listener })` or `redisStore(send, { blockingSend })` as in [Choose a store](storage.md). Without it, subscribers hear each decision on the next poll, 100 ms on Postgres and 250 ms on Redis.
