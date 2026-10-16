/**
 * An ops pager on `attend serve`: events go in over HTTP as NDJSON, attend settles and judges
 * them, and a client hears the decision as an MCP Events subscription. Everything runs in this
 * process so it needs no service. Run it with `pnpm build && node --experimental-strip-types examples/serve.ts`.
 */
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
const heard = (async () => {
  for (let item = await feed.next(); !item.done; item = await feed.next()) {
    if (item.value.kind === 'decision') return item.value.data;
  }
  throw new Error('feed ended');
})();
await new Promise((resolve) => setTimeout(resolve, 300));

const lines = [
  { scope: 'ops/db', id: 'a1', at: Date.now(), name: 'alert.fired', data: {}, text: 'replica lag 40s on db-2' },
  { scope: 'ops/db', id: 'a2', at: Date.now(), name: 'page.acked', data: {}, text: 'sam acked the page' },
];
const body = lines.map((line) => JSON.stringify(line)).join('\n');
const sent = await fetch(`${url}/v1/ingest`, { method: 'POST', headers: { 'content-type': 'application/x-ndjson' }, body });
console.log('ingest', await sent.json());

const decision = await heard;
console.log(`[${decision.route}] ${decision.scope}\n${decision.notice}`);
stop.abort();
await client.close();
await server.close();
