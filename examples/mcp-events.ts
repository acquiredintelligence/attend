/**
 * attend on an MCP Events server that runs as a child process over stdio. The source picks
 * `events/stream` or `events/poll` from `events/list`, and the scope's record keeps the
 * server's cursor.
 */
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import { createAttend, defaultNotice, memoryStore, routes, type Decision } from '@acquiredintelligence/attend';
import { mcpEventsSource } from '@acquiredintelligence/attend/mcp';

const client = new Client({ name: 'my-agent', version: '1.0.0' });
await client.connect(new StdioClientTransport({ command: 'node', args: ['./ci-events-server.js'] }));

const tell = (decision: Decision) => console.log(`[${decision.route}]\n${defaultNotice(decision)}`);

const attend = createAttend({
  store: memoryStore(),
  kind: (event) => (event.name === 'comment.created' ? 'flush' : event.name === 'ci.finished' ? 'closing' : 'consequence'),
  judge: async ({ window }) => ({ route: window.reason === 'flush' ? 'interrupt' : 'note' }),
  sink: routes({ interrupt: tell, queue: tell, note: tell, ask: tell }),
  deadLetter: (letter) => console.error('dead letter', letter.id, letter.message),
});

const controller = new AbortController();
process.once('SIGINT', () => controller.abort());

const source = mcpEventsSource(client, {
  name: 'repo.activity',
  arguments: { repo: 'acme/shop' },
  onError: (error) => console.warn('mcp events', error),
});

await Promise.all([
  attend.run({ signal: controller.signal }),
  attend.attach('repo:acme/shop', source, { signal: controller.signal }),
]);
await client.close();
