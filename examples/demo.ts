/**
 * A CI server speaks MCP Events over an in-memory transport, and attend decides what the agent
 * hears. Run it with `pnpm demo`. It needs no API key and no database.
 */
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import type { ServerCapabilities } from '@modelcontextprotocol/sdk/types.js';
import { z } from 'zod/v4';
import { createAttend, defaultNotice, memoryStore, routes, type Decision, type Event, type Judge, type Kind, type Trace } from 'attend';
import { mcpEventsSource } from 'attend/mcp';

const SCOPE = 'repo:acme/shop';
const EVENT = 'repo.activity';

/** What the server emits, oldest first, with the pause before each event. */
const STORY: { pauseMs: number; name: string; data: Record<string, string | number> }[] = [
  { pauseMs: 0, name: 'ci.started', data: { run: 4521, branch: 'main' } },
  { pauseMs: 100, name: 'ci.log', data: { line: 'pnpm install' } },
  { pauseMs: 100, name: 'ci.log', data: { line: 'pnpm test' } },
  { pauseMs: 100, name: 'ci.log', data: { line: '211 passed, 1 failed' } },
  { pauseMs: 100, name: 'ci.test_failed', data: { test: 'checkout applies the coupon' } },
  { pauseMs: 600, name: 'comment.created', data: { author: 'sam', body: 'can you look?' } },
  { pauseMs: 800, name: 'deploy.finished', data: { env: 'staging', sha: '9f3c2e1' } },
];

const KIND_BY_NAME: Record<string, Kind> = {
  'comment.created': 'flush',
  'ci.test_failed': 'closing',
  'deploy.finished': 'closing',
};

const kind = (event: Event): Kind => KIND_BY_NAME[event.name] ?? 'consequence';

/** A direct comment interrupts. A failure waits for the user to react, then queues. A deploy is background. */
const judge: Judge = async ({ window, routes: offered }) => {
  const has = (name: string) => window.events.some((e) => e.name === name);
  if (window.reason === 'flush') return { route: 'interrupt' };
  if (has('ci.test_failed')) return { route: offered.some((o) => o.route === 'wait') ? 'wait' : 'queue' };
  if (has('deploy.finished')) return { route: 'note' };
  return { route: 'skip' };
};

const started = Date.now();
const pause = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function say(label: string, detail: string): void {
  const elapsed = `${((Date.now() - started) / 1000).toFixed(1)}s`;
  console.log(`${elapsed.padStart(5)}  ${label.padEnd(8)} ${detail}`);
}

function show(trace: Trace): void {
  switch (trace.kind) {
    case 'settled':
      return say('settle', `${trace.count} ${trace.count === 1 ? 'event' : 'events'} (${trace.reason})${trace.merged ? ', merged with the held window' : ''}`);
    case 'waiting':
      return say('judge', 'wait (hold the window and judge it again)');
    case 'judged':
      return say('judge', trace.route);
    default:
      return;
  }
}

const shown = new Set<string>();

function tell(decision: Decision): void {
  if (shown.has(decision.id)) return;
  shown.add(decision.id);
  say('deliver', decision.route);
  for (const line of defaultNotice(decision).split('\n')) console.log(`${' '.repeat(16)}${line}`);
}

const SUBSCRIPTION = 'io.modelcontextprotocol/subscriptionId';
const ListEvents = z.object({ method: z.literal('events/list') });
const StreamEvents = z.object({ method: z.literal('events/stream'), params: z.object({ name: z.string(), cursor: z.string().nullable() }) });

/** A minimal MCP Events server: one push event, streamed from the request's cursor. */
function ciServer(): Server {
  const capabilities: ServerCapabilities & { events: { listChanged: boolean } } = { events: { listChanged: true } };
  const server = new Server({ name: 'ci', version: '1.0.0' }, { capabilities });
  server.setRequestHandler(ListEvents, () => ({
    events: [{ name: EVENT, description: 'CI runs, comments and deploys on one repo', delivery: ['push'], inputSchema: { type: 'object' }, payloadSchema: { type: 'object' } }],
  }));
  server.setRequestHandler(StreamEvents, async (request, extra) => {
    const _meta = { [SUBSCRIPTION]: extra.requestId };
    const send = (kind: string, params: Record<string, unknown>) => extra.sendNotification({ method: `notifications/events/${kind}`, params: { ...params, _meta } });
    const from = Number(request.params.cursor ?? 0);
    await send('active', { cursor: String(from), truncated: false });
    for (const [i, step] of STORY.entries()) {
      if (i < from) continue;
      await pause(step.pauseMs);
      if (extra.signal.aborted) break;
      say('event', `${step.name} ${JSON.stringify(step.data)}`);
      await send('event', { eventId: `evt-${i + 1}`, name: step.name, timestamp: new Date().toISOString(), data: step.data, cursor: String(i + 1) });
    }
    if (!extra.signal.aborted) await new Promise((resolve) => extra.signal.addEventListener('abort', resolve, { once: true }));
    return { _meta: {} };
  });
  return server;
}

console.log('attend demo: a CI server streams MCP Events, and attend decides what the agent hears.\n');

const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
await ciServer().connect(serverTransport);
const client = new Client({ name: 'agent', version: '1.0.0' });
await client.connect(clientTransport);

const attend = createAttend({
  store: memoryStore(),
  kind,
  judge,
  sink: routes({ interrupt: tell, queue: tell, note: tell, ask: tell }),
  deadLetter: (letter) => console.error(`dead letter ${letter.id}: ${letter.message}`),
  settle: { closingGraceMs: 200, waitHorizonMs: 4000 },
  observe: show,
});

const controller = new AbortController();
const running = attend.run({ signal: controller.signal, sweepMs: 100 });
const attached = attend.attach(SCOPE, mcpEventsSource(client, { name: EVENT }), { signal: controller.signal });

/** Every event ingested and nothing left to settle, judge or deliver. */
async function quiet(): Promise<void> {
  for (;;) {
    const record = await attend.inspect(SCOPE);
    if (record !== null && record.seen.length === STORY.length && record.dueAt === null) return;
    await pause(50);
  }
}

await Promise.race([attached, quiet()]);
controller.abort();
await Promise.all([running, attached]);
await client.close();

console.log(`\n${STORY.length} events in, ${shown.size} deliveries out. Waking on every event would have run the agent ${STORY.length} times.`);
