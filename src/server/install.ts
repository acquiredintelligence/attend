import type { DecisionLog } from '../store/types.js';
import { decisionDescriptor, deadLetterDescriptor, eventsError } from './events.js';
import { createFanout, type Fanout } from './fanout.js';
import { httpHandler } from './http.js';
import { pollEvents } from './poll.js';
import { streamEvents } from './stream.js';
import { callTool, listTools } from './tools.js';
import { EVENTS_EXTENSION, type EventsServer, type EventsServerOptions, type ServerContext } from './types.js';

/** Takes a DecisionLog so a read-only process can serve subscribers without judging. */
export function eventsServer(log: DecisionLog, options: EventsServerOptions = {}): EventsServer {
  const descriptors = [decisionDescriptor(), deadLetterDescriptor()];
  const controller = new AbortController();
  let fanout: Fanout | undefined;
  const context: ServerContext = { log, options, descriptors, signal: controller.signal, get fanout() { return (fanout ??= createFanout(log)); } };
  const result: EventsServer = {
    descriptors,
    capabilities: () => ({ extensions: { [EVENTS_EXTENSION]: {} } }),
    install(server, installOptions) {
      server.registerCapabilities({ ...result.capabilities(), ...(options.tools === false ? {} : { tools: {} }) });
      const previous = server.fallbackRequestHandler?.bind(server);
      const principal = installOptions?.principal ?? options.localPrincipal ?? { id: 'local', scopes: [''] };
      server.fallbackRequestHandler = async (request, ctx) => {
        const params = request.params ?? {};
        switch (request.method) {
          case 'events/list':
            if (params.cursor != null) throw eventsError('InvalidParams', 'Unknown cursor', { field: 'cursor', reason: 'unknown cursor' });
            return { resultType: 'complete', events: structuredClone(descriptors) };
          case 'events/poll':
            return { ...await pollEvents(context, params, principal, ctx.mcpReq.signal) };
          case 'events/stream':
            return { ...await streamEvents(context, params as unknown as Parameters<typeof streamEvents>[1], principal, ctx.mcpReq.id, (notification) => ctx.mcpReq.notify(notification as { method: string; params: { [key: string]: unknown } }), ctx.mcpReq.signal) };
          case 'tools/list':
            if (options.tools !== false) return { resultType: 'complete', tools: listTools() };
            break;
          case 'tools/call':
            if (options.tools !== false) return { resultType: 'complete', ...await callTool(context, params.name, params.arguments, principal, ctx.mcpReq.signal) };
            break;
        }
        if (previous) return previous(request, ctx);
        throw Object.assign(new Error('Method not found'), { code: -32601 });
      };
    },
    http: (httpOptions) => httpHandler(context, result.install, httpOptions),
    async close() {
      if (controller.signal.aborted) return;
      controller.abort();
      fanout?.close();
    },
  };
  return result;
}
