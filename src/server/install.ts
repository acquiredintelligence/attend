import type { JsonObject } from '../core/types.js';
import type { DecisionLog } from '../store/types.js';
import { decisionDescriptor, deadLetterDescriptor, eventsError } from './events.js';
import { createFanout, type Fanout } from './fanout.js';
import { httpHandler } from './http.js';
import { pollEvents } from './poll.js';
import { streamEvents } from './stream.js';
import { callTool, listTools } from './tools.js';
import { EVENTS_EXTENSION, type EventsServer, type EventsServerOptions, type ServerContext, type McpRequestLike, type McpRequestContextLike, type McpToolSchemaLike } from './types.js';

type Handler = (request: McpRequestLike, ctx: McpRequestContextLike) => Promise<{ [key: string]: unknown }>;

function jsonSchema(schema: JsonObject): McpToolSchemaLike {
  return { '~standard': { version: 1, vendor: 'attend', validate: (value) => ({ value }), jsonSchema: { input: () => schema, output: () => schema } } };
}

/** Takes a DecisionLog so a read-only process can serve subscribers without judging. */
export function eventsServer(log: DecisionLog, options: EventsServerOptions = {}): EventsServer {
  const descriptors = [decisionDescriptor(), deadLetterDescriptor()];
  const controller = new AbortController();
  let fanout: Fanout | undefined;
  let epoch: string | undefined;
  const context: ServerContext = { log, options, descriptors, async epoch() { return (epoch ??= await log.epoch()); }, signal: controller.signal, get fanout() { return (fanout ??= createFanout(log)); } };
  const result: EventsServer = {
    descriptors,
    capabilities: () => ({ extensions: { [EVENTS_EXTENSION]: {} } }),
    install(server, installOptions) {
      const registry = 'registerTool' in server;
      const low = registry ? server.server : server;
      low.registerCapabilities({ ...result.capabilities(), ...(!registry && options.tools !== false ? { tools: {} } : {}) });
      const previous = low.fallbackRequestHandler?.bind(low);
      const principal = installOptions?.principal ?? options.localPrincipal ?? { id: 'local', scopes: [''] };
      const toolCall: Handler = async (request, ctx) => ({ resultType: 'complete', ...await callTool(context, request.params?.name, request.params?.arguments, principal, ctx.mcpReq.signal) });
      if (options.tools !== false) {
        if (registry) {
          for (const tool of listTools()) server.registerTool(tool.name, { description: tool.description, inputSchema: jsonSchema(tool.inputSchema) }, (args, ctx) => callTool(context, tool.name, args, principal, ctx.mcpReq.signal));
        } else {
          const listing = (app: Handler): Handler => async (request, ctx) => {
            const listed = await app(request, ctx);
            return request.params?.cursor === undefined ? { ...listed, tools: [...(listed.tools as unknown[]), ...listTools()] } : listed;
          };
          const calling = (app: Handler): Handler => (request, ctx) => request.params?.name === 'attend_wait' || request.params?.name === 'attend_window' ? toolCall(request, ctx) : app(request, ctx);
          const set = low.setRequestHandler.bind(low);
          for (const method of ['tools/list', 'tools/call']) {
            // The SDK has no public getter for handlers registered before install.
            const app = (low as { _getRequestHandler?(method: string): Handler | undefined })._getRequestHandler?.call(low, method);
            if (app) set(method, method === 'tools/list' ? listing(app) : calling(app));
          }
          low.setRequestHandler = (method, ...args) => {
            const [handler, ...rest] = args;
            if ((method === 'tools/list' || method === 'tools/call') && typeof handler === 'function') {
              set(method, method === 'tools/list' ? listing(handler as Handler) : calling(handler as Handler), ...rest);
            } else set(method, ...args);
          };
        }
      }
      low.fallbackRequestHandler = async (request, ctx) => {
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
            if (!registry && options.tools !== false) return { resultType: 'complete', tools: listTools() };
            break;
          case 'tools/call':
            if (!registry && options.tools !== false) return toolCall(request, ctx);
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
