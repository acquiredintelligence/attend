import type { JsonObject } from '../core/types.js';
import { EventsError, eventsError, occurrenceFor } from './events.js';
import { pollEvents } from './poll.js';
import type { Principal, ServerContext } from './types.js';

/** An MCP tool offered by the event server. */
export interface ToolDefinition {
  name: string;
  description: string;
  inputSchema: JsonObject;
}

/** Text and structured representations of a tool result. */
export interface ToolResult {
  content: { type: 'text'; text: string }[];
  structuredContent?: JsonObject;
  isError?: boolean;
}

/** attend_wait and attend_window. */
export function listTools(): ToolDefinition[] {
  return [
    { name: 'attend_wait', description: 'Wait for routed decisions or dead letters after your saved cursor, then persist the returned cursor.', inputSchema: {
      type: 'object', additionalProperties: false, properties: { name: { type: 'string', default: 'attention.decision' }, arguments: { type: 'object' }, cursor: { type: ['string', 'null'] }, waitMs: { type: 'integer', minimum: 0, maximum: 30000, default: 30000 }, maxEvents: { type: 'integer', minimum: 1, maximum: 20, default: 20 } },
    } },
    { name: 'attend_window', description: 'Read the full event window for a decision you are allowed to see.', inputSchema: { type: 'object', additionalProperties: false, required: ['decisionId'], properties: { decisionId: { type: 'string' } } } },
  ];
}

/** Calls a tool, preserving application errors as structured tool results. */
export async function callTool(context: ServerContext, name: unknown, args: unknown, principal: Principal, signal: AbortSignal): Promise<ToolResult> {
  if (name !== 'attend_wait' && name !== 'attend_window') throw eventsError('InvalidParams', 'Unknown tool', { field: 'name', reason: 'unknown tool' });
  try {
    const value = args === undefined ? {} : args;
    if (typeof value !== 'object' || value === null || (Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null)) throw eventsError('InvalidParams', 'Invalid arguments', { field: 'arguments', reason: 'must be an object' });
    const values = value as Record<string, unknown>;
    const allowed = name === 'attend_wait' ? ['name', 'arguments', 'cursor', 'maxEvents', 'waitMs'] : ['decisionId'];
    for (const key of Object.keys(values)) if (!allowed.includes(key)) throw eventsError('InvalidParams', 'Unknown argument', { field: `arguments.${key}`, reason: 'unknown' });
    let structuredContent: JsonObject;
    if (name === 'attend_wait') {
      const cap = (key: string, fallback: number, minimum: number) => {
        const value = values[key];
        if (value === undefined) return fallback;
        if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < minimum) throw eventsError('InvalidParams', `Invalid ${key}`, { field: key, reason: `must be a safe integer >= ${minimum}` });
        return Math.min(value, fallback);
      };
      const result = await pollEvents(context, { ...values, name: values.name ?? 'attention.decision', maxEvents: cap('maxEvents', 20, 1), waitMs: cap('waitMs', 30_000, 0) }, principal, signal);
      structuredContent = { events: result.events.map((event) => ({ ...event })), cursor: result.cursor, truncated: result.truncated };
    } else {
      if (typeof values.decisionId !== 'string') throw eventsError('InvalidParams', 'Invalid decisionId', { field: 'decisionId', reason: 'required' });
      const row = await context.log.find('decision', values.decisionId);
      if (!row || !principal.scopes.some((grant) => row.scope.startsWith(grant))) throw eventsError('NotFound', 'Decision not found', { kind: 'decision' });
      const occurrence = occurrenceFor(row, 'events', context.options.payload?.maxEventsBytes);
      structuredContent = { ...occurrence.data, ...(occurrence._meta?.['attend/eventsOmitted'] ? { eventsOmitted: true } : {}) } as unknown as JsonObject;
    }
    return { content: [{ type: 'text', text: JSON.stringify(structuredContent) }], structuredContent };
  } catch (error) {
    if (!(error instanceof EventsError)) throw error;
    return { content: [{ type: 'text', text: error.message }], structuredContent: { error: { ...error.toJSON() } }, isError: true };
  }
}
