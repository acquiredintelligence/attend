import type { JsonObject } from '../core/types.js';
import type { Principal, ServerContext } from './types.js';

export interface ToolDefinition {
  name: string;
  description: string;
  inputSchema: JsonObject;
}

export interface ToolResult {
  content: { type: 'text'; text: string }[];
  structuredContent?: JsonObject;
  isError?: boolean;
}

/** attend_wait and attend_window. */
export function listTools(): ToolDefinition[] {
  throw new Error('attend: listTools is not implemented');
}

export function callTool(context: ServerContext, name: string, args: JsonObject | undefined, principal: Principal, signal: AbortSignal): Promise<ToolResult> {
  throw new Error('attend: callTool is not implemented');
}
