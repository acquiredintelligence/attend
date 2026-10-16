import type { IncomingMessage, ServerResponse } from 'node:http';
import type { Attend } from '../shell/types.js';
import type { DecisionLog, Position } from '../store/types.js';
import type { DeliveredRoute, Entry, JsonObject, Route, SettleReason } from '../core/types.js';

export const EVENTS_EXTENSION = 'io.modelcontextprotocol/events';
export const SUBSCRIPTION_ID_META = 'io.modelcontextprotocol/subscriptionId';
export const DECISION_EVENT = 'attention.decision';
export const DEAD_LETTER_EVENT = 'attention.deadLetter';
export type AttendEventName = typeof DECISION_EVENT | typeof DEAD_LETTER_EVENT;
export type DeliveryMode = 'poll' | 'push';

export interface EventDescriptor {
  name: string;
  description: string;
  delivery: DeliveryMode[];
  inputSchema: JsonObject;
  payloadSchema: JsonObject;
  _meta?: JsonObject;
}

/** Every result this extension defines carries `resultType: "complete"`. */
export interface CompleteResult {
  resultType: 'complete';
}

export interface ListEventsParams {
  cursor?: string;
}

export interface ListEventsResult extends CompleteResult {
  events: EventDescriptor[];
  nextCursor?: string;
}

/** An absent `cursor` means null, in both directions. */
export interface EventOccurrence<D extends object = JsonObject> {
  eventId: string;
  name: string;
  timestamp: string;
  data: D;
  cursor?: string | null;
  _meta?: JsonObject;
}

/** `attention.decision` arguments. The schema starts strict and only loosens. `scope` and `scopePrefix` are exclusive. */
export interface DecisionArguments {
  scope?: string;
  scopePrefix?: string;
  routes?: DeliveredRoute[];
  minProbability?: number;
  include?: 'notice' | 'events';
}

export interface DeadLetterArguments {
  scope?: string;
  scopePrefix?: string;
  stage?: 'judge' | 'sink';
}

/** `attention.decision` data: triage fields only. The full window is behind `include: "events"` or the `attend_window` tool. */
export interface AttentionDecisionData {
  decisionId: string;
  scope: string;
  route: DeliveredRoute;
  probabilities: Record<Route, number>;
  predicates: Record<string, number>;
  confidence: number | null;
  reason: SettleReason;
  settledAt: string;
  decidedAt: string;
  eventCount: number;
  eventIds: string[];
  dropped: number;
  gap: boolean;
  waits: number;
  /** `defaultNotice(decision)`. */
  notice: string;
  /** With `include: "events"` only, capped at 1 MiB; past the cap it is omitted and `_meta["attend/eventsOmitted"]` is set. */
  events?: Entry[];
}

export interface AttentionDeadLetterData {
  letterId: string;
  scope: string;
  stage: 'judge' | 'sink';
  message: string;
  permanent: boolean;
  attempts: number;
  decisionId: string | null;
  route: DeliveredRoute | null;
  reason: SettleReason;
  eventCount: number;
  eventIds: string[];
  at: string;
}

/** `waitMs` (a long poll) is accepted, not advertised. */
export interface PollParams {
  name: string;
  arguments?: JsonObject;
  cursor?: string | null;
  maxAgeMs?: number;
  maxEvents?: number;
  waitMs?: number;
}

export interface PollResult extends CompleteResult {
  events: EventOccurrence[];
  cursor: string | null;
  truncated: boolean;
  hasMore: boolean;
  nextPollMs: number;
}

export interface StreamParams {
  name: string;
  arguments?: JsonObject;
  cursor?: string | null;
  maxAgeMs?: number;
}

/** Carried on every `notifications/events/*` message: the parent `events/stream` request's JSON-RPC id. */
export interface SubscriptionMeta {
  'io.modelcontextprotocol/subscriptionId': string | number;
}

export interface EventsErrorObject {
  code: number;
  message: string;
  data?: JsonObject;
}

/** `active` always comes first. Heartbeats come at least every 30 s. */
export type StreamNotification =
  | { method: 'notifications/events/active'; params: { cursor: string | null; truncated: boolean; _meta: SubscriptionMeta } }
  | { method: 'notifications/events/event'; params: EventOccurrence & { _meta: SubscriptionMeta & JsonObject } }
  | { method: 'notifications/events/heartbeat'; params: { cursor: string | null; _meta: SubscriptionMeta } }
  | { method: 'notifications/events/error'; params: { error: EventsErrorObject; _meta: SubscriptionMeta } }
  | { method: 'notifications/events/terminated'; params: { error: EventsErrorObject; _meta: SubscriptionMeta } };

/** The final frame, sent only when the server closes the stream. */
export interface StreamEventsResult extends CompleteResult {
  _meta: SubscriptionMeta;
}

/** `v1.<epoch>.<position>` cursors: opaque to clients, one sequence per store. */
export interface CursorParts {
  epoch: string;
  position: Position;
}

/** Compiled subscription arguments. Matching is O(1) once the fan-out index has found the candidate. */
export interface SubscriptionFilter {
  name: AttendEventName;
  scope?: string;
  scopePrefix?: string;
  routes: number;
  minProbability: number;
  stage?: 'judge' | 'sink';
  include: 'notice' | 'events';
}

/** `scopes` are granted prefixes; `['']` grants every scope. */
export interface Principal {
  id: string;
  scopes: readonly string[];
}

export type PrincipalResolver = (req: IncomingMessage) => Promise<Principal | null> | Principal | null;

export interface EventsServerOptions {
  name?: string;
  version?: string;
  /** stdio and InMemory transports use `localPrincipal`; HTTP uses `principal(req)`. */
  principal?: PrincipalResolver;
  localPrincipal?: Principal;
  /** Default 15,000; a heartbeat must come at least every 30 s. */
  heartbeatMs?: number;
  /** Bytes of encoded frames one stream may buffer before it falls back to catch-up. Default 1 MiB. */
  streamBufferBytes?: number;
  /** A ResourceExhausted {limit: "streams"} quota per principal, not a concurrency cap. Default 256. */
  maxStreamsPerPrincipal?: number;
  poll?: { defaultMaxEvents?: number; maxEvents?: number; nextPollMs?: number };
  /** Registers `attend_wait` and `attend_window`. Default true. */
  tools?: boolean;
  payload?: { maxEventsBytes?: number };
}

/** The slice of the MCP SDK's request `extra` the handlers use. */
export interface McpRequestExtraLike {
  requestId: string | number;
  signal: AbortSignal;
  sendNotification(notification: { method: string; params?: object }): Promise<void>;
}

/** The slice of an SDK `Server` that `install` uses: zod-free registration through the fallback handler. */
export interface McpServerLike {
  fallbackRequestHandler?: (request: { method: string; params?: unknown }, extra: McpRequestExtraLike) => Promise<unknown>;
}

export type HttpRoute = (req: IncomingMessage, res: ServerResponse) => void | Promise<void>;

export interface HttpHandlerOptions {
  /** Default '/mcp'. */
  path?: string;
  /** Extra routes on the same port, such as 'POST /v1/ingest'. */
  routes?: Record<string, HttpRoute>;
  /** Defaults: 10,000 sessions, 1,800,000 ms idle. */
  sessions?: { max?: number; idleMs?: number };
  /** Builds the SDK Server for a new session, for an app that wants its own tools on the same endpoint. `install` is applied to it. */
  newServer?: () => McpServerLike & { connect(transport: unknown): Promise<void> };
}

export interface EventsServer {
  readonly descriptors: readonly EventDescriptor[];
  /** For `server/discover` and `initialize`. */
  capabilities(): { extensions: Record<string, JsonObject> };
  /** Registers events/list, events/poll, events/stream (every send awaited) and the tools on an SDK Server, for stdio and InMemory. */
  install(server: McpServerLike, options?: { principal?: Principal }): void;
  /**
   * The Streamable HTTP endpoint. `events/stream` is served by attend's own SSE writer: drain on
   * write() === false, abort on 'close', a setImmediate yield every 64 frames, catch-up instead of
   * drop. A guard first checks session, MCP-Protocol-Version, Mcp-Method, Accept and the principal.
   * Every other method goes to the SDK transport.
   */
  http(options: HttpHandlerOptions): HttpRoute;
  close(): Promise<void>;
}

/** Server-internal: what every events/* handler reads, built once by `eventsServer`. */
export interface ServerContext {
  readonly log: DecisionLog;
  readonly options: EventsServerOptions;
  readonly descriptors: readonly EventDescriptor[];
}

/** NDJSON lines. */
export type IngestLine =
  | { scope: string; id: string; at: number; name: string; data: JsonObject; text?: string }
  | { scope: string; flush: true }
  | { scope: string; end: true };

export interface IngestResult {
  accepted: number;
  duplicates: number;
  rejected: { line: number; scope: string | null; error: 'invalid' | 'outbox_full' | 'backpressure' | 'too_large' | 'forbidden' | 'conflict' }[];
}

export interface NdjsonIngestOptions {
  /** True: a long-lived NDJSON body, answered with {ack, through} and {credit} lines. False: one request, one IngestResult. */
  stream?: boolean;
  maxBodyBytes?: number;
  maxLineBytes?: number;
  /** Lines are accepted only for scopes inside the principal's grants. */
  principal?: PrincipalResolver;
  /** Default 64 lines parsed per macrotask. */
  slice?: number;
}

export interface ServeOptions {
  listen: { port: number; host?: string } | { path: string };
  principal?: PrincipalResolver;
  events?: Omit<EventsServerOptions, 'principal'>;
  ingest?: { stream?: boolean };
}
