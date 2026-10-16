import { defaultNotice } from '../attend.js';
import type { DeliveredRoute, JsonObject } from '../core/types.js';
import type { DecisionQuery, DecisionRow } from '../store/types.js';
import { DECISION_EVENT, DEAD_LETTER_EVENT } from './types.js';
import type { AttentionDeadLetterData, AttentionDecisionData, EventDescriptor, EventOccurrence, EventsErrorObject, Principal, SubscriptionFilter } from './types.js';

/** SEP-3415 error codes. */
export const ERROR_CODES = { InvalidParams: -32602, NotFound: -32023, Forbidden: -32024, ResourceExhausted: -32025, Unsupported: -32026, Transient: -32603 } as const;
/** A named SEP-3415 error. */
export type EventsErrorName = keyof typeof ERROR_CODES;

/** A JSON-RPC error the SDK sends verbatim: the SEP code, a message and typed data. */
export class EventsError extends Error implements EventsErrorObject {
  readonly code: number;
  readonly data?: JsonObject;

  constructor(name: EventsErrorName, message: string, data?: JsonObject) {
    super(message);
    this.name = 'EventsError';
    this.code = ERROR_CODES[name];
    if (data !== undefined) this.data = data;
  }

  toJSON(): EventsErrorObject {
    return { code: this.code, message: this.message, ...(this.data === undefined ? {} : { data: this.data }) };
  }
}

/** Makes an error the SDK can send without translation. */
export function eventsError(name: EventsErrorName, message: string, data?: JsonObject): EventsError {
  return new EventsError(name, message, data);
}

const routes: DeliveredRoute[] = ['interrupt', 'queue', 'note', 'ask'];
const bits = { interrupt: 1, queue: 2, note: 4, ask: 8 };
const reasons = ['closing', 'flush', 'stream_quiet', 'passive_quiet', 'overflow', 'age', 'forced', 'size'];
const scopeProperties = {
  scope: { type: 'string', description: 'Exactly this scope.' },
  scopePrefix: { type: 'string', description: 'Every scope that starts with this. Omit both scope and scopePrefix for every scope the principal may read.' },
};

/** Describes settled decisions delivered to an agent. */
export function decisionDescriptor(): EventDescriptor {
  return {
    name: DECISION_EVENT,
    description: 'A settled window of events that a judge routed to the agent: interrupt, queue, note or ask. One occurrence per decision; skip is never emitted.',
    delivery: ['poll', 'push'],
    inputSchema: {
      type: 'object', additionalProperties: false,
      properties: { ...scopeProperties, routes: { type: 'array', items: { enum: [...routes] }, uniqueItems: true }, minProbability: { type: 'number', minimum: 0, maximum: 1 }, include: { enum: ['notice', 'events'], default: 'notice' } },
      not: { required: ['scope', 'scopePrefix'] },
    },
    payloadSchema: {
      type: 'object',
      required: ['decisionId', 'scope', 'route', 'probabilities', 'predicates', 'confidence', 'reason', 'settledAt', 'decidedAt', 'eventCount', 'eventIds', 'dropped', 'gap', 'waits', 'notice'],
      properties: {
        decisionId: { type: 'string' }, scope: { type: 'string' }, route: { enum: [...routes] },
        probabilities: { type: 'object', additionalProperties: { type: 'number' } }, predicates: { type: 'object', additionalProperties: { type: 'number' } },
        confidence: { type: ['number', 'null'] }, reason: { enum: [...reasons] },
        settledAt: { type: 'string', format: 'date-time' }, decidedAt: { type: 'string', format: 'date-time' },
        eventCount: { type: 'integer' }, eventIds: { type: 'array', items: { type: 'string' } }, dropped: { type: 'integer' }, gap: { type: 'boolean' }, waits: { type: 'integer' }, notice: { type: 'string' }, events: { type: 'array', items: { type: 'object' } },
      },
    },
  };
}

/** Describes windows whose judge or sink gave up. */
export function deadLetterDescriptor(): EventDescriptor {
  return {
    name: DEAD_LETTER_EVENT,
    description: 'A window no judge or sink could take: the judge gave up (stage judge) or the sink did (stage sink).',
    delivery: ['poll', 'push'],
    inputSchema: { type: 'object', additionalProperties: false, properties: { ...scopeProperties, stage: { enum: ['judge', 'sink'] } }, not: { required: ['scope', 'scopePrefix'] } },
    payloadSchema: {
      type: 'object', required: ['letterId', 'scope', 'stage', 'message', 'permanent', 'attempts', 'decisionId', 'route', 'reason', 'eventCount', 'eventIds', 'at'],
      properties: {
        letterId: { type: 'string' }, scope: { type: 'string' }, stage: { enum: ['judge', 'sink'] }, message: { type: 'string' }, permanent: { type: 'boolean' }, attempts: { type: 'integer' }, decisionId: { type: ['string', 'null'] }, route: { enum: [...routes, null] }, reason: { enum: [...reasons] }, eventCount: { type: 'integer' }, eventIds: { type: 'array', items: { type: 'string' } }, at: { type: 'string', format: 'date-time' },
      },
    },
  };
}

function object(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null);
}

function invalid(field: string, reason: string): never {
  throw eventsError('InvalidParams', `Invalid ${field}: ${reason}`, { field, reason });
}

function delivered(value: unknown): value is DeliveredRoute {
  return value === 'interrupt' || value === 'queue' || value === 'note' || value === 'ask';
}

/** Validates the event arguments and applies the principal's granted prefixes. */
export function compileFilter(name: string, args: unknown, principal: Principal): SubscriptionFilter {
  if (name !== DECISION_EVENT && name !== DEAD_LETTER_EVENT) throw eventsError('NotFound', 'Unknown event', { kind: 'event' });
  const value = args === undefined ? {} : args;
  if (!object(value)) invalid('arguments', 'must be an object');
  const allowed = name === DECISION_EVENT ? ['scope', 'scopePrefix', 'routes', 'minProbability', 'include'] : ['scope', 'scopePrefix', 'stage'];
  for (const key of Object.keys(value)) if (!allowed.includes(key)) invalid(`arguments.${key}`, 'unknown');
  for (const key of ['scope', 'scopePrefix']) if (key in value && typeof value[key] !== 'string') invalid(`arguments.${key}`, 'must be a string');
  if ('scope' in value && 'scopePrefix' in value) invalid('arguments', 'scope and scopePrefix are exclusive');
  let mask = 15;
  if ('routes' in value) {
    if (!Array.isArray(value.routes) || !value.routes.every(delivered) || new Set(value.routes).size !== value.routes.length) invalid('arguments.routes', 'must be unique delivered routes');
    mask = value.routes.reduce((sum, route: DeliveredRoute) => sum | bits[route], 0);
  }
  if ('minProbability' in value && (typeof value.minProbability !== 'number' || !Number.isFinite(value.minProbability) || value.minProbability < 0 || value.minProbability > 1)) invalid('arguments.minProbability', 'must be a finite number between 0 and 1');
  if ('include' in value && value.include !== 'notice' && value.include !== 'events') invalid('arguments.include', 'must be notice or events');
  if ('stage' in value && value.stage !== 'judge' && value.stage !== 'sink') invalid('arguments.stage', 'must be judge or sink');
  const filter: SubscriptionFilter = { name, routes: mask, minProbability: typeof value.minProbability === 'number' ? value.minProbability : 0, include: value.include === 'events' ? 'events' : 'notice' };
  if (typeof value.scope === 'string') filter.scope = value.scope;
  if (typeof value.scopePrefix === 'string') filter.scopePrefix = value.scopePrefix;
  if (value.stage === 'judge' || value.stage === 'sink') filter.stage = value.stage;
  const requested = filter.scope ?? filter.scopePrefix;
  if (principal.scopes.length === 0 || (requested !== undefined && !principal.scopes.some((grant) => requested.startsWith(grant)))) {
    throw eventsError('Forbidden', 'Scope is outside the principal grants', { reason: 'scope is outside granted prefixes' });
  }
  if (requested === undefined && !principal.scopes.includes('')) {
    if (principal.scopes.length === 1) filter.scopePrefix = principal.scopes[0]!;
    else filter.grants = [...principal.scopes];
  }
  return filter;
}

/** Checks the row against every subscription constraint. */
export function matches(filter: SubscriptionFilter, row: DecisionRow): boolean {
  if ((row.kind === 'decision' ? DECISION_EVENT : DEAD_LETTER_EVENT) !== filter.name) return false;
  if (filter.scope !== undefined && row.scope !== filter.scope) return false;
  if (filter.scopePrefix !== undefined && !row.scope.startsWith(filter.scopePrefix)) return false;
  if (filter.grants && !filter.grants.some((grant) => row.scope.startsWith(grant))) return false;
  if (row.kind === 'deadLetter') return filter.stage === undefined || row.body.stage === filter.stage;
  return row.route !== null && (filter.routes & bits[row.body.route]) !== 0 && row.body.probabilities[row.body.route] >= filter.minProbability;
}

/** The store-side part of a filter, for `DecisionLog.read`. */
export function queryFor(filter: SubscriptionFilter): Pick<DecisionQuery, 'scope' | 'scopePrefix' | 'kinds' | 'routes'> {
  return {
    kinds: [filter.name === DECISION_EVENT ? 'decision' : 'deadLetter'],
    ...(filter.scope === undefined ? {} : { scope: filter.scope }),
    ...(filter.scopePrefix === undefined ? {} : { scopePrefix: filter.scopePrefix }),
    ...(filter.routes === 15 ? {} : { routes: routes.filter((route) => (filter.routes & bits[route]) !== 0) }),
  };
}

/** Parses an events/poll or events/stream request's subscription part; throws an EventsError. */
export function parseSubscription(params: unknown, principal: Principal): { filter: SubscriptionFilter; cursor: string | null; maxAgeMs: number | undefined } {
  if (!object(params)) invalid('params', 'must be an object');
  if (typeof params.name !== 'string') invalid('name', 'required');
  const filter = compileFilter(params.name, params.arguments, principal);
  if (params.cursor !== undefined && params.cursor !== null && typeof params.cursor !== 'string') invalid('cursor', 'must be a string or null');
  if (params.maxAgeMs !== undefined && (typeof params.maxAgeMs !== 'number' || !Number.isSafeInteger(params.maxAgeMs) || params.maxAgeMs < 0)) invalid('maxAgeMs', 'must be a non-negative safe integer');
  return { filter, cursor: params.cursor ?? null, maxAgeMs: params.maxAgeMs };
}

/** Builds a detached payload; large windows carry an omission marker. */
export function occurrenceFor(row: DecisionRow, include: 'notice' | 'events', maxEventsBytes = 1_048_576): EventOccurrence<AttentionDecisionData | AttentionDeadLetterData> {
  const body = row.body;
  const w = body.window;
  if (row.kind === 'deadLetter') {
    const letter = row.body;
    return { eventId: row.id, name: DEAD_LETTER_EVENT, timestamp: new Date(letter.at).toISOString(), data: {
      letterId: letter.id, scope: letter.scope, stage: letter.stage, message: letter.message, permanent: letter.permanent, attempts: letter.attempts,
      decisionId: letter.decision?.id ?? null, route: letter.decision?.route ?? null, reason: w.reason, eventCount: w.events.length, eventIds: w.events.map((e) => e.id), at: new Date(letter.at).toISOString(),
    } };
  }
  const decision = row.body;
  const data: AttentionDecisionData = {
    decisionId: decision.id, scope: decision.scope, route: decision.route, probabilities: { ...decision.probabilities }, predicates: { ...decision.predicates }, confidence: decision.confidence,
    reason: w.reason, settledAt: new Date(w.settledAt).toISOString(), decidedAt: new Date(decision.at).toISOString(), eventCount: w.events.length, eventIds: w.events.map((e) => e.id), dropped: w.dropped, gap: w.gap, waits: w.waits,
    notice: defaultNotice(decision as Parameters<typeof defaultNotice>[0]),
  };
  const occurrence: EventOccurrence<AttentionDecisionData> = { eventId: row.id, name: DECISION_EVENT, timestamp: new Date(decision.at).toISOString(), data };
  if (include === 'events') {
    const json = JSON.stringify(w.events);
    if (new TextEncoder().encode(json).length <= maxEventsBytes) data.events = JSON.parse(json);
    else occurrence._meta = { 'attend/eventsOmitted': true };
  }
  return occurrence;
}
