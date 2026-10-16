/**
 * `attend/server`: MCP Events over a store's decision log, with polling, streaming and
 * Streamable HTTP, plus NDJSON ingestion and a combined server. A read-only process can
 * serve decisions without running a judge.
 */
export { eventsServer } from './server/install.js';
export { ndjsonIngest } from './server/ingest.js';
export { serve } from './server/serve.js';
export { DEAD_LETTER_EVENT, DECISION_EVENT, EVENTS_EXTENSION } from './server/types.js';
export type { AttentionDeadLetterData, AttentionDecisionData, DeadLetterArguments, DecisionArguments, EventOccurrence, EventsServer, EventsServerOptions, HttpHandlerOptions, HttpRoute, IngestLine, IngestResult, NdjsonIngestOptions, Principal, PrincipalResolver, ServeOptions } from './server/types.js';
