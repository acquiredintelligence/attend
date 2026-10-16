/**
 * A client for the MCP Events extension (SEP-3415) written from the SEP's text alone. It uses
 * `fetch`, a hand-written SSE parser and newline-delimited stdio framing, and imports nothing
 * from attend or from an MCP SDK, so a bug in attend's server cannot also be a bug here.
 *
 * Every cursor the server sends (`active`, `event`, `heartbeat`, or a poll result) is written to
 * a file before the frame is processed, and every reconnect resumes from that file.
 */
import { spawn as spawnProcess, type ChildProcess } from 'node:child_process';
import { readFileSync, writeFileSync } from 'node:fs';

const SUBSCRIPTION_ID = 'io.modelcontextprotocol/subscriptionId';
const EVENTS_EXTENSION = 'io.modelcontextprotocol/events';

export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export interface Message { jsonrpc: '2.0'; id?: number | string; method?: string; params?: Record<string, any>; result?: Record<string, any>; error?: { code: number; message: string; data?: Json } }
export interface Request { id: number | string; method: string; params: Record<string, Json> }

export interface Subscription {
  name: string;
  arguments: Record<string, Json>;
  maxAgeMs?: number;
}

export interface Received { eventId: string; cursor: string | null; timestamp: string }

/** What a run observed, in arrival order. */
export interface Ledger {
  received: Received[];
  /** Every `notifications/events/active` (push) or poll result (poll): where the server started. */
  starts: { cursor: string | null; truncated: boolean; afterEvents: number }[];
  heartbeats: number;
  /** Connections opened (push) or requests sent (poll), including the ones that were dropped. */
  connections: number;
  finals: number;
  /** Notifications whose `_meta` subscription id did not equal the request id, and other protocol faults. */
  violations: string[];
  errors: { code: number; message: string }[];
  terminated: { code: number; message: string }[];
}

export const newLedger = (): Ledger => ({ received: [], starts: [], heartbeats: 0, connections: 0, finals: 0, violations: [], errors: [], terminated: [] });

/** The persisted cursor: written before a frame is processed, read on every (re)connect. */
export class CursorFile {
  writes = 0;
  constructor(readonly path: string) {}
  read(): string | null {
    try { return (JSON.parse(readFileSync(this.path, 'utf8')) as { cursor: string | null }).cursor; } catch { return null; }
  }
  write(cursor: string | null | undefined, via: string): void {
    if (cursor === undefined || cursor === null) return;
    writeFileSync(this.path, JSON.stringify({ cursor, via }));
    this.writes++;
  }
}

export const sleep = (ms: number, signal?: AbortSignal) => new Promise<void>((resolve) => {
  const timer = setTimeout(done, ms);
  function done() { clearTimeout(timer); signal?.removeEventListener('abort', done); resolve(); }
  signal?.addEventListener('abort', done, { once: true });
});

/** Server-sent events: yields the `data` of each event. Comments and other fields are ignored. */
export async function* sseData(body: ReadableStream<Uint8Array>): AsyncGenerator<string> {
  const decoder = new TextDecoder();
  let pending = '';
  let data: string[] = [];
  let lineEnd = /\r\n|\n|\r/;
  const reader = body.getReader();
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      pending += decoder.decode(value, { stream: true });
      for (;;) {
        const match = lineEnd.exec(pending);
        // A lone CR at the very end could be the first half of CRLF: wait for more bytes.
        if (!match || (match[0] === '\r' && match.index === pending.length - 1)) break;
        const line = pending.slice(0, match.index);
        pending = pending.slice(match.index + match[0].length);
        if (line === '') {
          if (data.length > 0) { const joined = data.join('\n'); data = []; yield joined; }
        } else if (line.startsWith(':')) continue;
        else {
          const colon = line.indexOf(':');
          const field = colon < 0 ? line : line.slice(0, colon);
          let text = colon < 0 ? '' : line.slice(colon + 1);
          if (text.startsWith(' ')) text = text.slice(1);
          if (field === 'data') data.push(text);
        }
      }
    }
  } finally { reader.releaseLock(); }
}

/** Where a request goes. `open` runs a long-lived `events/stream`; `call` runs one request/response. */
export interface Link {
  open(request: Request, onMessage: (message: Message) => void, signal: AbortSignal): Promise<void>;
  call(request: Request, signal: AbortSignal): Promise<Message>;
}

const clientCapabilities = { extensions: { [EVENTS_EXTENSION]: {} } };

/** Streamable HTTP on the 2026-07-28 wire: every request stands alone, with its method in a header. */
export class HttpLink implements Link {
  constructor(readonly url: string) {}
  private fetch(request: Request, signal: AbortSignal) {
    return fetch(this.url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Accept: 'application/json, text/event-stream', 'MCP-Protocol-Version': '2026-07-28', 'Mcp-Method': request.method },
      body: JSON.stringify({ jsonrpc: '2.0', id: request.id, method: request.method, params: { ...request.params, _meta: { 'io.modelcontextprotocol/protocolVersion': '2026-07-28', 'io.modelcontextprotocol/clientCapabilities': clientCapabilities } } }),
      signal,
    });
  }
  async open(request: Request, onMessage: (message: Message) => void, signal: AbortSignal) {
    const response = await this.fetch(request, signal);
    if (!response.body) throw new Error('no body');
    if (!(response.headers.get('content-type') ?? '').startsWith('text/event-stream')) {
      onMessage(await response.json() as Message);
      return;
    }
    for await (const data of sseData(response.body)) onMessage(JSON.parse(data) as Message);
  }
  async call(request: Request, signal: AbortSignal) {
    const response = await this.fetch(request, signal);
    if (!response.body) throw new Error('no body');
    if ((response.headers.get('content-type') ?? '').startsWith('text/event-stream')) {
      for await (const data of sseData(response.body)) {
        const message = JSON.parse(data) as Message;
        if (message.id === request.id) return message;
      }
      throw new Error('stream ended without a response');
    }
    return await response.json() as Message;
  }
}

interface StdioConnection {
  child: ChildProcess;
  handlers: Map<string, { onMessage: (message: Message) => void; settle: (error?: Error) => void }>;
  ready: Promise<void>;
  exited: boolean;
}

const key = (id: number | string | undefined) => JSON.stringify(id);

/** A child process speaking newline-delimited JSON-RPC. A killed child is respawned on the next request. */
export class StdioLink implements Link {
  connection: StdioConnection | undefined;
  spawns = 0;
  private closed = false;
  constructor(private readonly spawn: () => ChildProcess, private readonly respawnMs = 150) {}

  kill(): void { this.connection?.child.kill('SIGKILL'); }

  /** Spawns the child now, so it serves before any request needs it. */
  start(): Promise<void> { return this.connect().ready.catch(() => undefined); }

  /** Kills the child and stops respawning it. */
  close(): void { this.closed = true; this.kill(); }

  private connect(): StdioConnection {
    if (this.connection && !this.connection.exited) return this.connection;
    const child = this.spawn();
    this.spawns++;
    const connection: StdioConnection = { child, handlers: new Map(), exited: false, ready: Promise.resolve() };
    this.connection = connection;
    let buffer = '';
    child.stdout!.setEncoding('utf8');
    child.stdout!.on('data', (chunk: string) => {
      buffer += chunk;
      for (let newline = buffer.indexOf('\n'); newline >= 0; newline = buffer.indexOf('\n')) {
        const line = buffer.slice(0, newline).replace(/\r$/, '');
        buffer = buffer.slice(newline + 1);
        if (line === '') continue;
        let message: Message;
        try { message = JSON.parse(line) as Message; } catch { continue; }
        const id = message.method === undefined ? message.id : message.params?._meta?.[SUBSCRIPTION_ID];
        const handler = connection.handlers.get(key(id));
        if (!handler) continue;
        handler.onMessage(message);
        if (message.method === undefined) { connection.handlers.delete(key(id)); handler.settle(); }
      }
    });
    child.on('exit', () => {
      connection.exited = true;
      if (!this.closed) setTimeout(() => { if (!this.closed && this.connection === connection) this.connect(); }, this.respawnMs);
      for (const handler of connection.handlers.values()) handler.settle(new Error('server exited'));
      connection.handlers.clear();
    });
    child.stdin!.on('error', () => undefined);
    const send = (message: unknown) => { if (!connection.exited) child.stdin!.write(JSON.stringify(message) + '\n'); };
    connection.ready = new Promise<void>((resolve, reject) => {
      connection.handlers.set(key('init'), {
        onMessage: (message) => { if (message.error) reject(new Error(message.error.message)); else { send({ jsonrpc: '2.0', method: 'notifications/initialized' }); resolve(); } },
        settle: (error) => { if (error) reject(error); },
      });
      send({ jsonrpc: '2.0', id: 'init', method: 'initialize', params: { protocolVersion: '2025-11-25', capabilities: clientCapabilities, clientInfo: { name: 'raw-client', version: '0' } } });
    });
    connection.ready.catch(() => undefined);
    return connection;
  }

  private async run(request: Request, onMessage: (message: Message) => void, signal: AbortSignal) {
    const connection = this.connect();
    await connection.ready;
    if (signal.aborted) return;
    await new Promise<void>((resolve, reject) => {
      const abort = () => {
        connection.handlers.delete(key(request.id));
        if (!connection.exited) connection.child.stdin!.write(JSON.stringify({ jsonrpc: '2.0', method: 'notifications/cancelled', params: { requestId: request.id } }) + '\n');
        resolve();
      };
      signal.addEventListener('abort', abort, { once: true });
      connection.handlers.set(key(request.id), { onMessage, settle: (error) => { signal.removeEventListener('abort', abort); if (error) reject(error); else resolve(); } });
      connection.child.stdin!.write(JSON.stringify({ jsonrpc: '2.0', id: request.id, method: request.method, params: request.params }) + '\n');
    });
  }

  open(request: Request, onMessage: (message: Message) => void, signal: AbortSignal) { return this.run(request, onMessage, signal); }
  async call(request: Request, signal: AbortSignal) {
    let response: Message | undefined;
    await this.run(request, (message) => { response = message; }, signal);
    if (!response) throw new Error('aborted');
    return response;
  }
}

export interface Control {
  /** Closes the current connection (HTTP: abort the request; stdio: `notifications/cancelled`) and reconnects at once. */
  drop(): void;
  /** Closes the connection and stays disconnected until `release`. Resolves once nothing is open. */
  hold(): Promise<void>;
  release(): void;
  /** True while a connection is open. */
  readonly connected: boolean;
}

export interface RunOptions {
  link: Link;
  subscription: Subscription;
  cursor: CursorFile;
  ledger: Ledger;
  signal: AbortSignal;
  /** Treat a push stream as dead after this long without a frame; the SEP says twice the heartbeat interval. */
  deadMs: number;
  /** Poll only: the most the loop waits between polls. The SEP's client floor is 1000 ms; tests lower this to run quickly. */
  maxWaitMs?: number;
}

function control(): { control: Control; gate(signal: AbortSignal): Promise<void>; connection(conn: AbortController | undefined): void } {
  let current: AbortController | undefined;
  let held: { promise: Promise<void>; resolve: () => void } | undefined;
  return {
    control: {
      drop() { current?.abort(); },
      async hold() {
        let resolve: () => void = () => undefined;
        held = { promise: new Promise<void>((r) => { resolve = r; }), resolve };
        current?.abort();
        while (current) await sleep(5);
      },
      release() { held?.resolve(); held = undefined; },
      get connected() { return current !== undefined; },
    },
    gate: (signal) => held ? Promise.race([held.promise, new Promise<void>((resolve) => signal.addEventListener('abort', () => resolve(), { once: true }))]) : Promise.resolve(),
    connection(conn) { current = conn; },
  };
}

function record(ledger: Ledger, file: CursorFile, requestId: number | string, message: Message): void {
  const params = message.params ?? {};
  if (params._meta?.[SUBSCRIPTION_ID] !== requestId) ledger.violations.push(`${message.method}: subscription id ${JSON.stringify(params._meta?.[SUBSCRIPTION_ID])} != ${JSON.stringify(requestId)}`);
  switch (message.method) {
    case 'notifications/events/active':
      file.write(params.cursor, 'active');
      ledger.starts.push({ cursor: params.cursor ?? null, truncated: params.truncated === true, afterEvents: ledger.received.length });
      break;
    case 'notifications/events/event':
      file.write(params.cursor, 'event');
      ledger.received.push({ eventId: params.eventId, cursor: params.cursor ?? null, timestamp: params.timestamp });
      break;
    case 'notifications/events/heartbeat':
      file.write(params.cursor, 'heartbeat');
      ledger.heartbeats++;
      break;
    case 'notifications/events/error': ledger.errors.push({ code: params.error.code, message: params.error.message }); break;
    case 'notifications/events/terminated': ledger.terminated.push({ code: params.error.code, message: params.error.message }); break;
    default: ledger.violations.push(`unexpected notification ${message.method}`);
  }
}

function requestParams(subscription: Subscription, cursor: string | null, extra: Record<string, Json> = {}): Record<string, Json> {
  return { name: subscription.name, arguments: subscription.arguments, cursor, ...(subscription.maxAgeMs === undefined ? {} : { maxAgeMs: subscription.maxAgeMs }), ...extra };
}

/** Subscribes with `events/stream` and keeps the subscription alive from the persisted cursor until `signal` aborts. */
export function pushLoop(options: RunOptions): { done: Promise<void>; control: Control } {
  const ctl = control();
  const { link, subscription, cursor, ledger, signal } = options;
  const done = (async () => {
    let attempt = 0;
    while (!signal.aborted) {
      await ctl.gate(signal);
      if (signal.aborted) break;
      attempt++;
      ledger.connections++;
      const id: number | string = attempt % 2 === 1 ? attempt : `stream-${attempt}`;
      const connection = new AbortController();
      ctl.connection(connection);
      const stop = AbortSignal.any([signal, connection.signal]);
      let last = Date.now();
      const watchdog = setInterval(() => { if (Date.now() - last > options.deadMs) connection.abort(); }, Math.max(10, options.deadMs / 4));
      try {
        await link.open({ id, method: 'events/stream', params: requestParams(subscription, cursor.read()) }, (message) => {
          last = Date.now();
          if (message.method !== undefined) record(ledger, cursor, id, message);
          else if (message.error) ledger.errors.push({ code: message.error.code, message: message.error.message });
          else {
            ledger.finals++;
            if (message.result?._meta?.[SUBSCRIPTION_ID] !== id || message.result?.resultType !== 'complete') ledger.violations.push(`bad final frame ${JSON.stringify(message.result)}`);
          }
        }, stop);
      } catch { /* dropped, refused or reset: reconnect from the persisted cursor */ }
      finally { clearInterval(watchdog); ctl.connection(undefined); }
      if (!stop.aborted) await sleep(20, signal);
    }
  })();
  return { done, control: ctl.control };
}

/** Subscribes with `events/poll`: the first poll sends `null`, every later one the persisted cursor. */
export function pollLoop(options: RunOptions): { done: Promise<void>; control: Control } {
  const ctl = control();
  const { link, subscription, cursor, ledger, signal } = options;
  const done = (async () => {
    let attempt = 0;
    while (!signal.aborted) {
      await ctl.gate(signal);
      if (signal.aborted) break;
      const connection = new AbortController();
      ctl.connection(connection);
      let wait = 20;
      try {
        const id = ++attempt;
        ledger.connections++;
        const response = await link.call({ id, method: 'events/poll', params: requestParams(subscription, cursor.read(), { maxEvents: 100 }) }, AbortSignal.any([signal, connection.signal]));
        if (response.error || !response.result) throw new Error(response.error?.message ?? 'no result');
        const result = response.result;
        if (result.resultType !== 'complete') ledger.violations.push('poll result without resultType complete');
        cursor.write(result.cursor, 'poll');
        ledger.starts.push({ cursor: result.cursor ?? null, truncated: result.truncated === true, afterEvents: ledger.received.length });
        for (const event of result.events as { eventId: string; timestamp: string; cursor?: string | null }[]) {
          ledger.received.push({ eventId: event.eventId, cursor: event.cursor ?? null, timestamp: event.timestamp });
        }
        wait = result.hasMore ? 0 : Math.min(result.nextPollMs ?? 1000, options.maxWaitMs ?? 1000);
      } catch { /* dropped, refused or reset: poll again from the persisted cursor */ }
      finally { ctl.connection(undefined); }
      if (wait > 0) await sleep(wait, signal);
    }
  })();
  return { done, control: ctl.control };
}

export const spawnNode = (script: string, env: Record<string, string>, args: string[] = []): ChildProcess =>
  spawnProcess(process.execPath, [script, ...args], { env: { ...process.env, ...env }, stdio: ['pipe', 'pipe', 'pipe'] });
