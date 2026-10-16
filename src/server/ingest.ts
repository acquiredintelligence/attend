import { BackpressureError, ConflictError, OutboxFullError } from '../errors.js';
import type { Event, JsonObject } from '../core/types.js';
import type { Attend } from '../shell/types.js';
import type { HttpRoute, IngestResult, NdjsonIngestOptions, Principal } from './types.js';

type Rejection = IngestResult['rejected'][number];
type Parsed =
  | { kind: 'event'; scope: string; event: Event }
  | { kind: 'flush' | 'end'; scope: string }
  | { kind: 'rejected'; scope: string | null; error: Rejection['error'] };

function object(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function parse(text: string | null, principal: Principal | null): Parsed {
  if (text === null) return { kind: 'rejected', scope: null, error: 'too_large' };
  let value: unknown;
  try { value = JSON.parse(text); }
  catch { return { kind: 'rejected', scope: null, error: 'invalid' }; }
  const scope = object(value) && typeof value.scope === 'string' ? value.scope : null;
  const invalid: Parsed = { kind: 'rejected', scope, error: 'invalid' };
  if (!object(value) || !scope) return invalid;
  const { scope: _, flush, end, ...event } = value;
  let parsed: Parsed;
  if (flush !== undefined || end !== undefined) {
    if (flush === true && end === undefined) parsed = { kind: 'flush', scope };
    else if (end === true && flush === undefined) parsed = { kind: 'end', scope };
    else return invalid;
  } else {
    if (typeof event.id !== 'string' || typeof event.at !== 'number' || !Number.isFinite(event.at)
      || typeof event.name !== 'string' || !object(event.data)
      || (event.text !== undefined && typeof event.text !== 'string')) return invalid;
    parsed = { kind: 'event', scope, event: {
      ...event, id: event.id, at: event.at, name: event.name, data: event.data as JsonObject,
      ...(typeof event.text === 'string' ? { text: event.text } : {}),
    } };
  }
  if (principal && !principal.scopes.some((prefix) => scope.startsWith(prefix))) {
    return { kind: 'rejected', scope, error: 'forbidden' };
  }
  return parsed;
}

export function ndjsonIngest(attend: Attend, options: NdjsonIngestOptions = {}): HttpRoute {
  const stream = options.stream ?? false;
  const maxBodyBytes = options.maxBodyBytes ?? 64 * 1024 * 1024;
  const maxLineBytes = options.maxLineBytes ?? 1024 * 1024;
  const slice = options.slice ?? 64;
  if (!Number.isSafeInteger(slice) || slice < 1) throw new RangeError('attend: slice must be a positive integer');
  if (!Number.isSafeInteger(maxBodyBytes) || maxBodyBytes < 0 || !Number.isSafeInteger(maxLineBytes) || maxLineBytes < 0) {
    throw new RangeError('attend: byte limits must be non-negative integers');
  }

  return async (request, response) => {
    let principal: Principal | null = null;
    try {
      if (options.principal) {
        principal = await options.principal(request);
        if (!principal) { response.writeHead(401).end(); return; }
      }
    } catch { response.writeHead(500).end('Internal server error'); return; }
    if (request.destroyed || response.destroyed) return;
    if (request.method !== 'POST') { response.writeHead(405, { Allow: 'POST' }).end(); return; }
    if (request.headers['content-type']?.split(';')[0]?.trim().toLowerCase() !== 'application/x-ndjson') {
      response.writeHead(415).end(); return;
    }

    const credit = 8192;
    const result: IngestResult = { accepted: 0, duplicates: 0, rejected: [] };
    let buffer: Buffer = Buffer.alloc(0);
    let offset = 0;
    let parts: Buffer[] = [];
    let lineBytes = 0;
    let queue: { line: number; text: string | null }[] = [];
    let cursor = 0;
    let total = 0;
    let written = 0;
    let sequence = 0;
    let sourceEnded = false;
    let framedEnd = false;
    let stopped = false;
    let scheduled = false;
    let ackScheduled = false;
    let outputBlocked = false;
    let retryAfterMs: number | null = null;
    let blockedUntil = 0;
    const retryTimers = new Set<ReturnType<typeof setTimeout>>();
    const done = new Map<number, Rejection | null>();
    const withheld = new Set<number>();

    const cleanup = () => {
      stopped = true;
      for (const timer of retryTimers) clearTimeout(timer);
      retryTimers.clear();
    };
    const fail = () => {
      if (stopped) return;
      cleanup();
      request.pause();
      if (stream) response.destroy();
      else response.writeHead(500).end('Internal server error');
    };
    const frame = (value: object) => {
      if (!response.write(JSON.stringify(value) + '\n')) {
        outputBlocked = true;
        request.pause();
      }
    };
    const complete = () => {
      if (stopped || !framedEnd || written !== total || withheld.size > 0) return;
      cleanup();
      if (stream) response.end();
      else {
        result.rejected.sort((a, b) => a.line - b.line);
        response.writeHead(retryAfterMs === null ? 200 : 429, {
          'content-type': 'application/json',
          ...(retryAfterMs === null ? {} : { 'Retry-After': String(Math.max(1, Math.ceil(retryAfterMs / 1000))) }),
        }).end(JSON.stringify(result));
      }
    };
    const flushAck = () => {
      ackScheduled = false;
      if (stopped) return;
      const rejected: number[] = [];
      const errors: Rejection['error'][] = [];
      let granted = 0;
      const start = written;
      while (done.has(written)) {
        const rejection = done.get(written);
        if (rejection) { rejected.push(written); errors.push(rejection.error); }
        if (!withheld.has(written)) granted += 1;
        done.delete(written++);
      }
      if (stream && written > start) {
        frame({ ack: ++sequence, through: written, ...(rejected.length ? { rejected, errors } : {}) });
        if (granted) frame({ credit: granted });
      }
      fill();
      schedule();
      complete();
    };
    const finish = (line: number, rejection: Rejection | null) => {
      if (stopped) return;
      if (rejection) { if (!stream) result.rejected.push(rejection); }
      else result.accepted += 1;
      done.set(line, rejection);
      if (!ackScheduled) { ackScheduled = true; queueMicrotask(flushAck); }
    };
    const holdCredit = (line: number, delay: number) => {
      const until = Date.now() + delay;
      blockedUntil = Math.max(blockedUntil, until);
      withheld.add(line);
      request.pause();
      const arm = (remaining: number) => {
        const timer = setTimeout(() => {
          retryTimers.delete(timer);
          if (stopped) return;
          if (Date.now() < until) { arm(until - Date.now()); return; }
          withheld.delete(line);
          if (line < written) frame({ credit: 1 });
          fill();
          schedule();
          complete();
        }, Math.max(0, remaining));
        retryTimers.add(timer);
      };
      arm(delay);
    };
    const reject = (line: number, scope: string, error: unknown) => {
      if (stopped) return;
      let code: Rejection['error'];
      if (error instanceof BackpressureError) {
        code = 'backpressure';
        retryAfterMs = Math.max(retryAfterMs ?? 0, error.retryAfterMs);
        if (stream) holdCredit(line, error.retryAfterMs);
      } else if (error instanceof OutboxFullError) code = 'outbox_full';
      else if (error instanceof ConflictError) code = 'conflict';
      else { fail(); return; }
      finish(line, { line, scope, error: code });
    };
    const blocked = () => stream && (Date.now() < blockedUntil || outputBlocked);
    const processSlice = async () => {
      try {
        let count = 0;
        while (!stopped && !blocked() && cursor < queue.length && count++ < slice) {
          const item = queue[cursor++];
          if (!item) break;
          const parsed = parse(item.text, principal);
          if (!stream && retryAfterMs !== null) {
            finish(item.line, { line: item.line, scope: parsed.scope, error: 'backpressure' });
          } else if (parsed.kind === 'rejected') {
            finish(item.line, { line: item.line, scope: parsed.scope, error: parsed.error });
          } else {
            try {
              const operation = parsed.kind === 'event' ? attend.push(parsed.scope, [parsed.event])
                : parsed.kind === 'flush' ? attend.flush(parsed.scope) : attend.end(parsed.scope);
              operation.then(() => finish(item.line, null), (error: unknown) => reject(item.line, parsed.scope, error));
            } catch (error) { reject(item.line, parsed.scope, error); }
          }
          // Observe immediate admission failures without waiting for the group commit.
          await Promise.resolve();
        }
      } catch { fail(); }
      finally {
        if (cursor >= queue.length / 2) { queue = queue.slice(cursor); cursor = 0; }
        scheduled = false;
        schedule();
        complete();
      }
    };
    function schedule() {
      if (stopped || scheduled || blocked() || cursor === queue.length) return;
      scheduled = true;
      setImmediate(() => { void processSlice(); });
    }
    const endLine = () => {
      const text = lineBytes > maxLineBytes ? null : Buffer.concat(parts, lineBytes).toString('utf8');
      if (text === null || text.trim() !== '') queue.push({ line: total++, text });
      parts = [];
      lineBytes = 0;
    };
    function fill() {
      if (stopped || blocked()) return;
      while (offset < buffer.length && (!stream || total - written < 2 * credit)) {
        const newline = buffer.indexOf(10, offset);
        const end = newline < 0 ? buffer.length : newline;
        const part = buffer.subarray(offset, end);
        lineBytes += part.length;
        if (lineBytes <= maxLineBytes) parts.push(part);
        else parts = [];
        offset = newline < 0 ? end : end + 1;
        if (newline >= 0) endLine();
      }
      if (offset === buffer.length) {
        buffer = Buffer.alloc(0);
        offset = 0;
        if (sourceEnded && (!stream || total - written < 2 * credit)) {
          if (lineBytes) endLine();
          framedEnd = true;
        } else if (!sourceEnded && (!stream || total - written < 2 * credit)) request.resume();
      }
      schedule();
    }

    response.on('close', cleanup);
    response.on('error', fail);
    response.on('drain', () => { outputBlocked = false; fill(); schedule(); });
    request.on('error', fail);
    request.on('close', () => { if (!request.complete) { cleanup(); response.destroy(); } });
    if (stream) {
      request.setTimeout(0);
      response.setTimeout(0);
      response.writeHead(200, { 'content-type': 'application/x-ndjson' });
      response.flushHeaders();
      frame({ credit });
      request.on('data', (chunk: Buffer) => {
        request.pause();
        buffer = chunk;
        offset = 0;
        fill();
      });
      request.on('end', () => { sourceEnded = true; fill(); complete(); });
    } else {
      const chunks: Buffer[] = [];
      let bytes = 0;
      request.on('data', (chunk: Buffer) => {
        if (stopped) return;
        bytes += chunk.length;
        if (bytes > maxBodyBytes) {
          cleanup();
          chunks.length = 0;
          request.pause();
          response.writeHead(413).end();
        } else chunks.push(chunk);
      });
      request.on('end', () => {
        if (stopped) return;
        buffer = Buffer.concat(chunks, bytes);
        sourceEnded = true;
        fill();
        complete();
      });
    }
  };
}
