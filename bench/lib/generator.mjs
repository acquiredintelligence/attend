import http from 'node:http';
import { performance } from 'node:perf_hooks';
import { createHistogram } from './histogram.mjs';
import { eventLine } from './workload.mjs';

export async function drive({ socketPath, schedule, keepLines = false, stall = null, onEnd = () => {}, drainMs = 30000 }) {
  const { total, warmCount } = schedule;
  if (total <= warmCount) throw new Error('schedule needs measured lines');
  const latency = createHistogram();
  const service = createHistogram();
  const first = createHistogram();
  const last = createHistogram();
  const sent = new Float64Array(total);
  const intended = keepLines ? new Float64Array(total) : null;
  const ackTimes = keepLines ? new Float64Array(total) : null;
  let credits = 0;
  let acked = 0;
  let next = 0;
  let measured = 0;
  let rejected = 0;
  let seconds = 0;
  let t0;
  let epoch0;
  let lastAck;
  let maxLag = 0;
  let stalled = false;
  let drain;
  let immediate;
  let closed = false;
  let stream;
  let resolveDone;
  let rejectDone;
  const completion = new Promise((resolve, reject) => { resolveDone = resolve; rejectDone = reject; });
  const request = http.request({ socketPath, path: '/v1/ingest/stream', method: 'POST', agent: false, headers: { 'content-type': 'application/x-ndjson', 'transfer-encoding': 'chunked' } });
  const finish = async () => {
    const end = performance.now();
    if (closed) return;
    closed = true;
    for (let i = Math.max(acked, warmCount); i < total; i += 1) {
      const value = Math.max(0, (end - (t0 + schedule.intendedMs(i))) * 1000);
      latency.record(value);
      if (i - warmCount < (total - warmCount) / 5) first.record(value);
      if (i - warmCount >= (total - warmCount) * 4 / 5) last.record(value);
    }
    const spanEnd = acked === total ? lastAck : end;
    seconds = t0 === undefined || spanEnd === undefined ? 0 : Math.max(0, (spanEnd - (t0 + schedule.intendedMs(warmCount))) / 1000);
    clearImmediate(immediate);
    clearTimeout(drain);
    try { await onEnd(); resolveDone(); } catch (error) { rejectDone(error); }
  };
  request.on('error', rejectDone);
  request.on('response', (response) => {
    stream = response;
    if (response.statusCode !== 200) { rejectDone(new Error(`ingest status ${response.statusCode}`)); return; }
    let partial = '';
    response.setEncoding('utf8');
    response.on('error', rejectDone);
    response.on('end', () => { if (acked < total && !closed) rejectDone(new Error('ingest stream ended before all acknowledgments')); });
    response.on('data', (chunk) => {
      if (closed) return;
      partial += chunk;
      const lines = partial.split('\n');
      partial = lines.pop();
      for (const line of lines) {
        if (!line) continue;
        let frame;
        try { frame = JSON.parse(line); } catch (error) { rejectDone(error); return; }
        if (frame.credit !== undefined) {
          credits += frame.credit;
          if (t0 === undefined) {
            t0 = performance.now();
            epoch0 = Date.now();
            if (intended) for (let i = 0; i < total; i += 1) intended[i] = t0 + schedule.intendedMs(i);
            drain = setTimeout(() => void finish(), schedule.intendedMs(total - 1) + drainMs);
            immediate = setImmediate(loop);
          }
        }
        if (frame.through !== undefined) {
          if (!Number.isInteger(frame.through) || frame.through < acked || frame.through > next) { rejectDone(new Error('invalid ack prefix')); return; }
          if (frame.rejected !== undefined && (!Array.isArray(frame.rejected) || frame.rejected.some((index, i) => !Number.isInteger(index) || index < acked || index >= frame.through || (i > 0 && index <= frame.rejected[i - 1])))) {
            rejectDone(new Error('invalid rejection list')); return;
          }
          const now = performance.now();
          let rejection = 0;
          for (; acked < frame.through; acked += 1) {
            if (ackTimes) ackTimes[acked] = now;
            const isRejected = frame.rejected?.[rejection] === acked;
            if (isRejected) { rejected += 1; rejection += 1; }
            if (acked < warmCount) continue;
            lastAck = now;
            if (isRejected) continue;
            const value = (now - (t0 + schedule.intendedMs(acked))) * 1000;
            latency.record(Math.max(0, value));
            service.record(Math.max(0, (now - sent[acked]) * 1000));
            if (acked - warmCount < (total - warmCount) / 5) first.record(Math.max(0, value));
            if (acked - warmCount >= (total - warmCount) * 4 / 5) last.record(Math.max(0, value));
            measured += 1;
          }
          if (acked === total) void finish();
        }
      }
    });
  });
  function loop() {
    if (closed) return;
    try {
      let chunk = '';
      let batchStart = next;
      const dueAt = performance.now();
      const flush = () => {
        if (!chunk) return;
        const now = performance.now();
        for (let i = batchStart; i < next; i += 1) {
          sent[i] = now;
          if (i >= warmCount) maxLag = Math.max(maxLag, now - (t0 + schedule.intendedMs(i)));
        }
        if (now >= t0 + schedule.intendedMs(total - 1)) {
          clearTimeout(drain);
          drain = setTimeout(() => void finish(), drainMs);
        }
        request.write(chunk);
        chunk = '';
        batchStart = next;
      };
      while (next < total && next < credits && t0 + schedule.intendedMs(next) <= dueAt) {
        if (stall && !stalled && next === stall.atLine) {
          flush();
          stalled = true;
          const until = performance.now() + stall.ms;
          while (performance.now() < until) {}
        }
        chunk += eventLine(next, schedule.scopeOf(next), schedule.kindOf(next), Math.round(epoch0 + schedule.intendedMs(next)));
        next += 1;
      }
      flush();
      if (next === total) request.end();
      else immediate = setImmediate(loop);
    } catch (error) { rejectDone(error); }
  }
  request.flushHeaders();
  try {
    await completion;
    return {
      latency, service, first, last, total, acked, failed: total - acked, rejected, measured,
      achievedRate: seconds ? measured / seconds : 0, generatorMaxLagMs: maxLag, seconds,
      ...(keepLines ? { lines: { intended, sent, acked: ackTimes } } : {}),
    };
  } finally {
    closed = true; clearImmediate(immediate); clearTimeout(drain); request.destroy(); stream?.destroy();
  }
}
