import { z } from 'zod';

const number = z.number().finite().nonnegative();
export const Percentiles = z.strictObject({ p50: number, p90: number.optional(), p99: number, p999: number.optional(), max: number.optional() }).superRefine((value, ctx) => {
  let previous = value.p50;
  for (const key of ['p90', 'p99', 'p999', 'max']) {
    if (value[key] === undefined) continue;
    if (value[key] < previous) ctx.addIssue({ code: 'custom', path: [key], message: 'must be at least the preceding percentile' });
    previous = value[key];
  }
});
export const Runner = z.strictObject({ pinned: z.boolean(), host: z.string(), loadAverage: number });
export const DurabilityMode = z.discriminatedUnion('store', [
  z.strictObject({ store: z.literal('memory') }),
  z.strictObject({ store: z.literal('sqlite'), journalMode: z.enum(['WAL', 'DELETE']), synchronous: z.enum(['OFF', 'NORMAL', 'FULL']), worker: z.boolean() }),
  z.strictObject({ store: z.literal('postgres'), synchronousCommit: z.enum(['on', 'off', 'local', 'remote_write', 'remote_apply']) }),
  z.strictObject({ store: z.literal('redis'), appendonly: z.boolean(), appendfsync: z.enum(['always', 'everysec', 'no']) }),
]);
const common = { adapterVersion: z.string(), runner: Runner };
const mixes = z.enum(['continuous', 'realistic', 'tape']);
export const DurableReport = z.strictObject({
  report: z.literal('durable'), durability: DurabilityMode, scopes: number, eventsPerScope: number, maxWindow: number,
  eventsPerSecond: number, eventsPerCommit: number, commitsPerSecond: number, commitLatencyMs: Percentiles,
  bytesWrittenPerEvent: number, eventBytes: number, cpuMicrosPerEvent: number, ...common,
});
export const OpenLoopReport = z.strictObject({
  report: z.literal('openloop'), targetRate: number, achievedRate: number, scopes: number, seconds: number,
  mix: z.enum(['continuous', 'realistic', 'tape', 'noop']), latencyUs: Percentiles, engineUs: Percentiles, noopFloorUs: Percentiles,
  rejected: number, noopFloorRate: number, noopFloorFailed: number,
  p50FirstUs: number, p50LastUs: number, failed: number, attendCores: number, gcShare: number, maxSchedulerLagMs: number,
  generatorMaxLagMs: number, windows: number, meanWindow: number, p99Window: number, overdueRule: z.boolean(), ...common,
});
export const AllocationReport = z.strictObject({
  report: z.literal('alloc'), layer: z.enum(['reducer', 'endToEnd']), reducer: z.enum(['step', 'core']),
  window: number, seenCount: number, events: number, bytesPerEvent: number, sampledBytesPerEvent: number.optional(), cpuMicrosPerEvent: number, ...common,
});
export const LatencyReport = z.strictObject({
  report: z.literal('latency'), durability: DurabilityMode,
  load: z.strictObject({ mix: mixes, targetRate: number, achievedRate: number, scopes: number }).nullable(),
  samples: number, errors: number, flushToJudgeUs: Percentiles, judgeToSinkUs: Percentiles, nonJudgeUs: Percentiles,
  flushToSinkUs: Percentiles, commitToSubscriberUs: Percentiles.nullable(), commitLatencyMs: Percentiles.nullable(), ...common,
});
export const ServeReport = z.strictObject({
  report: z.literal('serve'), durability: DurabilityMode, samples: number, errors: number,
  ingestUs: Percentiles, commitToSubscriberUs: Percentiles, endToEndUs: Percentiles, ...common,
});
const reports = { durable: DurableReport, openloop: OpenLoopReport, alloc: AllocationReport, latency: LatencyReport, serve: ServeReport };
export function validateReport(value) {
  const schema = reports[value?.report];
  if (!schema) return { ok: false, errors: ['report: unknown report type'] };
  const parsed = schema.safeParse(value);
  if (parsed.success) return { ok: true, report: parsed.data };
  return { ok: false, errors: parsed.error.issues.flatMap((issue) => issue.code === 'unrecognized_keys'
    ? issue.keys.map((key) => `${[...issue.path, key].join('.')}: unknown key`)
    : [`${issue.path.join('.')}: ${issue.message}`]) };
}
