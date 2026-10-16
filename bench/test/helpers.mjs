import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

export const root = fileURLToPath(new URL('../../', import.meta.url));
export const dist = fileURLToPath(new URL('../../dist', import.meta.url));
export const fakeDist = fileURLToPath(new URL('./fixtures/core/dist', import.meta.url));
export const stateDist = fileURLToPath(new URL('./fixtures/state/dist', import.meta.url));
export function cli(script, flags = [], nodeFlags = []) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [...nodeFlags, `bench/${script}.mjs`, ...flags], { cwd: root, stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';
    child.stdout.setEncoding('utf8').on('data', (chunk) => { stdout += chunk; });
    child.stderr.setEncoding('utf8').on('data', (chunk) => { stderr += chunk; });
    const timeout = setTimeout(() => child.kill('SIGKILL'), 110000);
    child.on('error', reject);
    child.on('exit', (code) => { clearTimeout(timeout); resolve({ code, stdout, stderr }); });
  });
}
export const percentiles = () => ({ p50: 10, p99: 20, p999: 30, max: 40 });
export const runner = () => ({ pinned: true, host: 'test/arch CPU x2, node v24', loadAverage: 0 });
const common = () => ({ adapterVersion: '1.2.3+abcdef0', runner: runner() });
export const openReport = (changes = {}) => ({
  report: 'openloop', targetRate: 50000, achievedRate: 50000, scopes: 1000, seconds: 60, mix: 'continuous',
  latencyUs: percentiles(), engineUs: percentiles(), noopFloorUs: percentiles(), p50FirstUs: 10, p50LastUs: 10,
  failed: 0, attendCores: 1, gcShare: 0, maxSchedulerLagMs: 0, generatorMaxLagMs: 0, windows: 1, meanWindow: 10, p99Window: 10,
  overdueRule: false, ...common(), ...changes,
});
export const allocReport = (changes = {}) => ({ report: 'alloc', layer: 'reducer', reducer: 'step', window: 10, seenCount: 0, events: 20000, bytesPerEvent: 1000, cpuMicrosPerEvent: 1, ...common(), ...changes });
export const durableReport = (changes = {}) => ({
  report: 'durable', durability: { store: 'sqlite', journalMode: 'WAL', synchronous: 'NORMAL', worker: false },
  scopes: 200, eventsPerScope: 700, maxWindow: 50, eventsPerSecond: 2000, eventsPerCommit: 2, commitsPerSecond: 1000,
  commitLatencyMs: percentiles(), bytesWrittenPerEvent: 300, eventBytes: 100, cpuMicrosPerEvent: 1, ...common(), ...changes,
});
export const latencyReport = (changes = {}) => ({
  report: 'latency', durability: { store: 'memory' }, load: null, samples: 200, errors: 0,
  flushToJudgeUs: percentiles(), judgeToSinkUs: percentiles(), nonJudgeUs: percentiles(), flushToSinkUs: percentiles(),
  commitToSubscriberUs: null, commitLatencyMs: null, ...common(), ...changes,
});
