import { spawnTarget } from './spawn.mjs';
import { drive } from './generator.mjs';
import { histogramFromJSON } from './histogram.mjs';
import { runnerInfo, finishRunner } from './runner.mjs';

export async function runTarget(options, schedule, driveOptions = {}) {
  const runner = runnerInfo();
  const target = spawnTarget({ ...options, warmLines: schedule.warmCount });
  try {
    await target.ready;
    const run = await drive({
      socketPath: target.socketPath, schedule, ...driveOptions,
      onEnd: () => target.request({ type: 'measure', phase: 'end' }),
    });
    const { stats } = await target.request('stats');
    return { run, stats, runner: finishRunner(runner, stats) };
  } finally { await target.exit(); }
}

export function openLoopReport({ run, stats, runner }, { schedule, scopes, mix, floor }) {
  return {
    report: 'openloop', targetRate: schedule.targetRate, achievedRate: run.achievedRate,
    scopes, seconds: run.seconds, mix, latencyUs: run.latency.percentiles(),
    engineUs: histogramFromJSON(stats.engine).percentiles(), noopFloorUs: floor,
    p50FirstUs: run.first.percentile(50), p50LastUs: run.last.percentile(50), failed: run.failed + stats.rejected,
    attendCores: stats.wallMs ? stats.cpuMicros / (stats.wallMs * 1000) : 0,
    gcShare: stats.wallMs ? stats.gcMs / stats.wallMs : 0, maxSchedulerLagMs: stats.maxLagMs,
    generatorMaxLagMs: run.generatorMaxLagMs, windows: stats.windows, meanWindow: stats.meanWindow,
    p99Window: stats.p99Window, overdueRule: stats.port === 'scope', adapterVersion: stats.adapterVersion, runner,
  };
}
