const pass = (name, ok, detail) => ({ name, status: ok ? 'met' : 'not met', detail });
const missing = (name, detail) => ({ name, status: 'not verified', detail });
const finish = (slo, checks, info) => ({
  slo, status: checks.some((check) => check.status === 'not met') ? 'not met' : checks.some((check) => check.status === 'not verified') ? 'not verified' : 'met',
  checks, ...(info ? { info } : {}),
});
const flat = (r) => r.p50LastUs <= 2 * r.p50FirstUs + 100;
const ratePass = (r) => r.achievedRate >= 0.999 * r.targetRate && flat(r) && r.failed === 0 && r.rejected === 0;
const MIN_PROBES = 200;
const median = (values) => {
  const sorted = [...values].sort((a, b) => a - b);
  return (sorted[Math.floor((sorted.length - 1) / 2)] + sorted[Math.floor(sorted.length / 2)]) / 2;
};

export function evaluateGates(reports) {
  const open = reports.filter((r) => r.report === 'openloop' && r.scopes >= 1000 && r.seconds >= 59.9);
  const mixes = ['continuous', 'realistic'];
  const one = [];
  const knee = {};
  for (const mix of mixes) {
    const runs = open.filter((r) => r.mix === mix && r.targetRate === 50000);
    one.push(runs.length < 3 ? missing(mix, `${runs.length}/3 runs at 50000 over 1000 scopes for 60 s`) : pass(mix, runs.every((r) => ratePass(r) && r.attendCores <= 1), 'rate, flat p50, no failures or rejections, at most one core'));
    const rates = [...new Set(open.filter((r) => r.mix === mix).map((r) => r.targetRate))];
    knee[mix] = Math.max(0, ...rates.filter((rate) => open.filter((r) => r.mix === mix && r.targetRate === rate).every(ratePass)));
  }
  const two = [];
  for (const mix of mixes) for (const rate of [10000, 30000, 50000]) {
    const all = open.filter((r) => r.mix === mix && r.targetRate === rate);
    const pinned = all.filter((r) => r.runner.pinned);
    const floored = pinned.filter((r) => r.noopFloorFailed === 0 && r.noopFloorRate >= 0.999 * r.targetRate);
    const name = `${mix}/${rate}`;
    two.push(floored.length < 3 ? missing(name, `${floored.length}/3 pinned runs with usable floors over 1000 scopes for 60 s; ${all.length - pinned.length} unpinned; ${pinned.length - floored.length} with unusable floor`)
      : pass(name, floored.every((r) => ratePass(r) && r.engineUs.p99 <= 100 && r.latencyUs.p99 - r.noopFloorUs.p99 <= 100), 'rate within 0.1%, flat p50, no failures or rejections, clean floor at rate, engine and floor-adjusted p99 at most 100 us'));
  }
  const alloc = reports.filter((r) => r.report === 'alloc');
  const three = [];
  const versions = [...new Set(alloc.map((r) => r.adapterVersion))];
  const windows = [10, 100, 250, 1000];
  const seen = [0, 50, 500, 2000];
  if (!versions.length) three.push(missing('grid', 'no allocation reports'));
  for (const version of versions) {
    const cells = alloc.filter((r) => r.adapterVersion === version);
    const complete = ['reducer', 'endToEnd'].every((layer) => windows.every((window) => seen.every((seenCount) => cells.some((r) => r.layer === layer && r.window === window && r.seenCount === seenCount))));
    if (!complete) { three.push(missing(version, 'needs both layers and every W x S cell from the same build')); continue; }
    for (const layer of ['reducer', 'endToEnd']) {
      const rows = cells.filter((r) => r.layer === layer && windows.includes(r.window) && seen.includes(r.seenCount));
      const bounded = (group) => {
        const mid = median(group.map((r) => r.bytesPerEvent));
        return group.every((r) => r.bytesPerEvent >= mid * 0.8 && r.bytesPerEvent <= mid * 1.2);
      };
      for (const s of seen) three.push(pass(`${version}/${layer}/S=${s}`, bounded(rows.filter((r) => r.seenCount === s)), 'within 20% of row median'));
      for (const w of windows) three.push(pass(`${version}/${layer}/W=${w}`, bounded(rows.filter((r) => r.window === w)), 'within 20% of column median'));
      if (layer === 'endToEnd') three.push(pass(`${version}/endToEnd/size`, rows.filter((r) => r.seenCount === 500).every((r) => r.bytesPerEvent <= 1536), 'at most 1536 bytes/event at S=500'));
    }
  }
  const durable = reports.filter((r) => r.report === 'durable');
  const workloads = [
    ['200x700', (r) => r.scopes >= 200 && r.eventsPerScope >= 700],
    ['1000 scopes', (r) => r.scopes >= 1000 && r.scopes * r.eventsPerScope >= 140000],
  ];
  const sized = durable.filter((r) => workloads.some(([, matches]) => matches(r)));
  const modeLabel = ({ durability: d }) => d.store === 'sqlite' ? `sqlite ${d.journalMode}/${d.synchronous}${d.worker ? ' worker' : ''}`
    : d.store === 'postgres' ? `postgres synchronous_commit=${d.synchronousCommit}`
      : d.store === 'redis' ? `redis appendonly${d.appendonly ? '' : '=false'}` : d.store;
  const modes = ['sqlite WAL/NORMAL', 'sqlite WAL/FULL', 'sqlite DELETE/FULL', 'postgres synchronous_commit=on', 'redis appendonly'];
  const four = [];
  for (const mode of modes) for (const [workload, matches] of workloads) for (const window of [50, 250]) {
    if (!sized.some((r) => modeLabel(r) === mode && matches(r) && r.maxWindow === window)) four.push(missing(`${mode}/${workload}/W=${window}`, 'no report of at least this size'));
  }
  for (const r of sized) {
    const name = `${modeLabel(r)}/${r.scopes}x${r.eventsPerScope} W=${r.maxWindow}`;
    four.push(r.eventsPerSecond < 1000 ? missing(`${name}/grouping`, 'below 1000 events/s, so grouping at that rate is not shown')
      : pass(`${name}/grouping`, r.eventsPerCommit > 1, 'more than one event per commit at 1000 events/s or more'));
    four.push(pass(`${name}/bytes`, r.bytesWrittenPerEvent <= 4 * r.eventBytes, 'at most four times the event bytes'));
  }
  const latency = reports.filter((r) => r.report === 'latency');
  const five = [];
  const classes = [
    ['memory idle', (r) => r.durability.store === 'memory' && r.load === null, () => 2000],
    ['memory loaded', (r) => r.durability.store === 'memory' && r.load?.targetRate >= 50000, () => 10000],
    ...['sqlite', 'postgres', 'redis'].map((store) => [store, (r) => r.durability.store === store, (r) => r.commitLatencyMs === null ? null : 2000 * r.commitLatencyMs.p99 + 2000]),
  ];
  const usable = (r) => r.errors === 0 && r.samples >= MIN_PROBES && (r.load === null || (r.load.achievedRate >= 0.999 * r.load.targetRate && r.load.scopes >= 1000));
  const usableReports = [];
  for (const [name, matches, limit] of classes) {
    const rows = latency.filter(matches);
    for (const [i, r] of rows.entries()) if (r.errors > 0) five.push(pass(`${name}/${i + 1}`, false, `${r.errors} failed probes`));
    const accepted = rows.filter(usable);
    usableReports.push(...accepted);
    if (!accepted.length) five.push(missing(name, !rows.length ? 'missing report' : `none had at least ${MIN_PROBES} probes with none failed${rows.some((r) => r.load !== null) ? ' and the full load over 1000 scopes' : ''}`));
    for (const [i, r] of rows.entries()) {
      if (!usable(r)) continue;
      const threshold = limit(r);
      five.push(threshold === null ? missing(`${name}/${i + 1}`, 'missing commit latency') : pass(`${name}/${i + 1}`, r.nonJudgeUs.p99 <= threshold, `non-judge p99 <= ${threshold} us`));
    }
  }
  five.push(usableReports.some((r) => r.commitToSubscriberUs !== null) ? pass('commit to subscriber', true, 'segment reported; no bound specified') : missing('commit to subscriber', 'MCP Events segment unavailable'));
  return [finish(1, one, { knee }), finish(2, two), finish(3, three), finish(4, four, durable.length > sized.length ? { undersized: durable.length - sized.length } : undefined), finish(5, five)];
}
