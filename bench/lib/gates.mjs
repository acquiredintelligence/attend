const pass = (name, ok, detail) => ({ name, status: ok ? 'met' : 'not met', detail });
const missing = (name, detail) => ({ name, status: 'not verified', detail });
const finish = (slo, checks, info) => ({
  slo, status: checks.some((check) => check.status === 'not met') ? 'not met' : checks.some((check) => check.status === 'not verified') ? 'not verified' : 'met',
  checks, ...(info ? { info } : {}),
});
const flat = (r) => r.p50LastUs <= 2 * r.p50FirstUs + 100;
const ratePass = (r) => r.achievedRate >= 0.999 * r.targetRate && flat(r) && r.failed === 0;
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
    one.push(runs.length < 3 ? missing(mix, `${runs.length}/3 runs at 50000 over 1000 scopes for 60 s`) : pass(mix, runs.every((r) => ratePass(r) && r.attendCores <= 1), 'rate, flat p50, no failures, at most one core'));
    const rates = [...new Set(open.filter((r) => r.mix === mix).map((r) => r.targetRate))];
    knee[mix] = Math.max(0, ...rates.filter((rate) => open.filter((r) => r.mix === mix && r.targetRate === rate).every(ratePass)));
  }
  const two = [];
  for (const mix of mixes) for (const rate of [10000, 30000, 50000]) {
    const all = open.filter((r) => r.mix === mix && r.targetRate === rate);
    const pinned = all.filter((r) => r.runner.pinned);
    const name = `${mix}/${rate}`;
    two.push(pinned.length < 3 ? missing(name, `${pinned.length}/3 pinned runs over 1000 scopes for 60 s; ${all.length - pinned.length} unpinned`)
      : pass(name, pinned.every((r) => r.engineUs.p99 <= 100 && r.latencyUs.p99 - r.noopFloorUs.p99 <= 100), 'engine and floor-adjusted p99 at most 100 us'));
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
  const four = [
    ...['sqlite', 'postgres', 'redis'].filter((store) => !durable.some((r) => r.durability.store === store)).map((store) => missing(store, 'no durable report')),
    ...durable.flatMap((r, i) => [
      pass(`report ${i + 1}/grouping`, r.eventsPerSecond < 1000 || r.eventsPerCommit > 1, 'more than one event/commit at rate >=1000'),
      pass(`report ${i + 1}/bytes`, r.bytesWrittenPerEvent <= 4 * r.eventBytes, 'at most four times event bytes'),
    ]),
  ];
  const latency = reports.filter((r) => r.report === 'latency');
  const five = [];
  const classes = [
    ['memory idle', (r) => r.durability.store === 'memory' && r.load === null, () => 2000],
    ['memory loaded', (r) => r.durability.store === 'memory' && r.load?.targetRate >= 50000, () => 10000],
    ['durable', (r) => r.durability.store !== 'memory', (r) => r.commitLatencyMs === null ? null : 2000 * r.commitLatencyMs.p99 + 2000],
  ];
  for (const [name, matches, limit] of classes) {
    const rows = latency.filter(matches);
    if (!rows.length) five.push(missing(name, 'missing report'));
    else for (const [i, r] of rows.entries()) {
      const threshold = limit(r);
      five.push(threshold === null ? missing(`${name}/${i + 1}`, 'missing commit latency') : pass(`${name}/${i + 1}`, r.nonJudgeUs.p99 <= threshold, `non-judge p99 <= ${threshold} us`));
    }
  }
  five.push(latency.some((r) => r.commitToSubscriberUs !== null) ? pass('commit to subscriber', true, 'segment reported; no bound specified') : missing('commit to subscriber', 'MCP Events segment unavailable'));
  return [finish(1, one, { knee }), finish(2, two), finish(3, three), finish(4, four), finish(5, five)];
}
