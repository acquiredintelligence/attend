import fc from 'fast-check';
import { expect, test } from 'vitest';
import { coverage, drive, planArbitrary } from './support/arbitraries.js';

test('drive is deterministic', () => {
  for (const plan of fc.sample(planArbitrary({ maxSteps: 80 }), { numRuns: 20, seed: 2 })) {
    expect(drive(plan)).toEqual(drive(plan));
  }
});

test('plans exercise every oracle input kind and edge', () => {
  const plans = fc.sample(planArbitrary(), { numRuns: 300, seed: 1 });
  const report = coverage(plans.map(drive));
  console.log(JSON.stringify(report));
  expect(Math.max(...plans.map((plan) => plan.steps.length))).toBeGreaterThanOrEqual(250);
  for (const kind of ['events', 'tick', 'flush', 'judged', 'judgeFailed', 'judgeReleased', 'delivered', 'deliveryFailed', 'deliveryReleased']) {
    expect(report.inputs[kind] ?? 0, kind).toBeGreaterThanOrEqual(10);
  }
  const edges = [
    'id:new', 'id:readmitted', 'dup:batch', 'dup:seen', 'dup:open', 'dup:held', 'seen:evicted',
    'events:source', 'events:cursorNull', 'events:truncated', 'time:backward', 'tick:due-1', 'tick:due', 'tick:due+1',
    'result:live', 'result:stale', 'judged:notOffered', 'effect:judge', 'effect:deliver',
    'trace:ingested', 'trace:ingested:duplicates', 'trace:gap', 'trace:dropped', 'trace:dropped:age', 'trace:dropped:overflow',
    'trace:settled', 'trace:settled:closing', 'trace:settled:flush', 'trace:settled:stream_quiet', 'trace:settled:passive_quiet',
    'trace:settled:overflow', 'trace:settled:age', 'trace:settled:forced', 'trace:settled:merged',
    'trace:judging', 'trace:judging:offerWait', 'trace:judging:noWait', 'trace:judged',
    'trace:judged:interrupt', 'trace:judged:queue', 'trace:judged:note', 'trace:judged:ask', 'trace:judged:skip', 'trace:waiting',
    'trace:retrying', 'trace:retrying:judge', 'trace:retrying:sink', 'trace:retrying:deadLetter', 'trace:delivering', 'trace:delivered',
    'trace:released', 'trace:released:judge', 'trace:released:sink', 'trace:released:deadLetter', 'trace:deadLettered',
    'trace:deadLettered:judge', 'trace:deadLettered:sink', 'trace:stale', 'trace:stale:flush', 'trace:stale:judged',
    'trace:stale:judgeFailed', 'trace:stale:judgeReleased', 'trace:stale:delivered', 'trace:stale:deliveryFailed', 'trace:stale:deliveryReleased',
  ];
  for (const edge of edges) expect(report.edges[edge] ?? 0, edge).toBeGreaterThanOrEqual(10);
}, 30_000);
