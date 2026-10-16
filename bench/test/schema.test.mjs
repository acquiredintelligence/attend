import { test, expect } from 'vitest';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import os from 'node:os';
import { join } from 'node:path';
import { validateReport } from '../lib/schema.mjs';
import { openReport, allocReport, durableReport, latencyReport, cli } from './helpers.mjs';

test('Schemas accept one complete fixture per report', () => {
  for (const fixture of [openReport(), allocReport(), durableReport(), latencyReport()]) expect(validateReport(fixture)).toEqual({ ok: true, report: fixture });
});
test('Schemas reject missing, extra, enum, negative, percentile and durability fields with paths', () => {
  const missing = openReport();
  delete missing.scopes;
  const fixtures = [
    [missing, 'scopes'], [openReport({ extra: true }), 'extra'], [openReport({ mix: 'invalid' }), 'mix'],
    [allocReport({ bytesPerEvent: -1 }), 'bytesPerEvent'], [latencyReport({ samples: Infinity }), 'samples'],
    [openReport({ latencyUs: { p50: 20, p99: 10 } }), 'latencyUs.p99'],
    [openReport({ engineUs: { p50: 1, p99: 2, p999: 1 } }), 'engineUs.p999'],
    [durableReport({ durability: { store: 'redis', appendfsync: 'always' } }), 'durability.appendonly'],
    [durableReport({ durability: { store: 'sqlite', journalMode: 'WAL', synchronous: 'EXTRA', worker: false } }), 'durability.synchronous'],
  ];
  for (const [fixture, field] of fixtures) {
    const result = validateReport(fixture);
    expect(result.ok).toBe(false);
    expect(result.errors.join('\n')).toContain(field);
  }
});
test('validate CLI reports non-JSON lines and exits one', async () => {
  const dir = await mkdtemp(join(os.tmpdir(), 'ab-test-'));
  try {
    const file = join(dir, 'reports.jsonl');
    await writeFile(file, JSON.stringify(openReport()) + '\nnot JSON\n');
    const result = await cli('validate', [file]);
    expect(result.code).toBe(1);
    expect(result.stdout).toContain('2 reports, 1 invalid');
    expect(result.stderr).toContain(':2: invalid JSON');
  } finally { await rm(dir, { recursive: true, force: true }); }
});
