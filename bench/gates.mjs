#!/usr/bin/env node
// Judges validated benchmark reports against the five service objectives. Usage: node bench/gates.mjs <file.jsonl>...
import { evaluateGates } from './lib/gates.mjs';
import { readReports } from './lib/cli.mjs';

try {
  if (process.argv.length < 3) throw new Error('Usage: node bench/gates.mjs <file.jsonl>...');
  const results = evaluateGates(readReports(process.argv.slice(2)));
  console.log('SLO  STATUS        CHECKS');
  for (const result of results) {
    console.log(`${result.slo}    ${result.status.padEnd(13)} ${result.checks.length}`);
    for (const check of result.checks) if (check.status !== 'met') console.log(`     ${check.name}: ${check.status}; ${check.detail}`);
    if (result.info) console.log(`     ${JSON.stringify(result.info)}`);
  }
  if (results.some((result) => result.status === 'not met')) process.exitCode = 1;
} catch (error) { console.error(error.message); process.exitCode = 1; }
