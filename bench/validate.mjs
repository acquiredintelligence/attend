#!/usr/bin/env node
// Validates every JSON line against the strict benchmark report contracts. Usage: node bench/validate.mjs <file.jsonl>...
import { readFileSync } from 'node:fs';
import { validateReport } from './lib/schema.mjs';

let invalid = 0;
const files = process.argv.slice(2);
if (!files.length) { console.error('Usage: node bench/validate.mjs <file.jsonl>...'); process.exitCode = 1; }
for (const file of files) {
  let count = 0;
  let bad = 0;
  try {
    for (const [i, line] of readFileSync(file, 'utf8').split('\n').entries()) {
      if (!line.trim()) continue;
      count += 1;
      let result;
      try { result = validateReport(JSON.parse(line)); }
      catch { result = { ok: false, errors: ['invalid JSON'] }; }
      if (!result.ok) { bad += 1; console.error(`${file}:${i + 1}: ${result.errors.join('; ')}`); }
    }
  } catch (error) { bad += 1; console.error(error.message); }
  console.log(`${file}: ${count} reports, ${bad} invalid`);
  invalid += bad;
}
if (invalid) process.exitCode = 1;
