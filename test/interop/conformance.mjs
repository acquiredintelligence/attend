// Runs the MCP Events conformance scenarios (modelcontextprotocol/conformance PR 521) against the
// built `attend serve` (dist/cli.js) over Streamable HTTP and scores them.
//
//   node test/interop/conformance.mjs <path to the conformance checkout, built>
//
// Two servers run: one fed an event every 100 ms (discovery, poll, webhook), and one fed an event
// every 20 s (push), because the push scenario needs both an event and a quiet stretch to see a
// heartbeat. The events arrive through POST /v1/ingest, like any producer's would. Rows are scored from each scenario's checks.json.
//
// Exit status is 1 when a row fails outright, or when a row in conformance-baseline.json no longer
// passes. Rows that need a fixture control attend does not ship, and webhook rows, are reported but do not fail.
import { spawn } from 'node:child_process';
import { createRequire } from 'node:module';
import { createServer } from 'node:net';
import { mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync, appendFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const suite = resolve(process.argv[2] ?? '');
if (!process.argv[2]) throw new Error('usage: conformance.mjs <conformance checkout>');
const require = createRequire(join(suite, 'package.json'));
const yaml = require('js-yaml');

const SCENARIOS = [
  { id: 'events-discovery', server: 'busy', timeout: 30_000 },
  { id: 'events-poll', server: 'busy', timeout: 60_000 },
  { id: 'events-push', server: 'slow', timeout: 90_000 },
  { id: 'events-webhook', server: 'busy', timeout: 60_000 },
  { id: 'events-webhook-delivery', server: 'busy', timeout: 180_000 },
];

const freePort = () => new Promise((ready) => {
  const server = createServer();
  server.listen(0, '127.0.0.1', () => { const { port } = server.address(); server.close(() => ready(port)); });
});

async function launchServe(dir, name, produceMs) {
  const port = await freePort();
  const child = spawn(process.execPath, [join(here, '../../dist/cli.js'), 'serve', '--store', `sqlite:${join(dir, `${name}.db`)}`, '--port', String(port), '--default-kind', 'flush'], { stdio: ['ignore', 'pipe', 'pipe'] });
  await new Promise((ready, fail) => {
    let text = '';
    const onData = (chunk) => { text += chunk; if (text.includes('attend serving')) ready(); };
    child.stdout.on('data', onData);
    child.stderr.on('data', onData);
    child.once('exit', () => fail(new Error(`attend serve exited: ${text}`)));
  });
  let produced = 0;
  const producer = setInterval(() => {
    const n = produced++;
    const line = JSON.stringify({ scope: `ops/${n % 5}`, id: `tick-${n}`, at: Date.now(), name: 'tick', data: { n } });
    fetch(`http://127.0.0.1:${port}/v1/ingest`, { method: 'POST', headers: { 'Content-Type': 'application/x-ndjson' }, body: line + '\n' }).catch(() => undefined);
  }, produceMs);
  return { url: `http://127.0.0.1:${port}/mcp`, stop: () => { clearInterval(producer); child.kill('SIGKILL'); } };
}

function runScenario(scenario, url, out) {
  return new Promise((done) => {
    const child = spawn(process.execPath, [join(suite, 'dist/index.js'), 'server', '--url', url, '--scenario', scenario.id, '--timeout', String(scenario.timeout), '-o', out], { stdio: ['ignore', 'pipe', 'pipe'] });
    let text = '';
    child.stdout.on('data', (chunk) => { text += chunk; });
    child.stderr.on('data', (chunk) => { text += chunk; });
    child.on('exit', (code) => done({ code, text }));
  });
}

const dir = mkdtempSync(join(tmpdir(), 'attend-conformance-'));
const out = join(dir, 'results');
const busy = await launchServe(dir, 'busy', 100);
const quiet = await launchServe(dir, 'quiet', 25_000);
const servers = { busy, slow: quiet };
const runs = [];
try {
  for (const scenario of SCENARIOS) {
    const run = await runScenario(scenario, servers[scenario.server].url, out);
    runs.push({ scenario, ...run });
    console.log(`${scenario.id}: runner exit ${run.code}`);
  }
} finally { for (const server of Object.values(servers)) server.stop(); }

const requirements = yaml.load(readFileSync(join(suite, 'src/seps/sep-3415.yaml'), 'utf8')).requirements;
const declared = requirements.filter((row) => row.excluded === undefined).map((row) => row.check);

const emissions = new Map();
for (const folder of readdirSync(out)) {
  const scenario = folder.match(/^server-(events-[a-z-]+?)-\d{4}-/)?.[1];
  if (!scenario) continue;
  for (const check of JSON.parse(readFileSync(join(out, folder, 'checks.json'), 'utf8'))) {
    if (!check.id.startsWith('sep-3415-')) continue;
    const reason = String(check.details?.reason ?? check.errorMessage ?? '');
    let kind;
    if (check.status === 'SUCCESS') kind = 'pass';
    else if (check.status === 'SKIPPED') kind = 'skipped';
    else if (check.details?.untestable) kind = /webhook/i.test(reason) ? 'webhook' : 'unverified';
    else kind = 'fail';
    if (!emissions.has(check.id)) emissions.set(check.id, []);
    emissions.get(check.id).push({ scenario, kind, reason: reason.replace(/\s+/g, ' ').slice(0, 240), message: check.errorMessage ?? '' });
  }
}

const rows = declared.map((id) => {
  const list = emissions.get(id) ?? [];
  const kinds = new Set(list.map((entry) => entry.kind));
  let status;
  if (list.length === 0) status = 'not-emitted';
  else if (kinds.has('fail')) status = 'fail';
  else if (kinds.has('pass')) status = 'pass';
  else if (kinds.has('unverified')) status = 'unverified';
  else if (kinds.has('webhook')) status = 'not-applicable-webhook';
  else status = 'not-applicable';
  const why = list.filter((entry) => entry.kind !== 'pass').map((entry) => `${entry.scenario}: ${entry.kind === 'fail' ? entry.message : entry.reason}`)[0] ?? '';
  return { id, status, why };
});

const count = (status) => rows.filter((row) => row.status === status).length;
const notApplicable = rows.filter((row) => row.status.startsWith('not-applicable') || row.status === 'not-emitted').length;
const summary = { total: declared.length, passed: count('pass'), applicable: declared.length - notApplicable, failed: count('fail'), unverified: count('unverified'), notApplicable, webhook: count('not-applicable-webhook'), notEmitted: count('not-emitted') };

const baselinePath = join(here, 'conformance-baseline.json');
let baseline = [];
try { baseline = JSON.parse(readFileSync(baselinePath, 'utf8')); } catch { /* first run */ }
const passing = new Set(rows.filter((row) => row.status === 'pass').map((row) => row.id));
const regressed = baseline.filter((id) => !passing.has(id));
if (process.env.UPDATE_BASELINE) writeFileSync(baselinePath, JSON.stringify([...passing].sort(), null, 2) + '\n');

const lines = [
  `## MCP Events conformance (PR 521 @ ${process.env.CONFORMANCE_REV ?? 'local'})`,
  '',
  `**${summary.passed} passed of ${summary.applicable} applicable, ${summary.total} declared rows.** ${summary.failed} failed, ${summary.unverified} unverified (need a fixture control attend does not ship), ${summary.webhook} webhook rows not applicable (v0.1.0 has no webhook), ${summary.notApplicable - summary.webhook} other rows not applicable.`,
  '',
  '| row | status | reason |',
  '| --- | --- | --- |',
  ...rows.filter((row) => row.status !== 'pass').map((row) => `| ${row.id} | ${row.status} | ${row.why.replaceAll('|', '/')} |`),
];
console.log(lines.join('\n'));
console.log('\n' + JSON.stringify(summary));
if (process.env.GITHUB_STEP_SUMMARY) appendFileSync(process.env.GITHUB_STEP_SUMMARY, lines.join('\n') + '\n');
if (process.env.CONFORMANCE_REPORT) writeFileSync(process.env.CONFORMANCE_REPORT, JSON.stringify({ summary, rows }, null, 2));
rmSync(dir, { recursive: true, force: true });
if (summary.failed > 0 || regressed.length > 0) {
  if (regressed.length) console.error(`rows that passed before and no longer do: ${regressed.join(', ')}`);
  process.exit(1);
}
