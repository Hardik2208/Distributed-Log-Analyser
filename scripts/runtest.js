/**
 * Automated retry amplification test runner
 * Runs 4 scenarios back to back, waits for each to settle, prints comparison table
 *
 * Usage: node scripts/runTest.js
 * Requires: server running on :3000, Kafka + Redis up, consumers running
 */

const { Kafka } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');

const SERVER = 'http://localhost:3000';
const TOTAL_MESSAGES = 500;    // smaller per run so tests finish fast
const RATE_PER_SEC   = 50;
const SETTLE_MS      = 12000;  // wait after sending for retries to drain

const SERVICE_MAP = {
  auth:    ['/login',  '/signup'],
  payment: ['/pay',    '/refund'],
  order:   ['/create', '/status'],
};

function pick(arr) { return arr[Math.floor(Math.random() * arr.length)]; }
function rnd(min, max) { return Math.floor(Math.random() * (max - min + 1)) + min; }
function getStatus() {
  const r = Math.random();
  if (r < 0.7) return 200;
  if (r < 0.9) return 400;
  return 500;
}
function getLatency(service, status) {
  if (status >= 500) return rnd(200, 800);
  if (service === 'auth')    return rnd(20,  100);
  if (service === 'payment') return rnd(50,  200);
  if (service === 'order')   return rnd(100, 400);
  return rnd(50, 300);
}

async function setFailureRate(rate) {
  const res = await fetch(`${SERVER}/test/failure-rate`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ rate }),
  });
  return res.json();
}

async function getMetrics(service) {
  const res = await fetch(`${SERVER}/metrics?service=${service}`);
  return res.json();
}

async function sendBatch(producer, count, label) {
  let sent = 0;
  return new Promise((resolve) => {
    const iv = setInterval(async () => {
      for (let i = 0; i < RATE_PER_SEC; i++) {
        if (sent >= count) { clearInterval(iv); return resolve(); }
        const service  = pick(Object.keys(SERVICE_MAP));
        const endpoint = pick(SERVICE_MAP[service]);
        const status   = getStatus();
        await producer.send({
          topic: 'logs',
          messages: [{
            value: JSON.stringify({
              id: uuidv4(),
              trace_id: uuidv4(),
              timestamp: Date.now(),
              service, endpoint,
              method: pick(['GET', 'POST']),
              status_code: status,
              latency_ms: getLatency(service, status),
              user_id: 'user_' + rnd(1, 1000),
              region: pick(['ap-south-1', 'us-east-1', 'eu-west-1']),
              retry_count: 0,
            }),
          }],
        });
        sent++;
      }
      process.stdout.write(`\r[${label}] Sent ${sent}/${count}`);
    }, 1000);
  });
}

async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function summarise(metricsResp, label) {
  if (!metricsResp?.windows?.length) return { label, error: 'no data' };
  // aggregate across all windows
  const totals = metricsResp.windows.reduce((acc, w) => {
    acc.request_count         += w.request_count         || 0;
    acc.original_count        += w.original_count        || 0;
    acc.retry_count           += w.retry_count           || 0;
    acc.error_count           += w.error_count           || 0;
    acc.total_pipeline_latency += (w.avg_pipeline_latency * (w.request_count || 0));
    acc.total_ingestion_latency += (w.avg_ingestion_latency * (w.request_count || 0));
    return acc;
  }, { request_count: 0, original_count: 0, retry_count: 0, error_count: 0, total_pipeline_latency: 0, total_ingestion_latency: 0 });

  return {
    label,
    total_msgs:          totals.request_count,
    original:            totals.original_count,
    retried:             totals.retry_count,
    amplification:       totals.original_count > 0
      ? (totals.retry_count / totals.original_count).toFixed(2) + 'x'
      : 'N/A',
    failure_rate:        totals.request_count > 0
      ? ((totals.error_count / totals.request_count) * 100).toFixed(1) + '%'
      : 'N/A',
    avg_pipeline_ms:     totals.request_count > 0
      ? Math.round(totals.total_pipeline_latency  / totals.request_count) + 'ms'
      : 'N/A',
    avg_ingestion_ms:    totals.request_count > 0
      ? Math.round(totals.total_ingestion_latency / totals.request_count) + 'ms'
      : 'N/A',
  };
}

async function run() {
  const kafka    = new Kafka({ clientId: 'test-runner', brokers: ['localhost:9092'] });
  const producer = kafka.producer();
  await producer.connect();

  const SCENARIOS = [
    { label: 'Baseline  5% failure',  rate: 0.05 },
    { label: 'Baseline 10% failure',  rate: 0.10 },
    { label: 'Medium   20% failure',  rate: 0.20 },
    { label: 'Stress   40% failure',  rate: 0.40 },
  ];

  const results = [];

  for (const scenario of SCENARIOS) {
    console.log(`\n\n${'─'.repeat(60)}`);
    console.log(`▶ SCENARIO: ${scenario.label}`);
    console.log(`${'─'.repeat(60)}`);

    await setFailureRate(scenario.rate);
    console.log(`   failure rate set to ${scenario.rate * 100}%`);

    await sendBatch(producer, TOTAL_MESSAGES, scenario.label);
    console.log(`\n   waiting ${SETTLE_MS / 1000}s for retries to drain...`);
    await sleep(SETTLE_MS);

    const metricsAuth    = await getMetrics('auth');
    const metricsPayment = await getMetrics('payment');
    const metricsOrder   = await getMetrics('order');

    results.push(summarise(metricsAuth,    scenario.label + ' [auth]'));
    results.push(summarise(metricsPayment, scenario.label + ' [payment]'));
    results.push(summarise(metricsOrder,   scenario.label + ' [order]'));
  }

  await producer.disconnect();

  // ── Print results table ──────────────────────────────────────────────────
  console.log('\n\n' + '═'.repeat(110));
  console.log('TEST RESULTS');
  console.log('═'.repeat(110));
  console.log(
    'Scenario'.padEnd(36),
    'Msgs'.padStart(6),
    'Orig'.padStart(6),
    'Retried'.padStart(8),
    'Amplif.'.padStart(9),
    'Fail%'.padStart(7),
    'Avg pipeline'.padStart(14),
    'Avg ingest'.padStart(12),
  );
  console.log('─'.repeat(110));
  for (const r of results) {
    if (r.error) { console.log(r.label.padEnd(36), r.error); continue; }
    console.log(
      r.label.padEnd(36),
      String(r.total_msgs).padStart(6),
      String(r.original).padStart(6),
      String(r.retried).padStart(8),
      r.amplification.padStart(9),
      r.failure_rate.padStart(7),
      r.avg_pipeline_ms.padStart(14),
      r.avg_ingestion_ms.padStart(12),
    );
  }
  console.log('═'.repeat(110));

  // DLQ summary
  try {
    const dlq = await (await fetch(`${SERVER}/metrics/dlq`)).json();
    console.log('\nDLQ Summary:');
    console.log('  Total DLQ entries:        ', dlq.count);
    console.log('  Avg retries before DLQ:   ', dlq.avg_retries_before_dlq);
    console.log('  Avg time in pipeline (ms):', dlq.avg_time_in_pipeline_ms);
    console.log('  By reason:                ', JSON.stringify(dlq.by_reason));
  } catch { console.log('\n  (DLQ stats unavailable)'); }
}

run().catch(err => {
  console.error('🔥 TEST RUNNER FAILED:', err);
  process.exit(1);
});