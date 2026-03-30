/**
 * Backpressure and chaos stress test
 * Fires messages while triggering throttle/pause mid-run to observe system behaviour
 *
 * Usage: node scripts/stressTest.js [scenario]
 *   node scripts/stressTest.js throttle   — enables throttle at 50% through the run
 *   node scripts/stressTest.js pause      — pauses main consumer for 15s mid-run
 *   node scripts/stressTest.js both       — throttle + pause together
 *   node scripts/stressTest.js burst      — 5x message rate spike
 */

const { Kafka } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');

const SERVER   = 'http://localhost:3000';
const scenario = process.argv[2] || 'throttle';

const kafka    = new Kafka({ clientId: 'stress-tester', brokers: ['localhost:9092'] });
const producer = kafka.producer();

const SERVICE_MAP = {
  auth:    ['/login', '/signup'],
  payment: ['/pay',   '/refund'],
  order:   ['/create','/status'],
};

function pick(a) { return a[Math.floor(Math.random() * a.length)]; }
function rnd(min, max) { return Math.floor(Math.random() * (max - min + 1)) + min; }

async function post(path) {
  try {
    const r = await fetch(`${SERVER}${path}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' });
    return r.json();
  } catch (e) { return { error: e.message }; }
}

async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function sendMessages(count, rate, label) {
  let sent = 0;
  return new Promise(resolve => {
    const iv = setInterval(async () => {
      const batch = Math.min(rate, count - sent);
      for (let i = 0; i < batch; i++) {
        const service  = pick(Object.keys(SERVICE_MAP));
        const endpoint = pick(SERVICE_MAP[service]);
        const r        = Math.random();
        const status   = r < 0.7 ? 200 : r < 0.9 ? 400 : 500;
        await producer.send({
          topic: 'logs',
          messages: [{
            value: JSON.stringify({
              id: uuidv4(), trace_id: uuidv4(),
              timestamp: Date.now(),
              service, endpoint,
              method: pick(['GET','POST']),
              status_code: status,
              latency_ms: rnd(20, 400),
              user_id: 'user_' + rnd(1, 1000),
              region: pick(['ap-south-1','us-east-1','eu-west-1']),
              retry_count: 0,
            }),
          }],
        });
        sent++;
      }
      process.stdout.write(`\r[${label}] sent ${sent}/${count}`);
      if (sent >= count) { clearInterval(iv); resolve(); }
    }, 1000);
  });
}

async function snapshot(label) {
  try {
    const r = await fetch(`${SERVER}/metrics/all`);
    const d = await r.json();
    const w = d.windows || [];
    const agg = w.reduce((a, v) => {
      a.rc  += v.request_count        || 0;
      a.oc  += v.original_count       || 0;
      a.rtc += v.retry_count          || 0;
      a.pl  += (v.avg_pipeline_latency  || 0) * (v.request_count || 0);
      return a;
    }, { rc: 0, oc: 0, rtc: 0, pl: 0 });
    const amp = agg.oc > 0 ? (agg.rtc / agg.oc).toFixed(2) : 'N/A';
    const avgPl = agg.rc > 0 ? Math.round(agg.pl / agg.rc) : 0;
    console.log(`\n📸 SNAPSHOT [${label}]: msgs=${agg.rc} orig=${agg.oc} retried=${agg.rtc} amp=${amp}x avg_pipeline=${avgPl}ms`);
  } catch (e) { console.log('\n  (snapshot failed:', e.message, ')'); }
}

async function runThrottle() {
  console.log('\n🔥 SCENARIO: THROTTLE MID-RUN');
  console.log('   Send 400 msgs, enable throttle halfway, observe amplification + latency climb');

  await sendMessages(200, 50, 'pre-throttle');
  await snapshot('before throttle');

  console.log('\n⚠️  ENABLING THROTTLE...');
  console.log(await post('/test/throttle'));

  await sendMessages(200, 50, 'throttled');
  await snapshot('during throttle');
  await sleep(8000);

  console.log('\n✅ DISABLING THROTTLE...');
  console.log(await post('/test/unthrottle'));
  await sleep(10000);
  await snapshot('after drain');
}

async function runPause() {
  console.log('\n🔥 SCENARIO: PAUSE MAIN CONSUMER MID-RUN');
  console.log('   Pause main consumer for 15s — Kafka lag builds, then observe burst on resume');

  await sendMessages(200, 50, 'before-pause');
  await snapshot('before pause');

  console.log('\n⛔ PAUSING MAIN CONSUMER (15s)...');
  await post('/test/pause?ms=15000');

  // keep sending during the pause — these will queue in Kafka
  await sendMessages(200, 50, 'during-pause');
  await snapshot('during pause (kafka lag building)');

  console.log('\n   waiting for auto-resume + drain...');
  await sleep(20000);
  await snapshot('after resume + drain');
}

async function runBurst() {
  console.log('\n🔥 SCENARIO: MESSAGE BURST — 5x rate spike');
  console.log('   Normal rate 50/s → spike to 250/s → back to normal');

  await sendMessages(200, 50, 'normal-rate');
  await snapshot('before burst');

  console.log('\n💥 BURST: 5x rate for 5 seconds...');
  await sendMessages(250, 250, 'burst');
  await snapshot('during burst');

  await sleep(15000);
  await snapshot('post-burst drain');
}

async function run() {
  await producer.connect();
  console.log('✅ Producer connected');

  if (scenario === 'throttle') await runThrottle();
  else if (scenario === 'pause')  await runPause();
  else if (scenario === 'burst')  await runBurst();
  else if (scenario === 'both') {
    await runThrottle();
    await sleep(5000);
    await runPause();
  } else {
    console.log(`Unknown scenario: ${scenario}. Use: throttle | pause | burst | both`);
  }

  await producer.disconnect();
  console.log('\n\n✅ Stress test complete');
}

run().catch(err => {
  console.error('🔥 STRESS TEST FAILED:', err);
  process.exit(1);
});