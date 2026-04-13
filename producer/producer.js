const { Kafka } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');

const kafka = new Kafka({
  clientId: 'adaptive-producer',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer();
const controlConsumer = kafka.consumer({ groupId: 'producer-control' });

// --------------------------------
// CONTROL STATE
// --------------------------------
let baseRate = 10000;
let externalFactor = 1.0;

// 🔴 FIXED (REAL CONTROL RANGE)
const MIN_RATE = 10000;
const MAX_RATE = 10000;

// --------------------------------
// SERVICE CONFIG
// --------------------------------
const SERVICE = "order";

// --------------------------------
// PRIORITY CONFIG (NEW)
// --------------------------------
const PRIORITY_WEIGHTS = [
  { type: "HIGH", weight: 0.1 },   // critical logs
  { type: "MEDIUM", weight: 0.3 },
  { type: "LOW", weight: 0.6 }     // bulk logs
];

// --------------------------------
// ENDPOINT CONFIG
// --------------------------------
const VALID_ENDPOINTS = [
  { path: "/create", weight: 0.7 },
  { path: "/status", weight: 0.3 }
];

const INVALID_ENDPOINTS = [
  { path: "/update", weight: 0.5 },
  { path: "/fetch", weight: 0.5 }
];

const INVALID_RATE = 0.2;

// --------------------------------
// UTILS
// --------------------------------
function random(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function realisticLatency() {
  const r = Math.random();

  if (r < 0.7) return random(50, 120);
  if (r < 0.9) return random(120, 300);
  return random(300, 800);
}

function pickWeighted(arr) {
  const r = Math.random();
  let sum = 0;

  for (const e of arr) {
    sum += e.weight;
    if (r <= sum) return e.path;
  }
  return arr[0].path;
}

// 🔥 PRIORITY PICKER
function pickPriority() {
  const r = Math.random();
  let sum = 0;

  for (const p of PRIORITY_WEIGHTS) {
    sum += p.weight;
    if (r <= sum) return p.type;
  }
  return "LOW";
}

// 🔥 ENDPOINT PICKER
function pickEndpoint() {
  if (Math.random() < INVALID_RATE) {
    return pickWeighted(INVALID_ENDPOINTS);
  }
  return pickWeighted(VALID_ENDPOINTS);
}

// --------------------------------
// CONTROL SIGNAL CONSUMER
// --------------------------------
async function startControlConsumer() {
  await controlConsumer.connect();
  await controlConsumer.subscribe({
    topic: 'control-signals',
    fromBeginning: false
  });

  await controlConsumer.run({
    eachMessage: async ({ message }) => {
      try {
        const signal = JSON.parse(message.value.toString());

        if (signal.type === "RATE_ADJUST") {
          externalFactor = signal.factor;

          console.log(
            `🎯 CONTROL UPDATE → factor=${externalFactor.toFixed(2)}`
          );
        }

      } catch (err) {
        console.error("❌ CONTROL PARSE ERROR", err.message);
      }
    }
  });
}

// --------------------------------
// MAIN PRODUCER LOOP
// --------------------------------
async function run() {
  await producer.connect();
  await startControlConsumer();

  console.log("🚀 PRODUCER STARTED");

  while (true) {

    // --------------------------------
    // 1. APPLY CONTROL FACTOR (FIXED)
    // --------------------------------
    let adjustedRate = baseRate * externalFactor;

    adjustedRate = Math.max(MIN_RATE, Math.min(MAX_RATE, adjustedRate));

    const batchSize = Math.floor(adjustedRate);

    // --------------------------------
    // 2. BUILD BATCH (PRIORITY ADDED)
    // --------------------------------
    const batch = [];
    const now = Date.now();

    for (let i = 0; i < batchSize; i++) {

      const endpoint = pickEndpoint();
      const priority = pickPriority();

      const log = {
        id: uuidv4(),
        trace_id: uuidv4(),
        timestamp: now,

        service: SERVICE,
        endpoint: endpoint,
        method: "POST",

        status_code: 200,
        latency_ms: realisticLatency(),

        user_id: Math.random() < 0.02 ? null : "user_" + random(1, 5000),

        region: "ap-south-1",
        retry_count: 0,

        // 🔴 NEW: PRIORITY FIELD
        priority
      };

      batch.push({
        key: log.id,
        value: JSON.stringify(log)
      });
    }

    // --------------------------------
    // 3. SEND
    // --------------------------------
    try {
      await producer.send({
        topic: 'logs',
        messages: batch,
      });

      console.log(
        `📤 Sent=${batch.length} | factor=${externalFactor.toFixed(2)}`
      );

    } catch (err) {
      console.error("🔥 PRODUCER SEND FAILED:", err.message);
      await new Promise(res => setTimeout(res, 500));
      continue;
    }

    // --------------------------------
    // 4. CONTROLLED DRIFT
    // --------------------------------
    if (Math.random() < 0.2) {
      baseRate *= random(97, 103) / 100;
      baseRate = Math.max(MIN_RATE, Math.min(MAX_RATE, baseRate));
    }

    // --------------------------------
    // 5. STABLE INTERVAL
    // --------------------------------
    await new Promise(res => setTimeout(res, 1000));
  }
}

// --------------------------------
// START
// --------------------------------
run().catch(err => {
  console.error("🔥 PRODUCER CRASHED:", err);
  process.exit(1);
});