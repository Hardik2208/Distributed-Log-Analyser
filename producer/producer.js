const { Kafka } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');

// --------------------------------
// ENV VALIDATION
// --------------------------------
const broker = process.env.KAFKA_BROKER;

if (!broker) {
  throw new Error("❌ KAFKA_BROKER is not defined");
}

console.log("🚀 Connecting to Kafka:", broker);

// --------------------------------
// KAFKA INIT
// --------------------------------
const kafka = new Kafka({
  clientId: 'adaptive-producer',
  brokers: [broker],
});

// --------------------------------
// CLIENTS
// --------------------------------
const producer = kafka.producer();
const controlConsumer = kafka.consumer({
  groupId: 'producer-control',
});

// --------------------------------
// CONNECTION HELPERS
// --------------------------------
async function connectProducer(retries = 5) {
  try {
    await producer.connect();
    console.log("✅ Producer connected");
  } catch (err) {
    console.log(`⏳ Retrying producer connect... (${retries})`);
    if (retries === 0) throw err;
    await new Promise(res => setTimeout(res, 3000));
    return connectProducer(retries - 1);
  }
}

async function connectControlConsumer(retries = 5) {
  try {
    await controlConsumer.connect();
    console.log("✅ Control consumer connected");
  } catch (err) {
    console.log(`⏳ Retrying control consumer... (${retries})`);
    if (retries === 0) throw err;
    await new Promise(res => setTimeout(res, 3000));
    return connectControlConsumer(retries - 1);
  }
}

// --------------------------------
// CONTROL STATE
// --------------------------------
let baseRate = 1000;          // 🔥 FIXED to 1k/sec
let externalFactor = 1.0;

const MIN_RATE = 1000;
const MAX_RATE = 1000;

// --------------------------------
// SERVICE CONFIG
// --------------------------------
const SERVICE = "order";

// --------------------------------
// PRIORITY CONFIG
// --------------------------------
const PRIORITY_WEIGHTS = [
  { type: "HIGH", weight: 0.1 },
  { type: "MEDIUM", weight: 0.3 },
  { type: "LOW", weight: 0.6 }
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

function pickPriority() {
  const r = Math.random();
  let sum = 0;

  for (const p of PRIORITY_WEIGHTS) {
    sum += p.weight;
    if (r <= sum) return p.type;
  }
  return "LOW";
}

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
  await controlConsumer.subscribe({
    topic: 'control-signals',
    fromBeginning: false
  });

  console.log("🎧 Control consumer subscribed");

  await controlConsumer.run({
    eachMessage: async ({ message }) => {
      try {
        const signal = JSON.parse(message.value.toString());

        if (signal.type === "RATE_ADJUST") {
          externalFactor = signal.factor;
          console.log(`🎯 CONTROL UPDATE → factor=${externalFactor.toFixed(2)}`);
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
  await connectProducer();
  await connectControlConsumer();
  await startControlConsumer();

  console.log("🚀 PRODUCER STARTED (1K/sec)");

  const MAX_BATCH = 300;   // 🔥 SAFE LIMIT

  while (true) {

    let adjustedRate = baseRate * externalFactor;
    adjustedRate = Math.max(MIN_RATE, Math.min(MAX_RATE, adjustedRate));

    const totalMessages = Math.floor(adjustedRate);

    const now = Date.now();
    const batch = [];

    for (let i = 0; i < totalMessages; i++) {

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

        priority
      };

      batch.push({
        key: log.id,
        value: JSON.stringify(log)
      });
    }

    try {
      // 🔥 SPLIT INTO SAFE CHUNKS
      for (let i = 0; i < batch.length; i += MAX_BATCH) {
        const chunk = batch.slice(i, i + MAX_BATCH);

        await producer.send({
          topic: 'logs',
          messages: chunk,
        });
      }

      console.log(`📤 Sent=${totalMessages} (chunked)`);

    } catch (err) {
      console.error("🔥 PRODUCER SEND FAILED:", err.message);
      await new Promise(res => setTimeout(res, 500));
      continue;
    }

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