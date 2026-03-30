const { Kafka } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');

const kafka = new Kafka({
  clientId: 'adaptive-producer',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer();
const controlConsumer = kafka.consumer({ groupId: 'producer-control' });

// --------------------------------
// CONTROL STATE (EXTERNALIZED)
// --------------------------------
let baseRate = 1000;          // internal growth/shrink
let externalFactor = 1.0;     // from control loop

const MIN_RATE = 100;
const MAX_RATE = 5000;

// --------------------------------
// SERVICE CONFIG
// --------------------------------
const SERVICE = "order";

// --------------------------------
// UTILS
// --------------------------------
function random(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

// 🔴 REALISTIC LATENCY DISTRIBUTION
function realisticLatency() {
  const r = Math.random();

  if (r < 0.7) return random(50, 120);
  if (r < 0.9) return random(120, 300);
  return random(300, 800);
}

// 🔴 WEIGHTED ENDPOINTS
const endpoints = [
  { path: "/create", weight: 0.5 },
  { path: "/update", weight: 0.3 },
  { path: "/fetch", weight: 0.2 }
];

function pickEndpoint() {
  const r = Math.random();
  let sum = 0;

  for (const e of endpoints) {
    sum += e.weight;
    if (r <= sum) return e.path;
  }
  return "/create";
}

// --------------------------------
// 🔴 CONTROL SIGNAL CONSUMER
// --------------------------------
async function startControlConsumer() {
  await controlConsumer.connect();
  await controlConsumer.subscribe({ topic: 'control-signals', fromBeginning: false });

  await controlConsumer.run({
    eachMessage: async ({ message }) => {
      try {
        const signal = JSON.parse(message.value.toString());

        if (signal.type === "RATE_ADJUST") {
          externalFactor = signal.factor;

          console.log(
            `🎯 CONTROL SIGNAL → factor=${externalFactor}`
          );
        }

      } catch (err) {
        console.error("❌ CONTROL PARSE ERROR", err);
      }
    }
  });
}

// --------------------------------
// 🔴 MAIN PRODUCER LOOP
// --------------------------------
async function run() {
  await producer.connect();
  await startControlConsumer();

  while (true) {

    // --------------------------------
    // 1. APPLY CONTROL FACTOR
    // --------------------------------
    let adjustedRate = baseRate * externalFactor;

    adjustedRate = Math.max(MIN_RATE, Math.min(MAX_RATE, adjustedRate));

    // --------------------------------
    // 2. BURST TRAFFIC
    // --------------------------------
    let burstMultiplier = 1;

    if (Math.random() < 0.1) {
      burstMultiplier = random(2, 5);
      console.log(`🚀 BURST x${burstMultiplier}`);
    }

    const effectiveRate = Math.min(
      MAX_RATE,
      Math.floor(adjustedRate * burstMultiplier)
    );

    // --------------------------------
    // 3. BUILD BATCH
    // --------------------------------
    const batch = [];

    for (let i = 0; i < effectiveRate; i++) {

      const log = {
        id: uuidv4(),
        trace_id: uuidv4(),
        timestamp: Date.now(),

        service: SERVICE,
        endpoint: pickEndpoint(),
        method: "POST",

        status_code: 200,
        latency_ms: realisticLatency(),

        // slight bad data (real-world noise)
        user_id: Math.random() < 0.02 ? null : "user_" + random(1, 5000),

        region: "ap-south-1",
        retry_count: 0
      };

      batch.push({ value: JSON.stringify(log) });
    }

    // --------------------------------
    // 4. SEND
    // --------------------------------
    await producer.send({
      topic: 'logs',
      messages: batch,
    });

    console.log(
      `📤 Sent=${batch.length} | base=${Math.floor(baseRate)} | factor=${externalFactor.toFixed(2)}`
    );

    // --------------------------------
    // 5. NATURAL DRIFT (OPTIONAL BUT REALISTIC)
    // --------------------------------
    if (Math.random() < 0.3) {
      baseRate *= random(95, 105) / 100; // small drift
      baseRate = Math.max(MIN_RATE, Math.min(MAX_RATE, baseRate));
    }

    // --------------------------------
    // 6. VARIABLE DELAY
    // --------------------------------
    const delay = random(700, 1300);
    await new Promise(res => setTimeout(res, delay));
  }
}

// --------------------------------
// START
// --------------------------------
run().catch(err => {
  console.error("🔥 PRODUCER CRASHED:", err);
});