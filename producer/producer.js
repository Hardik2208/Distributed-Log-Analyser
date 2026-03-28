const { Kafka } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');

const kafka = new Kafka({
  clientId: 'load-producer',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer();

const TOTAL_MESSAGES = 1000;
const RATE_PER_SEC = 100;

// 🔴 Service → Endpoint mapping
const SERVICE_MAP = {
  auth: ["/login", "/signup"],
  payment: ["/pay", "/refund"],
  order: ["/create", "/status"]
};

// 🔴 Utility functions
function pick(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function random(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

// 🔴 Controlled status distribution
function getStatus() {
  const r = Math.random();
  if (r < 0.7) return 200;   // 70%
  if (r < 0.9) return 400;   // 20%
  return 500;                // 10%
}

// 🔴 Latency correlated with service + status
function getLatency(service, status) {
  if (status >= 500) return random(200, 800);

  if (service === "auth") return random(20, 100);
  if (service === "payment") return random(50, 200);
  if (service === "order") return random(100, 400);

  return random(50, 300); // fallback
}

async function run() {
  await producer.connect();

  let sent = 0;

  const interval = setInterval(async () => {
    for (let i = 0; i < RATE_PER_SEC; i++) {
      if (sent >= TOTAL_MESSAGES) {
        clearInterval(interval);
        console.log("✅ Load test completed");
        await producer.disconnect();
        return;
      }

      const service = pick(Object.keys(SERVICE_MAP));
      const endpoint = pick(SERVICE_MAP[service]);
      const status = getStatus();

      const log = {
        id: uuidv4(),
        trace_id: uuidv4(),

        timestamp: Date.now(),

        service,
        endpoint,
        method: pick(["GET", "POST"]),

        status_code: status,
        latency_ms: getLatency(service, status),

        user_id: "user_" + random(1, 1000),
        region: pick(["ap-south-1", "us-east-1", "eu-west-1"]),

        retry_count: 0
      };

      await producer.send({
        topic: 'logs',
        messages: [{ value: JSON.stringify(log) }],
      });

      sent++;
    }

    console.log(`Sent: ${sent}`);
  }, 1000);
}

run().catch(err => {
  console.error("🔥 PRODUCER CRASHED:", err);
});