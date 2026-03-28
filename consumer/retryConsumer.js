const { Kafka } = require('kafkajs');
const { connectRedis } = require('../config/redisClient');

const { processLog } = require('../processing/processLog');
const { classifyError } = require('../processing/classifier');

const kafka = new Kafka({
  clientId: 'retry-consumer',
  brokers: ['localhost:9092'],
});

// 🔴 FIXED GROUP (IMPORTANT)
const consumer = kafka.consumer({ groupId: 'log-group-retry' });

const producer = kafka.producer();

const MAX_RETRIES = 5;

let isRetryThrottled = false;

// 🔴 SINGLE SOURCE OF TRUTH
function getRetryDelay(retryCount) {
  const base = 500 * Math.pow(2, retryCount);
  const jitter = Math.random() * 300;
  return Math.min(base + jitter, 5000);
}

async function run() {
  console.log("🚀 RETRY CONSUMER STARTED");

  await connectRedis();

  await consumer.connect();
  console.log("✅ RETRY CONSUMER CONNECTED");

  await producer.connect();

  await consumer.subscribe({ topic: 'logs-retry', fromBeginning: true });
  console.log("📡 SUBSCRIBED TO logs-retry");

  await consumer.run({
    autoCommit: false,

    eachMessage: async ({ topic, partition, message }) => {

      console.log("📩 RETRY MESSAGE RECEIVED");

      let log;

      try {
        log = JSON.parse(message.value.toString());
      } catch {
        console.log("❌ PARSE ERROR");
        await sendToDLQ(message.value.toString(), "PARSE_ERROR");
        return commitOffset(topic, partition, message.offset);
      }

      log.source = "RETRY";

      console.log("🔁 RETRY PROCESSING:", log.id, "retry:", log.retry_count);

      const now = Date.now();

      if (log.next_retry_at && now < log.next_retry_at) {
        console.log("⏳ DELAY NOT REACHED → REQUEUE");

        await producer.send({
          topic: 'logs-retry',
          messages: [{ value: JSON.stringify(log) }],
        });

        return commitOffset(topic, partition, message.offset);
      }

      try {
        log.pipeline_latency = Date.now() - log.timestamp;

        console.log("⚙️ CALLING processLog (RETRY)");

        await processLog(log);

        console.log("✅ RETRY SUCCESS:", log.id);

      } catch (err) {

        console.log("🔥 RETRY ERROR:", err);

        let action;
        try {
          action = classifyError(err);
        } catch {
          console.log("⚠️ CLASSIFIER FAILED → DEFAULT RETRY");
          action = "RETRY";
        }

        console.log("🧠 RETRY CLASSIFIED AS:", action);

        if (err.type === "TEMPORARY") {
          action = "RETRY";
        }

        if (action === "RETRY" && log.retry_count < MAX_RETRIES) {
          log.retry_count += 1;

          // 🔴 FIX: USE CENTRAL FUNCTION
          const delay = getRetryDelay(log.retry_count);
          const effectiveDelay = isRetryThrottled ? delay * 2 : delay;

          console.log("🔁 RE-ENQUEUE RETRY IN", effectiveDelay, "ms");

          setTimeout(async () => {
            await producer.send({
              topic: 'logs-retry',
              messages: [{ value: JSON.stringify(log) }],
            });
          }, effectiveDelay);

        } else {
          console.log("📦 MOVING TO DLQ");

          await producer.send({
            topic: 'logs-dlq',
            messages: [{ value: JSON.stringify(log) }],
          });
        }
      }

      await commitOffset(topic, partition, message.offset);
    },
  });
}

async function commitOffset(topic, partition, offset) {
  await consumer.commitOffsets([
    {
      topic,
      partition,
      offset: (Number(offset) + 1).toString(),
    },
  ]);
}

async function sendToDLQ(log, reason) {
  await producer.send({
    topic: 'logs-dlq',
    messages: [
      {
        value: JSON.stringify({
          log,
          reason,
          failed_at: Date.now()
        }),
      },
    ],
  });
}

global.throttleRetryConsumer = () => {
  isRetryThrottled = true;
  console.log("⚠️ RETRY THROTTLING ENABLED");
};

global.unthrottleRetryConsumer = () => {
  isRetryThrottled = false;
  console.log("✅ RETRY THROTTLING DISABLED");
};

run().catch(err => {
  console.error("🔥 RETRY CONSUMER CRASHED:", err);
});