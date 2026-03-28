const { Kafka } = require('kafkajs');
const { connectRedis } = require('../config/redisClient');

const { processLog } = require('../processing/processLog');
const { classifyError } = require('../processing/classifier');

const kafka = new Kafka({
  clientId: 'main-consumer',
  brokers: ['localhost:9092'],
  requestTimeout: 30000,
  connectionTimeout: 30000
});

const consumer = kafka.consumer({
  groupId: 'log-group-main',
  sessionTimeout: 30000,
  heartbeatInterval: 3000
});

const producer = kafka.producer();

let isPaused = false;
const MAX_RETRIES = 5; // 🔴 ADD

async function run() {
  await connectRedis();

  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({
    topic: 'logs',
    fromBeginning: true
  });

  await consumer.run({
    autoCommit: false,

    eachMessage: async ({ topic, partition, message }) => {
      let log;

      try {
        log = JSON.parse(message.value.toString());
      } catch (err) {
        console.log("❌ PARSE ERROR");
        return;
      }

     log.source = "MAIN";

// 🔴 LIFECYCLE-LEVEL INGESTION TIMESTAMP (SET ONLY ONCE)
if (!log.first_processed_at) {
  log.first_processed_at = Date.now();
  console.log("⏱️ FIRST PROCESSING TIMESTAMP SET:", log.id);
}
      try {
        const now = Date.now();

// 🔴 END-TO-END LATENCY (existing behavior)
log.pipeline_latency = now - log.timestamp;

// 🔴 INGESTION LATENCY (NEW — just for visibility, not breaking anything)
log.ingestion_latency = log.first_processed_at - log.timestamp;

console.log(
  "📊 LATENCY:",
  "ingestion =", log.ingestion_latency,
  "ms | pipeline =", log.pipeline_latency, "ms"
);

await processLog(log);

      } catch (err) {

        console.log("🔥 ERROR CAUGHT:", err);

        let action;
        try {
          action = classifyError(err);
        } catch (e) {
          console.log("⚠️ CLASSIFIER FAILED, DEFAULTING TO RETRY");
          action = "RETRY";
        }

        console.log("🧠 CLASSIFIED AS:", action);

        if (err.type === "TEMPORARY") {
          action = "RETRY";
        }

        if (action === "RETRY") {

          // 🔴 ADD RETRY CAP
          if (log.retry_count >= MAX_RETRIES) {
            console.log("🚫 MAX RETRIES → DLQ");

            await sendToDLQ(log, "MAX_RETRIES_EXCEEDED");

          } else {
            log.retry_count += 1;

            console.log("🔁 SENDING TO RETRY");

            await producer.send({
              topic: 'logs-retry',
              messages: [{ value: JSON.stringify(log) }],
            });
          }

        } else if (action === "DLQ") {

          console.log("📦 SENT TO DLQ");

          await sendToDLQ(log, err.type || "UNKNOWN_ERROR");

        } else {

          console.log("⚠️ UNKNOWN ACTION → DEFAULT RETRY");

          if (log.retry_count >= MAX_RETRIES) {
            await sendToDLQ(log, "MAX_RETRIES_EXCEEDED");
          } else {
            log.retry_count += 1;

            await producer.send({
              topic: 'logs-retry',
              messages: [{ value: JSON.stringify(log) }],
            });
          }
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

global.pauseMainConsumer = async () => {
  if (!isPaused) {
    consumer.pause([{ topic: 'logs' }]);
    isPaused = true;
    console.log("⛔ MAIN CONSUMER PAUSED");
  }
};

global.resumeMainConsumer = async () => {
  if (isPaused) {
    consumer.resume([{ topic: 'logs' }]);
    isPaused = false;
    console.log("▶️ MAIN CONSUMER RESUMED");
  }
};

run().catch(err => {
  console.error("🔥 Consumer crashed:", err);
});