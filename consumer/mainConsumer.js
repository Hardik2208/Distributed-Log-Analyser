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

// 🔴 MAIN RUN FUNCTION
async function run() {
  await connectRedis();

  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({ topic: 'logs', fromBeginning: true });

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

      try {
        // 🔴 PIPELINE LATENCY
        log.pipeline_latency = Date.now() - log.timestamp;

        await processLog(log);

      } catch (err) {
        const action = classifyError(err);

        if (action === "RETRY") {
          log.retry_count += 1;

          await producer.send({
            topic: 'logs-retry',
            messages: [{ value: JSON.stringify(log) }],
          });

        } else if (action === "DLQ") {
          await sendToDLQ(log, err.type || "UNKNOWN_ERROR");
        }
      }

      await commitOffset(topic, partition, message.offset);
    },
  });
}

// 🔴 COMMIT HELPER
async function commitOffset(topic, partition, offset) {
  await consumer.commitOffsets([
    {
      topic,
      partition,
      offset: (Number(offset) + 1).toString(),
    },
  ]);
}

// 🔴 DLQ HELPER
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

// 🔴 START
run().catch(err => {
  console.error("🔥 Consumer crashed:", err);
});