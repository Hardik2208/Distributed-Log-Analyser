const { Kafka } = require('kafkajs');
const { connectRedis } = require('../config/redisClient');

const { processLog } = require('../processing/processLog');
const { classifyError } = require('../processing/classifier');

const kafka = new Kafka({
  clientId: 'retry-consumer',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'log-group-retry' });
const producer = kafka.producer();

const MAX_RETRIES = 5;

// 🔴 Exponential backoff with jitter
function getRetryDelay(retryCount) {
  const base = 500 * Math.pow(2, retryCount); // exponential
  const jitter = Math.random() * 300;         // jitter
  return Math.min(base + jitter, 5000);       // cap at 5s
}

async function run() {
  await connectRedis();

  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({ topic: 'logs-retry', fromBeginning: true });

  await consumer.run({
    autoCommit: false,

    eachMessage: async ({ topic, partition, message }) => {
      let log;

      // 🔴 1. Parse
      try {
        log = JSON.parse(message.value.toString());
      } catch {
        await sendToDLQ(message.value.toString(), "PARSE_ERROR");
        return commitOffset(topic, partition, message.offset);
      }

      const now = Date.now();

      // 🔴 2. Delay check (NO setTimeout)
      if (log.next_retry_at && now < log.next_retry_at) {
        // requeue for later
        await producer.send({
          topic: 'logs-retry',
          messages: [{ value: JSON.stringify(log) }],
        });

        return commitOffset(topic, partition, message.offset);
      }

      try {
  // 🔴 RECALCULATE (VERY IMPORTANT)
  const now = Date.now();
  log.pipeline_latency = now - log.timestamp;

  await processLog(log);

} catch (err) {
  const action = classifyError(err);

  if (action === "RETRY") {
    log.retry_count += 1;

    const base = 500 * Math.pow(2, log.retry_count);
    const jitter = Math.random() * 300;
    const delay = Math.min(base + jitter, 5000);

    setTimeout(async () => {
      await producer.send({
        topic: 'logs-retry',
        messages: [{ value: JSON.stringify(log) }],
      });
    }, delay);

  } else if (action === "DLQ") {
    await producer.send({
      topic: 'logs-dlq',
      messages: [{ value: JSON.stringify(log) }],
    });
  }
}

      // 🔴 6. Commit offset
      await commitOffset(topic, partition, message.offset);
    },
  });
}

// 🔴 Commit helper
async function commitOffset(topic, partition, offset) {
  await consumer.commitOffsets([
    {
      topic,
      partition,
      offset: (Number(offset) + 1).toString(),
    },
  ]);
}

// 🔴 DLQ helper
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

run().catch(err => {
  console.error("🔥 RETRY CONSUMER CRASHED:", err);
});