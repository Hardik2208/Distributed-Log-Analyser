const { Kafka } = require('kafkajs');
const pLimit = require('p-limit').default;

const { connectRedis, redisClient } = require('../config/redisClient');
const { addToDLQ } = require('../metrics/dlqService'); // ✅ FIXED

const kafka = new Kafka({
  clientId: 'dlq-consumer',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'log-group-dlq' });

const limit = pLimit(20);

// ----------------------
async function commitOffset(topic, partition, message) {
  await consumer.commitOffsets([{
    topic,
    partition,
    offset: (Number(message.offset) + 1).toString(),
  }]);
}

// ----------------------
async function run() {
  await connectRedis();
  await consumer.connect();

  await consumer.subscribe({ topic: 'logs-dlq', fromBeginning: false });

  console.log("🚀 DLQ CONSUMER STARTED");

  await consumer.run({
    autoCommit: false,

    eachMessage: async ({ topic, partition, message }) => {

      await limit(async () => {

        let payload;

        try {
          payload = JSON.parse(message.value.toString());
        } catch {
          console.log('❌ DLQ PARSE ERROR');
          await commitOffset(topic, partition, message);
          return;
        }

        const log = payload.log || payload;
        const reason = payload.reason || 'UNKNOWN';

        if (!log?.id) {
          await commitOffset(topic, partition, message);
          return;
        }

        // 🔥 IDEMPOTENCY (STRONG)
        const claimed = await redisClient.set(
          `dlq:processed:${log.id}`,
          "1",
          { NX: true, EX: 86400 }
        );

        if (!claimed) {
          await commitOffset(topic, partition, message);
          return;
        }

        try {
          await addToDLQ(log, {
            reason,
            source: payload.source || "DLQ_CONSUMER"
          });

          console.log(`💀 DLQ STORED ${log.id}`);

          await commitOffset(topic, partition, message);

        } catch (err) {

          console.error("🔥 DLQ PROCESSING ERROR:");
          console.error("➡️ ERROR:", err.message);

          // 🔴 DO NOT COMMIT → Kafka retry
        }

      });

    },
  });
}

run().catch(err => {
  console.error("🔥 DLQ CONSUMER FAILED:", err);
  process.exit(1);
});