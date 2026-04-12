const { Kafka } = require('kafkajs');
const pLimit = require('p-limit').default;

const { connectRedis } = require('../config/redisClient');
const { addToDLQ, isDuplicate } = require('../metrics/redisMetrics');

const kafka = new Kafka({
  clientId: 'dlq-consumer',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'log-group-dlq' });

// 🔥 CONTROLLED CONCURRENCY
const limit = pLimit(20);

// ----------------------
async function run() {
  await connectRedis();
  await consumer.connect();

  await consumer.subscribe({ topic: 'logs-dlq', fromBeginning: false });

  console.log("🚀 DLQ CONSUMER STARTED");

  await consumer.run({
    autoCommit: false,
    partitionsConsumedConcurrently: 1,

    eachMessage: async ({ topic, partition, message }) => {

  limit(async () => {

    try {

      let payload;

      try {
        payload = JSON.parse(message.value.toString());
      } catch {
        console.log('❌ DLQ PARSE ERROR');
        return;
      }

      const log = payload.log || payload;
      const reason = payload.reason || 'UNKNOWN';

      if (!log?.id) return;

      const duplicate = await isDuplicate(`dlq:${log.id}`);
      if (duplicate) return;

      await addToDLQ(log, reason);

      console.log(`💀 DLQ STORED ${log.id} | ${reason}`);

      await consumer.commitOffsets([{
        topic,
        partition,
        offset: (Number(message.offset) + 1).toString(),
      }]);

    } catch (err) {
      console.error("🔥 DLQ PROCESSING ERROR:", err.message);
      // DO NOT CRASH — just log
    }

  });

}
  });
}

run().catch(err => {
  console.error("🔥 DLQ CONSUMER FAILED:", err);
  process.exit(1);
});