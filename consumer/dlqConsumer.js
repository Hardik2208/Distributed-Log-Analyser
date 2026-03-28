const { Kafka } = require('kafkajs');
const { connectRedis, client: redis } = require('../config/redisClient');
const { addToSet } = require('../metrics/redisMetrics');

const kafka = new Kafka({
  clientId: 'dlq-consumer',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'log-group-dlq' });

async function run() {
  await connectRedis();

  await consumer.connect();

  await consumer.subscribe({ topic: 'logs-dlq', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      let log;

      try {
        log = JSON.parse(message.value.toString());
      } catch {
        console.log("❌ DLQ PARSE ERROR");
        return;
      }

      if (!log?.id) return;

      console.log("💀 DLQ:", log.id);

      // 🔴 Track DLQ
      await addToSet("dlq_ids", log.id);
    },
  });
}

run().catch(err => {
  console.error("🔥 DLQ CONSUMER CRASHED:", err);
});