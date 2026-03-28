const { Kafka } = require('kafkajs');
const { connectRedis } = require('../config/redisClient');
const { addToSet } = require('../metrics/redisMetrics');

const kafka = new Kafka({
  clientId: 'dlq-consumer',
  brokers: ['localhost:9092'],
});

// 🔴 KEEP SAME GROUP (DO NOT CHANGE)
const consumer = kafka.consumer({ groupId: 'log-group-dlq' });

async function run() {
  console.log("🚀 DLQ CONSUMER STARTING...");

  await connectRedis();
  console.log("✅ REDIS CONNECTED");

  await consumer.connect();
  console.log("✅ KAFKA CONNECTED");

  await consumer.subscribe({
    topic: 'logs-dlq',
    fromBeginning: true
  });

  console.log("📡 SUBSCRIBED TO logs-dlq");

  await consumer.run({
    autoCommit: true, // DLQ safe to auto commit

    eachMessage: async ({ topic, partition, message }) => {

      console.log("📩 DLQ MESSAGE RECEIVED");

      let payload;

      try {
        payload = JSON.parse(message.value.toString());
      } catch (err) {
        console.log("❌ DLQ PARSE ERROR:", err);
        return;
      }

      // 🔴 HANDLE BOTH FORMATS (VERY IMPORTANT)
      // sometimes DLQ wraps inside { log, reason }
      const log = payload.log || payload;

      if (!log?.id) {
        console.log("⚠️ INVALID DLQ MESSAGE (NO ID)");
        return;
      }

      console.log("💀 DLQ HIT:", log.id);

      try {
        // 🔴 TRACK DLQ ENTRY
        await addToSet("dlq_ids", log.id);

        console.log("✅ DLQ STORED IN REDIS:", log.id);

      } catch (err) {
        console.log("🔥 REDIS ERROR (DLQ):", err);
      }
    },
  });

  // 🔴 KEEP PROCESS ALIVE + ERROR HANDLING
  consumer.on('consumer.crash', (event) => {
    console.error("🔥 DLQ CONSUMER CRASHED:", event.payload);
  });

  consumer.on('consumer.disconnect', () => {
    console.warn("⚠️ DLQ CONSUMER DISCONNECTED");
  });
}

run().catch(err => {
  console.error("🔥 DLQ CONSUMER FAILED TO START:", err);
});