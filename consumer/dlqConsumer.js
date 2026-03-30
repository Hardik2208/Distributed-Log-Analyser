const { Kafka } = require('kafkajs');
const pLimit = require('p-limit');

const { connectRedis } = require('../config/redisClient');
const { addToDLQ, isDuplicate } = require('../metrics/redisMetrics');
// optional future: const db = require('../db/db');

const kafka = new Kafka({
  clientId: 'dlq-consumer',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'log-group-dlq' });

// 🔴 LIMIT DLQ PROCESSING
const limit = pLimit(20);

async function run() {
  console.log('🚀 DLQ CONSUMER STARTING...');

  await connectRedis();
  await consumer.connect();

  await consumer.subscribe({ topic: 'logs-dlq', fromBeginning: false });

  console.log('📡 SUBSCRIBED TO logs-dlq');

  await consumer.run({
    autoCommit: false,

    eachMessage: async ({ topic, partition, message }) => {

      await limit(async () => {

        let payload;

        try {
          payload = JSON.parse(message.value.toString());
        } catch (err) {
          console.log('❌ DLQ PARSE ERROR — skipping');
          return;
        }

        const log = payload.log || payload;
        const reason = payload.reason || 'UNKNOWN';

        if (!log?.id) {
          console.log('⚠️ INVALID DLQ MESSAGE — skipping');
          return;
        }

        // 🔴 IDEMPOTENCY (avoid duplicate DLQ writes)
        const already = await isDuplicate(`dlq:${log.id}`);
        if (already) {
          console.log(`⚠️ DUPLICATE DLQ ${log.id} — skipping`);
          return;
        }

        console.log(
          `💀 DLQ HIT: ${log.id} | reason=${reason} | retries=${log.retry_count || 0}`
        );

        try {
          // 🔴 REDIS STORE
          await addToDLQ(log, reason);

          // 🔴 OPTIONAL: DB STORE (future upgrade)
          // await db.insertDLQ(log, reason);

          // 🔴 MARK DLQ PROCESSED
          await markDLQProcessed(log.id);

          console.log(
            `✅ DLQ STORED: ${log.id} (${Date.now() - log.timestamp}ms total)`
          );

          // 🔴 COMMIT ONLY AFTER SUCCESS
          await consumer.commitOffsets([{
            topic,
            partition,
            offset: (Number(message.offset) + 1).toString(),
          }]);

        } catch (err) {
          console.error('🔥 DLQ STORE FAILED:', err.message);

          // 🔴 DO NOT COMMIT → retry later
        }

      });
    },
  });

  consumer.on('consumer.crash', event => {
    console.error('🔥 DLQ CONSUMER CRASHED:', event.payload);
  });

  consumer.on('consumer.disconnect', () => {
    console.warn('⚠️ DLQ CONSUMER DISCONNECTED');
  });
}

// 🔴 DLQ IDEMPOTENCY MARKER
async function markDLQProcessed(id) {
  const { client } = require('../config/redisClient');
  await client.set(`dlq:processed:${id}`, 1, { EX: 86400 });
}

run().catch(err => {
  console.error('🔥 DLQ CONSUMER FAILED TO START:', err);
});