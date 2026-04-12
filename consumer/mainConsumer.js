const { Kafka } = require('kafkajs');
const pLimit = require('p-limit').default;
const { recordAmplification } = require('../metrics/metricsService');
const { connectRedis } = require('../config/redisClient');
const { processLog } = require('../processing/processLog');

const kafka = new Kafka({
  clientId: 'main-consumer',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({
  groupId: 'log-group-main',
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
  maxWaitTimeInMs: 10,
  minBytes: 1,
});

const producer = kafka.producer();

// 🔥 CONCURRENCY
const limit = pLimit(100);

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
  await producer.connect();

  await consumer.subscribe({ topic: 'logs', fromBeginning: false });

  console.log("🚀 MAIN CONSUMER STARTED");

  await consumer.run({
    autoCommit: false,
    partitionsConsumedConcurrently: 3,

    eachMessage: async ({ topic, partition, message }) => {

      await limit(async () => {

        let log;

        try {
          log = JSON.parse(message.value.toString());

          // ----------------------
          // 🔥 ATTEMPT TRACKING (ORIGINAL)
          // ----------------------
          await recordAmplification(false);

          // ----------------------
          // 🔥 NORMALIZATION
          // ----------------------
          log.retry_count = log.retry_count || 0;
          log.ingestion_latency = Date.now() - log.timestamp;

          // 🔥 FIX: ATTEMPT TIMESTAMP (CRITICAL)
          log.attempt_timestamp = log.timestamp;

          // ----------------------
          // 🔥 PROCESS
          // ----------------------
          const result = await processLog(log);

          if (result?.status === "SUCCESS") {
            console.log(`✅ SUCCESS ${log.id}`);
          } 
          else if (result?.status === "SKIPPED_ALREADY_PROCESSED") {
            // duplicate — ignore
          }
          else if (result?.status === "ERROR_CLAIM_FAILED") {

            // ----------------------
            // 🔥 SAFE RETRY (CLAIM FAILURE)
            // ----------------------
            await producer.send({
              topic: 'logs-retry',
              messages: [{
                key: log.id,
                value: JSON.stringify({
                  ...log,
                  retry_count: 1,
                  retry_id: `${log.id}-r1`,
                  next_retry_at: Date.now() + 500,
                  attempt_timestamp: Date.now(), // 🔥 important
                  source: "CLAIM_FAILED"
                }),
              }],
            });

          } 
          else {
            console.log(`⚠️ UNKNOWN ${log.id}`);
          }

        } catch (err) {

          if (!log) {
            await commitOffset(topic, partition, message);
            return;
          }

          // ----------------------
          // 🔥 RETRY HANDLING
          // ----------------------
          if (err.type === "TEMPORARY") {

            const currentRetry = log.retry_count || 0;

            if (currentRetry >= 1) {
              console.log(`💀 DLQ ${log.id} retry overflow`);
              // retry consumer will take final decision
            } else {

              await producer.send({
                topic: 'logs-retry',
                messages: [{
                  key: log.id,
                  value: JSON.stringify({
                    ...log,
                    retry_count: 1,
                    retry_id: `${log.id}-r1`,
                    next_retry_at: Date.now() + 500,
                    attempt_timestamp: Date.now(), // 🔥 important
                    source: "MAIN_RETRY"
                  }),
                }],
              });

              console.log(`🔁 RETRY ${log.id} r=1`);
            }

          } else {
            // permanent → processLog already emitted final metric
            console.log(`💀 DLQ ${log.id} permanent`);
          }
        }

        // ----------------------
        // 🔥 ALWAYS COMMIT OFFSET
        // ----------------------
        await commitOffset(topic, partition, message);

      });

    },
  });
}

run().catch(err => {
  console.error("🔥 MAIN CONSUMER FAILED:", err);
  process.exit(1);
});