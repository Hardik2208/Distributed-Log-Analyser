const { Kafka } = require('kafkajs');
const pLimit = require('p-limit').default;

const { connectRedis } = require('../config/redisClient');
const { processLog } = require('../processing/processLog');
const { shouldRetry } = require('../control/circuitBreaker');

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
          // 🔥 NORMALIZATION
          // ----------------------
          log.retry_count = log.retry_count || 0;

          const now = Date.now();

          // ingestion latency
          log.ingestion_latency = now - log.timestamp;

          // 🔥 NEW: PIPELINE ENTRY (CRITICAL)
          if (!log.queue_entered_at) {
            log.queue_entered_at = log.timestamp;
          }

          // 🔥 ATTEMPT START (ONLY FOR EXECUTION)
          log.attempt_timestamp = now;

          // ----------------------
          // 🔥 PROCESS
          // ----------------------
          const result = await processLog(log);

          if (result?.status === "SUCCESS") {
            console.log(`✅ SUCCESS ${log.id}`);
          } 
          else if (result?.status === "SKIPPED_ALREADY_PROCESSED") {
            // ignore
          }
          else if (result?.status === "ERROR_CLAIM_FAILED") {

            const allowed = await shouldRetry();

            if (!allowed) {
              console.log(`⛔ CIRCUIT BLOCKED RETRY ${log.id}`);
              return;
            }

            const retryNow = Date.now();

            await producer.send({
              topic: 'logs-retry',
              messages: [{
                key: log.id,
                value: JSON.stringify({
                  ...log,
                  retry_count: 1,
                  retry_id: `${log.id}-r1`,
                  next_retry_at: retryNow + 500,

                  // 🔥 CRITICAL TIMING
                  retry_scheduled_at: retryNow,

                  // 🔥 NEW: RESET PIPELINE ENTRY FOR RETRY
                  queue_entered_at: retryNow,

                  // ❌ DO NOT SET attempt_timestamp HERE

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
          // 🔥 TEMPORARY FAILURE
          // ----------------------
          if (err.type === "TEMPORARY") {

            const currentRetry = log.retry_count || 0;
            const now = Date.now();

            const allowed = await shouldRetry();

            if (!allowed) {
              console.log(`⛔ CIRCUIT BLOCKED RETRY ${log.id}`);
              return;
            }

            if (currentRetry >= 1) {
              console.log(`💀 DLQ ${log.id} retry overflow`);
            } else {

              const retryNow = Date.now();

              await producer.send({
                topic: 'logs-retry',
                messages: [{
                  key: log.id,
                  value: JSON.stringify({
                    ...log,
                    retry_count: 1,
                    retry_id: `${log.id}-r1`,
                    next_retry_at: retryNow + 500,

                    // 🔥 CRITICAL TIMING
                    retry_scheduled_at: retryNow,

                    // 🔥 NEW: RESET PIPELINE ENTRY
                    queue_entered_at: retryNow,

                    // ❌ DO NOT SET attempt_timestamp HERE

                    source: "MAIN_RETRY"
                  }),
                }],
              });

              console.log(`🔁 RETRY ${log.id} r=1`);
            }

          } else {
            console.log(`💀 DLQ ${log.id} permanent`);
          }
        }

        // ----------------------
        // 🔥 ALWAYS COMMIT
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