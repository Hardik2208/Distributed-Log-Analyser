const { Kafka } = require('kafkajs');
const pLimit = require('p-limit').default;

const { connectRedis, redisClient } = require('../config/redisClient');
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

// ======================================================
// 🔥 HIGH CONCURRENCY
// ======================================================
let concurrency = 100;
const limit = pLimit(concurrency);

// ======================================================
// 🔥 LOG SAMPLING (IMPORTANT)
// ======================================================
const SAMPLE_RATE = 0.01; // 1%

function shouldLog() {
  return Math.random() < SAMPLE_RATE;
}

// ======================================================
async function commitOffset(topic, partition, message) {
  await consumer.commitOffsets([{
    topic,
    partition,
    offset: (Number(message.offset) + 1).toString(),
  }]);
}

// ======================================================
async function run() {
  await connectRedis();
  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({ topic: 'logs', fromBeginning: false });

  console.log("🚀 MAIN CONSUMER STARTED");

  await consumer.run({
    autoCommit: false,
    partitionsConsumedConcurrently: 10,

    eachMessage: async ({ topic, partition, message, heartbeat }) => {

      await limit(async () => {

        let log;

        try {
          await heartbeat();

          log = JSON.parse(message.value.toString());

          const now = Date.now();
          const priority = log.priority || "LOW";

          // ======================================================
          // 🔴 SAMPLE INPUT VISIBILITY
          // ======================================================
          if (shouldLog()) {
            console.log(`📥 IN id=${log.id} p=${priority}`);
          }

          // ======================================================
          // 🔴 SYSTEM STATE (can later cache if needed)
          // ======================================================
          const [systemState, circuitState, avgLatency] = await Promise.all([
            redisClient.get("system:state"),
            redisClient.get("circuit:state"),
            redisClient.get("system:avg_latency")
          ]);

          const latency = Number(avgLatency || 0);

          const isOverloaded =
            circuitState === "OPEN" ||
            systemState === "OVERLOADED";

          const isExtreme = latency > 1500;

          // ======================================================
          // 🔴 LOAD SHEDDING
          // ======================================================
          if (isOverloaded) {

            if (priority === "LOW") {
              if (shouldLog()) {
                console.log(`🧹 DROP_LOW id=${log.id}`);
              }
              await commitOffset(topic, partition, message);
              return;
            }

            if (priority === "MEDIUM") {
              if (Math.random() > 0.2) {
                if (shouldLog()) {
                  console.log(`🧹 DROP_MED id=${log.id}`);
                }
                await commitOffset(topic, partition, message);
                return;
              }
            }

            if (priority === "HIGH" && isExtreme) {
              if (Math.random() > 0.2) {
                if (shouldLog()) {
                  console.log(`🧹 DROP_HIGH id=${log.id}`);
                }
                await commitOffset(topic, partition, message);
                return;
              }
            }
          }

          // ======================================================
          // 🔴 NORMALIZATION
          // ======================================================
          log.retry_count = log.retry_count || 0;

          log.ingestion_latency = now - log.timestamp;
          log.queue_entered_at = now;
          log.attempt_timestamp = now;

          // ======================================================
          // 🔥 EXECUTION
          // ======================================================
          const result = await processLog(log);

          if (result?.status === "SUCCESS") {
            if (shouldLog()) {
              console.log(`✅ SUCCESS id=${log.id}`);
            }
          }

          else if (result?.status === "ERROR_CLAIM_FAILED") {

            const allowed = await shouldRetry();

            // ======================================================
            // 🔴 CIRCUIT BREAKER CONTROL
            // ======================================================
            if (!allowed) {

              if (priority === "LOW") {
                await commitOffset(topic, partition, message);
                return;
              }

              if (priority === "MEDIUM") {
                if (Math.random() > 0.2) {
                  await commitOffset(topic, partition, message);
                  return;
                }
              }

              if (priority === "HIGH") {
                if (Math.random() > 0.5) {
                  await commitOffset(topic, partition, message);
                  return;
                }
              }
            }

            // ======================================================
            // 🔥 RETRY SEND
            // ======================================================
            const retryNow = Date.now();

            if (shouldLog()) {
              console.log(`📤 RETRY_OUT id=${log.id}`);
            }

            await producer.send({
              topic: 'logs-retry',
              messages: [{
                key: log.id,
                value: JSON.stringify({
                  ...log,
                  retry_count: 1,
                  retry_id: `${log.id}-r1`,
                  next_retry_at: retryNow + 1000,
                  retry_scheduled_at: retryNow,
                  queue_entered_at: retryNow,
                  source: "MAIN_RETRY"
                }),
              }],
            });
          }

        } catch (err) {

          if (!log) {
            await commitOffset(topic, partition, message);
            return;
          }

          const priority = log?.priority || "LOW";
          const now = Date.now();

          if (err.type === "TEMPORARY") {

            const currentRetry = log.retry_count || 0;
            const allowed = await shouldRetry();

            if (!allowed) {

              if (priority === "LOW") {
                await commitOffset(topic, partition, message);
                return;
              }

              if (priority === "MEDIUM") {
                if (Math.random() > 0.2) {
                  await commitOffset(topic, partition, message);
                  return;
                }
              }

              if (priority === "HIGH") {
                if (Math.random() > 0.5) {
                  await commitOffset(topic, partition, message);
                  return;
                }
              }
            }

            if (currentRetry >= 1) {

              if (shouldLog()) {
                console.log(`💀 DLQ_TEMP id=${log.id}`);
              }

              await producer.send({
                topic: 'logs-dlq',
                messages: [{
                  value: JSON.stringify({
                    log,
                    reason: "MAIN_MAX_RETRY",
                    failed_at: now
                  }),
                }],
              });

            } else {

              const jitter = Math.random() * 300;
              const retryNow = Date.now();

              if (shouldLog()) {
                console.log(`📤 RETRY_TEMP id=${log.id}`);
              }

              await producer.send({
                topic: 'logs-retry',
                messages: [{
                  key: log.id,
                  value: JSON.stringify({
                    ...log,
                    retry_count: 1,
                    retry_id: `${log.id}-r1`,
                    next_retry_at: retryNow + 1000 + jitter,
                    retry_scheduled_at: retryNow,
                    queue_entered_at: retryNow,
                    source: "MAIN_RETRY"
                  }),
                }],
              });
            }

          } else {

            if (shouldLog()) {
              console.log(`💀 DLQ_PERM id=${log.id}`);
            }

            await producer.send({
              topic: 'logs-dlq',
              messages: [{
                value: JSON.stringify({
                  log,
                  reason: err.type || "PERMANENT",
                  failed_at: now
                }),
              }],
            });
          }
        }

        // ======================================================
        // 🔥 ALWAYS COMMIT
        // ======================================================
        await commitOffset(topic, partition, message);

      });

    },
  });
}

run().catch(err => {
  console.error("🔥 MAIN CONSUMER FAILED:", err);
  process.exit(1);
});