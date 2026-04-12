const { Kafka } = require('kafkajs');
const pLimit = require('p-limit').default;
const { redisClient, connectRedis } = require('../config/redisClient');
const { processLog } = require('../processing/processLog');
const { shouldRetry } = require('../control/circuitBreaker');
const { recordAmplification } = require('../metrics/metricsService');

const kafka = new Kafka({
  clientId: 'retry-consumer',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({
  groupId: 'log-group-retry',
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
  maxWaitTimeInMs: 10,
  minBytes: 1,
});

const producer = kafka.producer();

// ----------------------
// 🔥 DYNAMIC CONCURRENCY
// ----------------------
let dynamicConcurrency = 20;
let limiter = pLimit(dynamicConcurrency);

setInterval(() => {
  try {
    const state = cachedState.systemState;

    let newConcurrency = 20;

    if (state === "OVERLOADED") newConcurrency = 5;
    else if (state === "PRESSURED") newConcurrency = 10;

    if (newConcurrency !== dynamicConcurrency) {
      dynamicConcurrency = newConcurrency;
      limiter = pLimit(dynamicConcurrency);
      console.log(`🔁 RETRY CONCURRENCY → ${dynamicConcurrency}`);
    }
  } catch {}
}, 1000);

const limit = (fn) => limiter(fn);

// ----------------------
// 🔥 CACHE SYSTEM STATE
// ----------------------
let cachedState = {
  systemState: "HEALTHY",
  circuitState: "CLOSED",
  avgLatency: 0
};

setInterval(async () => {
  try {
    const [systemState, circuitState, avgLatency] = await Promise.all([
      redisClient.get("system:state"),
      redisClient.get("circuit:state"),
      redisClient.get("system:avg_latency")
    ]);

    cachedState.systemState = systemState || "HEALTHY";
    cachedState.circuitState = circuitState || "CLOSED";
    cachedState.avgLatency = Number(avgLatency || 0);
  } catch {}
}, 1000);

// ----------------------
const MAX_RETRY = 2;
const LATENCY_THRESHOLD = 2000;

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

  await consumer.subscribe({ topic: 'logs-retry', fromBeginning: false });

  console.log("🔁 RETRY CONSUMER STARTED");

  await consumer.run({
    autoCommit: false,
    partitionsConsumedConcurrently: 4,

    eachMessage: async ({ topic, partition, message }) => {

      await limit(async () => {

        let log;

        try {
          log = JSON.parse(message.value.toString());
          const executionId = log.retry_id || log.id;

          // ----------------------
          // ⏳ DELAY HANDLING
          // ----------------------
          if (log.next_retry_at && Date.now() < log.next_retry_at) {
            await commitOffset(topic, partition, message);

            await producer.send({
              topic: 'logs-retry',
              messages: [{ key: log.id, value: JSON.stringify(log) }],
            });

            return;
          }

          // ----------------------
          // 🔐 IDEMPOTENCY
          // ----------------------
          const exists = await redisClient.exists(`processed:${log.id}`);
          if (exists) {
            await commitOffset(topic, partition, message);
            return;
          }

          // ----------------------
          // 🔥 MAX RETRY → DLQ
          // ----------------------
          if (log.retry_count >= MAX_RETRY) {

            await producer.send({
              topic: 'logs-dlq',
              messages: [{
                value: JSON.stringify({
                  log,
                  reason: "MAX_RETRY_EXCEEDED",
                  failed_at: Date.now()
                }),
              }],
            });

            console.log(`💀 DLQ ${executionId} max retry`);
            await commitOffset(topic, partition, message);
            return;
          }

          // ----------------------
          // 🔥 BACKPRESSURE
          // ----------------------
          const isSystemBlocked =
            cachedState.systemState === "OVERLOADED" ||
            cachedState.systemState === "PRESSURED" ||
            cachedState.circuitState === "OPEN" ||
            cachedState.avgLatency >= LATENCY_THRESHOLD;

          if (isSystemBlocked && Math.random() < 0.7) {

            const nextRetry = log.retry_count + 1;
            const delay = 500 * Math.pow(2, log.retry_count);

            await producer.send({
              topic: 'logs-retry',
              messages: [{
                key: log.id,
                value: JSON.stringify({
                  ...log,
                  retry_count: nextRetry,
                  retry_id: `${log.id}-r${nextRetry}`,
                  next_retry_at: Date.now() + delay,
                  attempt_timestamp: Date.now(), // 🔥 FIX
                  source: "SYSTEM_BACKPRESSURE"
                }),
              }],
            });

            await commitOffset(topic, partition, message);
            return;
          }

          // ----------------------
          // 🔥 CIRCUIT BREAKER
          // ----------------------
          const allowRetry = await shouldRetry();

          if (!allowRetry) {

            const nextRetry = log.retry_count + 1;

            await producer.send({
              topic: 'logs-retry',
              messages: [{
                key: log.id,
                value: JSON.stringify({
                  ...log,
                  retry_count: nextRetry,
                  retry_id: `${log.id}-r${nextRetry}`,
                  next_retry_at: Date.now() + 1000,
                  attempt_timestamp: Date.now(), // 🔥 FIX
                  source: "CIRCUIT_DELAY"
                }),
              }],
            });

            await commitOffset(topic, partition, message);
            return;
          }

          // ----------------------
          // 🔥 ACTUAL RETRY EXECUTION
          // ----------------------

          // ✔ count ONLY real retry execution
          await recordAmplification(true);

          // ✔ correct latency tracking
          log.attempt_timestamp = Date.now();

          const result = await processLog(log);

          if (result?.status === "SUCCESS") {
            console.log(`✅ RETRY SUCCESS ${executionId}`);
          }

          await commitOffset(topic, partition, message);

        } catch (err) {

          if (!log) {
            await commitOffset(topic, partition, message);
            return;
          }

          const executionId = log.retry_id || log.id;

          if (err.type === "TEMPORARY") {

            const nextRetry = log.retry_count + 1;

            if (nextRetry > MAX_RETRY) {

              await producer.send({
                topic: 'logs-dlq',
                messages: [{
                  value: JSON.stringify({
                    log,
                    reason: "TEMPORARY_MAX_RETRY",
                    failed_at: Date.now()
                  }),
                }],
              });

              console.log(`💀 DLQ ${executionId} temp max retry`);

            } else {

              const delay = 500 * Math.pow(2, nextRetry);

              await producer.send({
                topic: 'logs-retry',
                messages: [{
                  key: log.id,
                  value: JSON.stringify({
                    ...log,
                    retry_count: nextRetry,
                    retry_id: `${log.id}-r${nextRetry}`,
                    next_retry_at: Date.now() + delay,
                    attempt_timestamp: Date.now(), // 🔥 FIX
                    source: "RETRY_AGAIN"
                  }),
                }],
              });

              console.log(`🔁 RETRY AGAIN ${executionId} r=${nextRetry}`);
            }

          } else {

            await producer.send({
              topic: 'logs-dlq',
              messages: [{
                value: JSON.stringify({
                  log,
                  reason: err.type || "PERMANENT",
                  failed_at: Date.now()
                }),
              }],
            });

            console.log(`💀 DLQ ${executionId} permanent`);
          }

          await commitOffset(topic, partition, message);
        }

      });

    },
  });
}

run().catch(err => {
  console.error("🔥 RETRY CONSUMER FAILED:", err);
  process.exit(1);
});