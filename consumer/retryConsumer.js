const { Kafka } = require('kafkajs');
const pLimit = require('p-limit').default;

const { redisClient, connectRedis } = require('../config/redisClient');
const { processLog } = require('../processing/processLog');
const { shouldRetry } = require('../control/circuitBreaker');

const kafka = new Kafka({
  clientId: 'retry-consumer',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({
  groupId: 'log-group-retry',
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
});

const producer = kafka.producer();

// ======================================================
// 🔥 HIGH CONCURRENCY (NO STARVATION)
// ======================================================
let dynamicConcurrency = 100;
let limiter = pLimit(dynamicConcurrency);

setInterval(() => {
  try {
    const state = cachedState.systemState;

    let newConcurrency = 100;

    if (state === "OVERLOADED") newConcurrency = 30;
    else if (state === "PRESSURED") newConcurrency = 60;

    if (newConcurrency !== dynamicConcurrency) {
      dynamicConcurrency = newConcurrency;
      limiter = pLimit(dynamicConcurrency);
    }
  } catch {}
}, 1000);

const limit = (fn) => limiter(fn);

// ======================================================
// 🔥 LOG SAMPLING
// ======================================================
const SAMPLE_RATE = 0.01;

function shouldLog() {
  return Math.random() < SAMPLE_RATE;
}

// ======================================================
// 🔁 STATE CACHE
// ======================================================
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
}, 500);

// ======================================================
const MAX_RETRY = 2;
const LATENCY_THRESHOLD = 1500;

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

  await consumer.subscribe({ topic: 'logs-retry', fromBeginning: false });

  console.log("🔁 RETRY CONSUMER STARTED");

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
          // 🔴 INPUT TRACE
          // ======================================================
          if (shouldLog()) {
            console.log(`📥 RETRY_IN id=${log.id} r=${log.retry_count}`);
          }

          // ======================================================
          // 🔴 DELAY HANDLING (NON-BLOCKING)
          // ======================================================
          const nextRetryAt = Number(log.next_retry_at || 0);
          const waitTime = nextRetryAt - now;

          if (waitTime > 0) {

            if (waitTime <= 2000) {

              // record retry delay properly
              log.retry_delay = waitTime;

              await commitOffset(topic, partition, message);

              setTimeout(async () => {
                try {
                  const execLog = {
                    ...log,
                    retry_count: (log.retry_count || 0) + 1,
                    retry_id: `${log.id}-r${(log.retry_count || 0) + 1}`,
                    attempt_timestamp: Date.now()
                  };

                  if (shouldLog()) {
                    console.log(`🚀 RETRY_EXEC_DELAYED id=${execLog.id}`);
                  }

                  await processLog(execLog);

                } catch (err) {
                  // fallback handled in main pipeline next cycle
                }
              }, waitTime);

              return;
            }

            // long delay → requeue
            if (shouldLog()) {
              console.log(`⏳ REQUEUE_LONG id=${log.id} delay=${waitTime}`);
            }

            await commitOffset(topic, partition, message);

            await producer.send({
              topic: 'logs-retry',
              messages: [{
                key: log.id,
                value: JSON.stringify({
                  ...log,
                  retry_scheduled_at: now,
                  queue_entered_at: now
                })
              }],
            });

            return;
          }

          // ======================================================
          // 🔐 IDEMPOTENCY
          // ======================================================
          const exists = await redisClient.exists(`processed:${log.id}`);
          if (exists) {
            await commitOffset(topic, partition, message);
            return;
          }

          // ======================================================
          // 🔥 MAX RETRY → DLQ
          // ======================================================
          if (log.retry_count >= MAX_RETRY) {

            if (shouldLog()) {
              console.log(`💀 DLQ_MAX id=${log.id}`);
            }

            await producer.send({
              topic: 'logs-dlq',
              messages: [{
                value: JSON.stringify({
                  log,
                  reason: "MAX_RETRY_EXCEEDED",
                  failed_at: now
                }),
              }],
            });

            await commitOffset(topic, partition, message);
            return;
          }

          // ======================================================
          // 🔴 LOAD SHEDDING
          // ======================================================
          const isOverloaded =
            cachedState.circuitState === "OPEN" ||
            cachedState.systemState === "OVERLOADED" ||
            cachedState.avgLatency > LATENCY_THRESHOLD;

          if (isOverloaded) {

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

            if (priority === "HIGH" && cachedState.avgLatency > LATENCY_THRESHOLD) {
              if (Math.random() > 0.2) {
                await commitOffset(topic, partition, message);
                return;
              }
            }
          }

          // ======================================================
          // 🔴 CIRCUIT BREAKER
          // ======================================================
          const allowRetry = await shouldRetry();

          if (!allowRetry) {

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
          // 🔥 EXECUTION (DIRECT)
          // ======================================================
          const nextRetry = (log.retry_count || 0) + 1;

          const executionLog = {
            ...log,
            retry_count: nextRetry,
            retry_id: `${log.id}-r${nextRetry}`,
            attempt_timestamp: Date.now(),
            retry_delay: 0
          };

          if (shouldLog()) {
            console.log(`🚀 RETRY_EXEC id=${executionLog.id} r=${nextRetry}`);
          }

          await processLog(executionLog);

          await commitOffset(topic, partition, message);

        } catch (err) {

          if (!log) {
            await commitOffset(topic, partition, message);
            return;
          }

          const now = Date.now();
          const nextRetry = (log.retry_count || 0) + 1;

          if (err.type === "TEMPORARY") {

            if (nextRetry > MAX_RETRY) {

              if (shouldLog()) {
                console.log(`💀 DLQ_TEMP id=${log.id}`);
              }

              await producer.send({
                topic: 'logs-dlq',
                messages: [{
                  value: JSON.stringify({
                    log,
                    reason: "TEMP_MAX_RETRY",
                    failed_at: now
                  }),
                }],
              });

            } else {

              const baseDelay = 500 * Math.pow(2, nextRetry);
              const jitter = Math.random() * 300;
              const delay = baseDelay + jitter;

              if (shouldLog()) {
                console.log(`🔁 RETRY_AGAIN id=${log.id} delay=${Math.round(delay)}`);
              }

              await producer.send({
                topic: 'logs-retry',
                messages: [{
                  key: log.id,
                  value: JSON.stringify({
                    ...log,
                    retry_count: nextRetry,
                    retry_id: `${log.id}-r${nextRetry}`,
                    next_retry_at: now + delay,
                    retry_scheduled_at: now,
                    queue_entered_at: now,
                    source: "RETRY_AGAIN"
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