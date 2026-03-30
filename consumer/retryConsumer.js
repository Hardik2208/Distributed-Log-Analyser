const { Kafka } = require('kafkajs');
const pLimit = require('p-limit');

const { connectRedis } = require('../config/redisClient');
const { processLog } = require('../processing/processLog');

const kafka = new Kafka({
  clientId: 'retry-consumer',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'log-group-retry' });
const producer = kafka.producer();

const MAX_RETRIES = 3;

let throttleFactor = 1;
let paused = false;

const baseLimit = 30;

// 🔴 GLOBAL CONTROL
global.throttleRetryConsumer = (factor = 0.5) => {
  throttleFactor = factor;
  console.log(`🐢 RETRY THROTTLE → ${factor}`);
};

global.unthrottleRetryConsumer = () => {
  throttleFactor = 1;
  console.log("🚀 RETRY NORMAL");
};

async function run() {
  await connectRedis();
  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({ topic: 'logs-retry', fromBeginning: false });

  await consumer.run({
    autoCommit: false,

    eachMessage: async ({ topic, partition, message }) => {

      const dynamicLimit = Math.max(1, Math.floor(baseLimit * throttleFactor));
      const limit = pLimit(dynamicLimit);

      await limit(async () => {

        let log;

        try {
          log = JSON.parse(message.value.toString());

          log.retry_count = log.retry_count || 1;
          log.ingestion_latency = Date.now() - log.timestamp;

          await processLog(log);

        } catch (err) {

          if (log.retry_count >= MAX_RETRIES) {

            await producer.send({
              topic: 'logs-dlq',
              messages: [{
                value: JSON.stringify({
                  log,
                  reason: err.type || "MAX_RETRIES",
                  failed_at: Date.now()
                }),
              }],
            });

          } else {

            const updated = {
              ...log,
              retry_count: log.retry_count + 1,
              source: "RETRY"
            };

            setTimeout(async () => {
              await producer.send({
                topic: 'logs-retry',
                messages: [{ value: JSON.stringify(updated) }],
              });
            }, 500 * Math.pow(2, log.retry_count));
          }
        }

        await consumer.commitOffsets([{
          topic,
          partition,
          offset: (Number(message.offset) + 1).toString(),
        }]);

      });
    },
  });
}

run().catch(console.error);