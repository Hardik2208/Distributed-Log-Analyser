const { Kafka } = require('kafkajs');
const pLimit = require('p-limit');

const { connectRedis } = require('../config/redisClient');
const { processLog } = require('../processing/processLog');

const kafka = new Kafka({
  clientId: 'main-consumer',
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'log-group-main' });
const producer = kafka.producer();

const limit = pLimit(50);

let paused = false;

// 🔴 GLOBAL CONTROL HOOKS
global.pauseMainConsumer = async () => {
  if (!paused) {
    consumer.pause([{ topic: 'logs' }]);
    paused = true;
    console.log("⏸️ MAIN CONSUMER PAUSED");
  }
};

global.resumeMainConsumer = async () => {
  if (paused) {
    consumer.resume([{ topic: 'logs' }]);
    paused = false;
    console.log("▶️ MAIN CONSUMER RESUMED");
  }
};

async function run() {
  await connectRedis();
  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({ topic: 'logs', fromBeginning: false });

  await consumer.run({
    autoCommit: false,

    eachMessage: async ({ topic, partition, message }) => {

      await limit(async () => {

        let log;

        try {
          log = JSON.parse(message.value.toString());

          log.retry_count = log.retry_count || 0;
          log.ingestion_latency = Date.now() - log.timestamp;

          await processLog(log);

        } catch (err) {

          if (log && log.retry_count === 0) {
            await producer.send({
              topic: 'logs-retry',
              messages: [{
                value: JSON.stringify({
                  ...log,
                  retry_count: 1,
                  source: "RETRY"
                }),
              }],
            });
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