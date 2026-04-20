const { createClient } = require('redis');

const redisUrl = process.env.REDIS_URL;

if (!redisUrl) {
  throw new Error("REDIS_URL is not defined");
}

const redisClient = createClient({
  url: redisUrl
});

redisClient.on('connect', () => {
  console.log('🔌 Redis connecting...');
});

redisClient.on('ready', () => {
  console.log('✅ Redis connected');
});

redisClient.on('error', (err) => {
  console.error('🔥 Redis error:', err);
});

async function connectRedis() {
  if (!redisClient.isOpen) {
    await redisClient.connect();
  }
}

module.exports = {
  redisClient,
  connectRedis
};