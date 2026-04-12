const { createClient } = require('redis');

const redisClient = createClient();

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