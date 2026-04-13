const { redisClient } = require('../config/redisClient');

const TTL = 3600; // 1 hour (optional windowing)

// ----------------------
// SAFE OPS
// ----------------------
async function safeIncr(key) {
  try {
    if (!redisClient || !redisClient.isOpen) return;

    const multi = redisClient.multi();
    multi.incr(key);
    multi.expire(key, TTL);
    await multi.exec();

  } catch (err) {
    console.warn(`⚠️ Counter failed [${key}]:`, err.message);
  }
}

async function safeIncrBy(key, amount) {
  try {
    if (!redisClient || !redisClient.isOpen) return;

    const multi = redisClient.multi();
    multi.incrBy(key, amount);
    multi.expire(key, TTL);
    await multi.exec();

  } catch (err) {
    console.warn(`⚠️ IncrBy failed [${key}]:`, err.message);
  }
}

// ----------------------
// 🔥 KEY BUILDER (SERVICE AWARE)
// ----------------------
function buildKey(service, metric) {
  return `counter:${service}:${metric}`;
}

// ----------------------
// PUBLIC COUNTERS
// ----------------------
async function incrementOriginal(service = "order") {
  await safeIncr(buildKey(service, 'original'));
}

async function incrementRetry(service = "order") {
  await safeIncr(buildKey(service, 'retry'));
}

async function incrementDLQ(service = "order") {
  await safeIncr(buildKey(service, 'dlq'));
}

async function incrementProcessed(service = "order") {
  await safeIncr(buildKey(service, 'processed'));
}

// 🔥 NEW (CRITICAL)
async function incrementTemporaryFailure(service = "order") {
  await safeIncr(buildKey(service, 'temporary'));
}

// ----------------------
// 🔥 BULK (FOR FUTURE USE)
// ----------------------
async function incrementBatch(service = "order", payload = {}) {
  try {
    if (!redisClient || !redisClient.isOpen) return;

    const multi = redisClient.multi();

    for (const [metric, value] of Object.entries(payload)) {
      multi.incrBy(buildKey(service, metric), value);
      multi.expire(buildKey(service, metric), TTL);
    }

    await multi.exec();

  } catch (err) {
    console.warn('⚠️ Batch counter failed:', err.message);
  }
}

// ----------------------
// RESET (TESTING ONLY)
// ----------------------
async function resetCounters(service = "order") {
  try {
    if (!redisClient || !redisClient.isOpen) return;

    await redisClient.del(
      buildKey(service, 'original'),
      buildKey(service, 'processed'),
      buildKey(service, 'retry'),
      buildKey(service, 'dlq'),
      buildKey(service, 'temporary')
    );

    console.log('✅ Counters reset');

  } catch (err) {
    console.warn('⚠️ Counter reset failed:', err.message);
  }
}

// ----------------------
// READ
// ----------------------
async function getCounters(service = "order") {
  try {
    if (!redisClient || !redisClient.isOpen) return null;

    const keys = [
      buildKey(service, 'original'),
      buildKey(service, 'processed'),
      buildKey(service, 'retry'),
      buildKey(service, 'dlq'),
      buildKey(service, 'temporary')
    ];

    const values = await Promise.all(keys.map(k => redisClient.get(k)));

    return {
      original: parseInt(values[0] || '0'),
      processed: parseInt(values[1] || '0'),
      retry: parseInt(values[2] || '0'),
      dlq: parseInt(values[3] || '0'),
      temporary: parseInt(values[4] || '0'),
    };

  } catch (err) {
    console.warn('⚠️ getCounters failed:', err.message);
    return null;
  }
}

// ----------------------
module.exports = {
  incrementOriginal,
  incrementRetry,
  incrementDLQ,
  incrementProcessed,
  incrementTemporaryFailure, // 🔥 new
  incrementBatch,
  resetCounters,
  getCounters,
};