const { redisClient } = require('../config/redisClient');

// ----------------------
// SAFE INCREMENT
// ----------------------
async function safeIncr(key) {
  try {
    if (!redisClient || !redisClient.isOpen) return;
    await redisClient.incr(key);
  } catch (err) {
    console.warn(`⚠️ Counter failed [${key}]:`, err.message);
  }
}

async function safeIncrBy(key, amount) {
  try {
    if (!redisClient || !redisClient.isOpen) return;
    await redisClient.incrBy(key, amount);
  } catch (err) {
    console.warn(`⚠️ IncrBy failed [${key}]:`, err.message);
  }
}

// ----------------------
// PUBLIC COUNTERS
// ----------------------
async function incrementOriginal() {
  await safeIncr('original_count');
}

async function incrementRetry() {
  await safeIncr('retry_count');
}

async function incrementDLQ() {
  await safeIncr('dlq_count');
}

// 🔥 CRITICAL FIX (YOU WERE MISSING THIS)
async function incrementProcessed() {
  await safeIncr('processed_count');
}

// ----------------------
// RESET (for testing)
// ----------------------
async function resetCounters() {
  try {
    if (!redisClient || !redisClient.isOpen) return;

    await redisClient.del(
      'original_count',
      'processed_count', // 🔥 added
      'retry_count',
      'dlq_count'
    );

    console.log('✅ Counters reset');
  } catch (err) {
    console.warn('⚠️ Counter reset failed:', err.message);
  }
}

// ----------------------
// READ
// ----------------------
async function getCounters() {
  try {
    if (!redisClient || !redisClient.isOpen) return null;

    const [original, processed, retry, dlq] = await Promise.all([
      redisClient.get('original_count'),
      redisClient.get('processed_count'), // 🔥 added
      redisClient.get('retry_count'),
      redisClient.get('dlq_count'),
    ]);

    return {
      original: parseInt(original || '0'),
      processed: parseInt(processed || '0'), // 🔥 added
      retry: parseInt(retry || '0'),
      dlq: parseInt(dlq || '0'),
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
  incrementProcessed, // 🔥 exported
  resetCounters,
  getCounters,
};