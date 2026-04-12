const { bulkInsertMetrics } = require('./db');

const queue = [];

// 🔥 CONFIG
const MAX_BATCH_SIZE = 50;
const FLUSH_INTERVAL = 100;

// 🔥 NEW: PARALLEL WORKERS
const DB_WORKERS = 3;

// 🔥 NEW: HARD LIMIT (BACKPRESSURE CONTROL)
const MAX_QUEUE_SIZE = 5000;

// 🔥 NEW: RETRY LIMIT
const MAX_RETRIES = 3;

// ----------------------
function pushToDBQueue(record) {
  if (queue.length >= MAX_QUEUE_SIZE) {
    console.warn("🚨 DB QUEUE OVERFLOW — DROPPING");
    return;
  }

  // attach retry metadata (non-breaking)
  queue.push({
    ...record,
    __retries: record.__retries || 0
  });
}

// ----------------------
async function flushDB() {

  if (queue.length === 0) return;

  if (queue.length > 1000) {
    console.warn("⚠️ DB QUEUE BUILDUP:", queue.length);
  }

  // 🔥 DYNAMIC BATCH SIZE (NO LOGIC LOSS)
  const size = Math.min(queue.length, MAX_BATCH_SIZE);
  const batch = queue.splice(0, size);

  try {
    await bulkInsertMetrics(batch);

  } catch (err) {
    console.error("❌ DB BULK INSERT FAILED", err.message);

    // 🔥 CONTROLLED RETRY (NO INFINITE LOOP)
    const retryBatch = [];

    for (const record of batch) {
      if ((record.__retries || 0) < MAX_RETRIES) {
        retryBatch.push({
          ...record,
          __retries: (record.__retries || 0) + 1
        });
      } else {
        console.error("💀 DROPPING RECORD AFTER MAX RETRIES");
      }
    }

    // 🔥 PUSH BACK SAFELY (FRONT = PRIORITY RETRY)
    queue.unshift(...retryBatch);
  }
}

// ----------------------
// 🔥 PARALLEL DB WORKERS
for (let i = 0; i < DB_WORKERS; i++) {
  setInterval(() => {
    flushDB();
  }, FLUSH_INTERVAL);
}

// ----------------------
module.exports = {
  pushToDBQueue,
};