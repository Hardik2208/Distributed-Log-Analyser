const { isDuplicate, markProcessed, updateMetrics } = require('../metrics/redisMetrics');
const { validate } = require('./validator');
const db = require('../db/fakeDb');

const getWindow = (timestamp) => Math.floor(timestamp / 60000) * 60000;

// 🔴 DB TIMEOUT WRAPPER (CRITICAL)
async function withTimeout(promise, ms = 1000) {
  return Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error("DB_TIMEOUT")), ms)
    )
  ]);
}

const processLog = async (log) => {
  validate(log);

  log.retry_count = Number.isInteger(log.retry_count) ? log.retry_count : 0;

  const service = log.service;
  const timestamp = log.timestamp;
  const window = getWindow(timestamp);
  const now = Date.now();

  const isRetry = log.retry_count > 0;
  const isFirstAttempt = log.retry_count === 0;

  // -------------------------------
  // IDEMPOTENCY (STRICT)
  // -------------------------------
  const duplicate = await isDuplicate(log.id);

  if (duplicate) {
    // 🔴 NEVER reprocess — retry or not
    return;
  }

  let failed = false;
  let errorType = null;

  try {
    // 🔴 REAL DEPENDENCY WITH TIMEOUT
    await withTimeout(db.query(log), 1000);

  } catch (err) {
    failed = true;

    // 🔴 ERROR CLASSIFICATION
    if (err.message === "DB_TIMEOUT") {
      errorType = "TEMPORARY";
    } else if (err.message.includes("connection")) {
      errorType = "TEMPORARY";
    } else {
      errorType = "PERMANENT";
    }

    console.error(`🔥 FAILURE ${log.id} type=${errorType}`);

    // 🔴 ASYNC METRICS (NON-BLOCKING)
    updateMetrics({
      service,
      window,
      isRetry,
      isFirstAttempt,
      retryCount: log.retry_count,
      failed: true,
      latency: log.latency_ms,
      pipelineLatency: now - timestamp,
      ingestionLatency: log.ingestion_latency || 0
    }).catch(() => {});

    throw {
      type: errorType,
      message: err.message
    };
  }

  // -------------------------------
  // SUCCESS PATH
  // -------------------------------

  // 🔴 MARK FIRST (IMPORTANT ORDER)
  await markProcessed(log.id);

  // 🔴 ASYNC METRICS (NON-BLOCKING)
  updateMetrics({
    service,
    window,
    isRetry,
    isFirstAttempt,
    retryCount: log.retry_count,
    failed: false,
    latency: log.latency_ms,
    pipelineLatency: now - timestamp,
    ingestionLatency: log.ingestion_latency || 0
  }).catch(() => {});

  console.log(`✅ SUCCESS ${log.id} retry=${log.retry_count}`);
};

module.exports = { processLog };