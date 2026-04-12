const { validate } = require('./validator');
const { pushToDBQueue } = require('../db/dbQueue');
const { redisClient } = require('../config/redisClient');
const { pushMetric } = require('../metrics/metricsBuffer');

// ----------------------
const getWindow = (timestamp) => Math.floor(timestamp / 60000) * 60000;

// ----------------------
const processLog = async (log) => {
  validate(log);

  // ----------------------
  // NORMALIZATION
  // ----------------------
  const retryCount = Number.isInteger(log.retry_count) ? log.retry_count : 0;
  log.retry_count = retryCount;

  const service = log.service || 'order';
  const timestamp = Number(log.timestamp) || Date.now();
  const now = Date.now();

  const window = getWindow(timestamp);

  const ingestionLatency = log.ingestion_latency || 0;
  const latency = log.latency_ms || 0;

  // 🔥 CORRECT LATENCY MODEL
  const attemptTs = log.attempt_timestamp || timestamp;

  const pipelineLatency = now - attemptTs;     // per attempt
  const endToEndLatency = now - timestamp;     // full lifecycle

  const processedKey = `processed:${log.id}`;

  // ----------------------
  // STEP 1: IDEMPOTENCY CLAIM
  // ----------------------
  try {
    const claimed = await redisClient.set(processedKey, "1", {
      NX: true,
      EX: 3600
    });

    if (!claimed) {
      return { status: "SKIPPED_ALREADY_PROCESSED" };
    }

  } catch {
    return { status: "ERROR_CLAIM_FAILED" };
  }

  // ----------------------
  // STEP 2: PROCESS
  // ----------------------
  try {
    const dbFailure = await redisClient.get('db:failure');

    // ----------------------
    // 🔴 TEMPORARY FAILURE (NO METRIC)
    // ----------------------
    if (dbFailure === '1') {
      const error = new Error("DB_DOWN");
      error.type = "TEMPORARY";
      throw error;
    }

    // ----------------------
    // ✅ SUCCESS PATH
    // ----------------------
    pushToDBQueue({
      service,
      window_start: new Date(window),
      request_count: 1,
      error_count: 0,
      error_rate: 0,
      avg_latency: latency,
      avg_pipeline_latency: pipelineLatency,
      avg_ingestion_latency: ingestionLatency,
      retry_amplification: 0,
      avg_retry_depth: retryCount
    });

    // 🔥 FINAL SUCCESS METRIC
    pushMetric({
      service,
      window,
      isRetry: retryCount > 0,
      isFirstAttempt: retryCount === 0,
      retryCount,
      failed: false,
      success: true,
      latency,
      pipelineLatency,
      endToEndLatency,
      ingestionLatency,
      isRetrySuccess: retryCount > 0,
      isRetryFailure: false
    });

  } catch (err) {

    const msg = ((err && err.message) || "").toLowerCase();

    const errorType =
      (msg.includes("connection") || msg.includes("db"))
        ? "TEMPORARY"
        : "PERMANENT";

    // ----------------------
    // 🔴 TEMPORARY → RETRY (NO METRIC)
    // ----------------------
    if (errorType === "TEMPORARY") {

      try {
        await redisClient.del(processedKey);
      } catch {}

      const error = new Error(err?.message || "PROCESS_LOG_ERROR");
      error.type = "TEMPORARY";

      throw error;
    }

    // ----------------------
    // 🔴 PERMANENT → FINAL FAILURE (METRIC)
    // ----------------------
    pushToDBQueue({
      service,
      window_start: new Date(window),
      request_count: 1,
      error_count: 1,
      error_rate: 1,
      avg_latency: latency,
      avg_pipeline_latency: pipelineLatency,
      avg_ingestion_latency: ingestionLatency,
      retry_amplification: 0,
      avg_retry_depth: retryCount
    });

    // 🔥 FINAL FAILURE METRIC (DLQ CASE)
    pushMetric({
      service,
      window,
      isRetry: retryCount > 0,
      isFirstAttempt: retryCount === 0,
      retryCount,
      failed: true,
      success: false,
      latency,
      pipelineLatency,
      endToEndLatency,
      ingestionLatency,
      isRetrySuccess: false,
      isRetryFailure: retryCount > 0
    });

    try {
      await redisClient.del(processedKey);
    } catch {}

    const error = new Error(err?.message || "PROCESS_LOG_ERROR");
    error.type = "PERMANENT";

    throw error;
  }

  // ----------------------
  return { status: "SUCCESS" };
};

module.exports = { processLog };