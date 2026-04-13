const { validate } = require('./validator');
const { pushToDBQueue } = require('../db/dbQueue');
const { redisClient } = require('../config/redisClient');
const { pushMetric } = require('../metrics/metricsBuffer');

// ----------------------
const getWindow = (timestamp) =>
  Math.floor(timestamp / 60000) * 60000;

// ----------------------
const processLog = async (log) => {
  validate(log);

  // ----------------------
  // 🔥 NORMALIZATION
  // ----------------------
  const retryCount = Number.isInteger(log.retry_count)
    ? log.retry_count
    : 0;

  const service = log.service || 'order';
  const timestamp = Number(log.timestamp) || Date.now();
  const window = getWindow(timestamp);

  const ingestionLatency = Number(log.ingestion_latency) || 0;
  const latency = Number(log.latency_ms) || 0;

  const processedKey = `processed:${log.id}`;

  // ----------------------
  // 🔥 TIME REFERENCES (CORRECTED)
  // ----------------------
  const attemptTs = Number(log.attempt_timestamp) || Date.now();
  const queueEnteredAt = Number(log.queue_entered_at) || timestamp;
  const retryScheduledAt = Number(log.retry_scheduled_at) || queueEnteredAt;

  // ----------------------
  // 🔥 IDEMPOTENCY
  // ----------------------
  try {
    const claimed = await redisClient.set(processedKey, "1", {
      NX: true,
      EX: 3600,
    });

    if (!claimed) {
      return { status: "SKIPPED_ALREADY_PROCESSED" };
    }
  } catch {
    return { status: "ERROR_CLAIM_FAILED" };
  }

  // ----------------------
  // 🔥 EXECUTION START
  // ----------------------
  const executionStart = Date.now();

  try {
    const dbFailure = await redisClient.get('db:failure');

    if (dbFailure === '1') {
      const error = new Error("DB_DOWN");
      error.type = "TEMPORARY";
      throw error;
    }

    // ----------------------
    // ✅ SUCCESS
    // ----------------------
    const executionEnd = Date.now();

    // ----------------------
    // 🔥 CORRECT LATENCIES
    // ----------------------
    const queueDelay = attemptTs - queueEnteredAt;
    const processingTime = executionEnd - attemptTs;
    const pipelineLatency = executionEnd - queueEnteredAt;
    const endToEndLatency = executionEnd - timestamp;
    const retryDelay = queueEnteredAt - retryScheduledAt;

    // ----------------------
    // DB WRITE
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
      avg_retry_depth: retryCount,
    });

    // ----------------------
    // METRIC
    // ----------------------
    pushMetric({
      service,
      window,
      isRetry: retryCount > 0,
      isFirstAttempt: retryCount === 0,
      retryCount,

      success: true,
      failed: false,
      isTemporaryFailure: false,

      latency,
      pipelineLatency,
      endToEndLatency,
      ingestionLatency,

      // 🔥 CORRECT SIGNALS
      retryDelay,
      queueDelay,
      processingTime
    });

  } catch (err) {

    const executionEnd = Date.now();

    const queueDelay = attemptTs - queueEnteredAt;
    const processingTime = executionEnd - attemptTs;
    const pipelineLatency = executionEnd - queueEnteredAt;
    const endToEndLatency = executionEnd - timestamp;
    const retryDelay = queueEnteredAt - retryScheduledAt;

    const isTemporary = err.type === "TEMPORARY";

    // ----------------------
    // 🔴 TEMPORARY FAILURE
    // ----------------------
    if (isTemporary) {

      pushMetric({
        service,
        window,
        isRetry: retryCount > 0,
        isFirstAttempt: retryCount === 0,
        retryCount,

        success: false,
        failed: false,
        isTemporaryFailure: true,

        latency,
        pipelineLatency,
        endToEndLatency,
        ingestionLatency,

        retryDelay,
        queueDelay,
        processingTime
      });

      try {
        await redisClient.del(processedKey);
      } catch {}

      throw err;
    }

    // ----------------------
    // 🔴 PERMANENT FAILURE
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
      avg_retry_depth: retryCount,
    });

    pushMetric({
      service,
      window,
      isRetry: retryCount > 0,
      isFirstAttempt: retryCount === 0,
      retryCount,

      success: false,
      failed: true,
      isTemporaryFailure: false,

      latency,
      pipelineLatency,
      endToEndLatency,
      ingestionLatency,

      retryDelay,
      queueDelay,
      processingTime
    });

    try {
      await redisClient.del(processedKey);
    } catch {}

    err.type = "PERMANENT";
    throw err;
  }

  return { status: "SUCCESS" };
};

module.exports = { processLog };