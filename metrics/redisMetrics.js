const { redisClient } = require('../config/redisClient');

// ----------------------
// 🔥 UPDATE METRICS (FINAL MODEL)
// ----------------------
async function updateMetrics({
  service,
  window,

  isRetry,
  isFirstAttempt,
  retryCount,

  success,
  failed,
  isTemporaryFailure,

  latency,
  pipelineLatency,
  endToEndLatency,
  ingestionLatency,

  // 🔥 NEW SIGNALS
  retryDelay,
  queueDelay,
  processingTime
}) {
  try {
    const key = `${service}:${window}`;
    const multi = redisClient.multi();

    multi.sAdd(`metrics:windows:${service}`, String(window));

    // ----------------------
    // COUNTS
    // ----------------------
    multi.hIncrBy(key, 'total', 1);

    if (success) multi.hIncrBy(key, 'success', 1);
    if (failed) multi.hIncrBy(key, 'failure', 1);
    if (isTemporaryFailure) multi.hIncrBy(key, 'temporary', 1);

    // ----------------------
    // ATTEMPT TYPE
    // ----------------------
    if (isFirstAttempt) multi.hIncrBy(key, 'original', 1);
    if (isRetry) multi.hIncrBy(key, 'retry', 1);

    // ----------------------
    // RETRY DEPTH
    // ----------------------
    if (retryCount > 0) {
      multi.hIncrBy(key, 'retryDepthSum', retryCount);
    }

    // ----------------------
    // LATENCIES
    // ----------------------
    multi.hIncrByFloat(key, 'latencySum', latency || 0);
    multi.hIncrByFloat(key, 'pipelineLatencySum', pipelineLatency || 0);
    multi.hIncrByFloat(key, 'endToEndLatencySum', endToEndLatency || 0);
    multi.hIncrByFloat(key, 'ingestionLatencySum', ingestionLatency || 0);

    // ----------------------
    // 🔥 NEW SIGNALS
    // ----------------------
    multi.hIncrByFloat(key, 'retryDelaySum', retryDelay || 0);
    multi.hIncrByFloat(key, 'queueDelaySum', queueDelay || 0);
    multi.hIncrByFloat(key, 'processingTimeSum', processingTime || 0);

    multi.expire(key, 3600);

    await multi.exec();

  } catch (err) {
    console.error("🔥 updateMetrics FAILED:", err.message);
  }
}

// ----------------------
// 🔥 FETCH METRICS (UPDATED)
// ----------------------
async function getMetrics(service, window) {
  const key = `${service}:${window}`;
  const data = await redisClient.hGetAll(key);

  if (!data || Object.keys(data).length === 0) return null;

  const total = +data.total || 0;

  if (total === 0) return null;

  const original = +data.original || 0;
  const retry = +data.retry || 0;

  const retryDepthSum = +data.retryDepthSum || 0;

  return {
    total_attempts: total,

    success: +data.success || 0,
    failures: +data.failure || 0,
    temporary_failures: +data.temporary || 0,

    success_rate: total ? +(data.success / total).toFixed(3) : 0,
    failure_rate: total ? +(data.failure / total).toFixed(3) : 0,

    retry_amplification:
      original ? +(total / original).toFixed(2) : 0,

    avg_retry_depth:
      retry ? +(retryDepthSum / retry).toFixed(2) : 0,

    // ----------------------
    // LATENCIES
    // ----------------------
    avg_pipeline_latency:
      +((+data.pipelineLatencySum || 0) / total).toFixed(2),

    avg_end_to_end_latency:
      +((+data.endToEndLatencySum || 0) / total).toFixed(2),

    avg_ingestion_latency:
      +((+data.ingestionLatencySum || 0) / total).toFixed(2),

    avg_latency:
      +((+data.latencySum || 0) / total).toFixed(2),

    // ----------------------
    // 🔥 NEW BREAKDOWN
    // ----------------------
    avg_retry_delay:
      +((+data.retryDelaySum || 0) / total).toFixed(2),

    avg_queue_delay:
      +((+data.queueDelaySum || 0) / total).toFixed(2),

    avg_processing_time:
      +((+data.processingTimeSum || 0) / total).toFixed(2)
  };
}

// ----------------------
// 🔥 DUPLICATE CHECK
// ----------------------
async function isDuplicate(id) {
  return await redisClient.exists(`processed:${id}`);
}

// ----------------------
// 🔥 MARK PROCESSED
// ----------------------
async function markProcessed(id) {
  await redisClient.set(`processed:${id}`, 1, { EX: 3600 });
}

// ----------------------
// 🔥 DLQ STORAGE (SAFE)
// ----------------------
async function addToDLQ(log, reason) {
  const key = `dlq:entry:${log.id}`;

  await redisClient.hSet(key, {
    id: String(log.id),
    service: String(log.service || ''),
    reason: String(reason || ''),

    retry_count: String(log.retry_count || 0),
    failed_at: String(Date.now())
  });

  await redisClient.expire(key, 86400);
  await redisClient.sAdd('dlq_ids', String(log.id));
}

// ----------------------
// 🔥 DLQ STATS
// ----------------------
async function getDLQStats() {
  const ids = await redisClient.sMembers('dlq_ids');
  if (!ids.length) return { count: 0 };

  return { count: ids.length };
}

// ----------------------
module.exports = {
  updateMetrics,
  getMetrics,
  addToDLQ,
  getDLQStats,
  isDuplicate,
  markProcessed
};