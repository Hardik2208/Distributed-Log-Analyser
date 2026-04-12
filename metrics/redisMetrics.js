const { redisClient } = require('../config/redisClient');

// ----------------------
// UPDATE METRICS (UNIFIED + PRODUCTION SAFE)
// ----------------------
async function updateMetrics({
  service,
  window,
  isRetry,
  isFirstAttempt,
  retryCount,
  failed,
  success,
  latency,
  pipelineLatency,
  ingestionLatency
}) {
  try {
    const key = `${service}:${window}`;

    const multi = redisClient.multi();

    // 🔥 TRACK WINDOW
    multi.sAdd(`metrics:windows:${service}`, String(window));

    // ----------------------
    // COUNTS
    // ----------------------
    multi.hIncrBy(key, 'count', 1);

    if (success) multi.hIncrBy(key, 'success', 1);
    if (failed) multi.hIncrBy(key, 'failure', 1);

    // ----------------------
    // RETRY TRACKING
    // ----------------------
    if (isFirstAttempt) multi.hIncrBy(key, 'original', 1);
    if (isRetry) {
      multi.hIncrBy(key, 'retry', 1);
      multi.hIncrBy(key, 'retryDepth', retryCount || 0);
    }

    // ----------------------
    // LATENCIES
    // ----------------------
    multi.hIncrByFloat(key, 'latency', latency || 0);
    multi.hIncrByFloat(key, 'pipelineLatency', pipelineLatency || 0);
    multi.hIncrByFloat(key, 'ingestionLatency', ingestionLatency || 0);

    // ----------------------
    // TTL (avoid memory leak)
    // ----------------------
    multi.expire(key, 3600);

    await multi.exec();

  } catch (err) {
    console.error("🔥 updateMetrics FAILED:", err.message);
  }
}

// ----------------------
// DUPLICATE CHECK
// ----------------------
async function isDuplicate(id) {
  try {
    return await redisClient.exists(`processed:${id}`);
  } catch (err) {
    throw { type: 'REDIS_ERROR', message: err.message };
  }
}

// ----------------------
// MARK PROCESSED
// ----------------------
async function markProcessed(id) {
  try {
    await redisClient.set(`processed:${id}`, 1, { EX: 3600 });
  } catch (err) {
    throw { type: 'REDIS_ERROR', message: err.message };
  }
}

// ----------------------
// GENERIC SET ADD
// ----------------------
async function addToSet(key, value) {
  try {
    await redisClient.sAdd(key, value);
  } catch (err) {
    throw { type: 'REDIS_ERROR', message: err.message };
  }
}

// ----------------------
// DLQ STORAGE
// ----------------------
async function addToDLQ(log, reason) {
  try {
    const entry = {
      id: log.id,
      service: log.service,
      reason,
      retry_count: log.retry_count || 0,
      retry_history: log.retry_history || [],
      pipeline_latency: log.pipeline_latency || 0,
      ingestion_latency: log.ingestion_latency || 0,
      original_timestamp: log.timestamp,
      failed_at: Date.now(),
      total_time_in_pipeline: Date.now() - log.timestamp,
    };

    const dlqKey = `dlq:entry:${log.id}`;

    await redisClient.hSet(dlqKey, {
      id: entry.id,
      service: entry.service || '',
      reason: entry.reason || '',
      retry_count: String(entry.retry_count),
      pipeline_latency: String(entry.pipeline_latency),
      total_time_in_pipeline: String(entry.total_time_in_pipeline),
      failed_at: String(entry.failed_at),
      retry_history: JSON.stringify(entry.retry_history),
    });

    await redisClient.expire(dlqKey, 86400);
    await redisClient.sAdd('dlq_ids', log.id);

  } catch (err) {
    throw { type: 'REDIS_ERROR', message: err.message };
  }
}

// ----------------------
// DLQ ANALYTICS
// ----------------------
async function getDLQStats() {
  const ids = await redisClient.sMembers('dlq_ids');
  if (!ids.length) return { count: 0, entries: [] };

  const entries = [];

  for (const id of ids.slice(-50)) {
    try {
      const data = await redisClient.hGetAll(`dlq:entry:${id}`);

      if (data && data.id) {
        entries.push({
          ...data,
          retry_count: parseInt(data.retry_count || 0),
          pipeline_latency: parseInt(data.pipeline_latency || 0),
          total_time_in_pipeline: parseInt(data.total_time_in_pipeline || 0),
          retry_history: JSON.parse(data.retry_history || '[]'),
        });
      }
    } catch {}
  }

  const avgRetries = entries.length
    ? entries.reduce((s, e) => s + e.retry_count, 0) / entries.length
    : 0;

  const avgTime = entries.length
    ? entries.reduce((s, e) => s + e.total_time_in_pipeline, 0) / entries.length
    : 0;

  const byReason = entries.reduce((acc, e) => {
    acc[e.reason] = (acc[e.reason] || 0) + 1;
    return acc;
  }, {});

  return {
    count: ids.length,
    avg_retries_before_dlq: parseFloat(avgRetries.toFixed(2)),
    avg_time_in_pipeline_ms: Math.round(avgTime),
    by_reason: byReason,
    recent: entries.slice(-10),
  };
}

// ----------------------
// METRICS FETCH (ALIGNED WITH BUFFER)
// ----------------------
async function getMetrics(service, window) {
  const key = `${service}:${window}`;
  const data = await redisClient.hGetAll(key);

  if (!data || Object.keys(data).length === 0) return null;

  const count = +data.count || 0;
  const success = +data.success || 0;
  const failure = +data.failure || 0;
  const original = +data.original || 0;
  const retry = +data.retry || 0;
  const retryDepth = +data.retryDepth || 0;

  return {
    total_attempts: count,
    success,
    failures: failure,

    success_rate: count ? +(success / count).toFixed(3) : 0,
    failure_rate: count ? +(failure / count).toFixed(3) : 0,

    retry_amplification: original ? +((original + retry) / original).toFixed(2) : 0,
    avg_retry_depth: retry ? +(retryDepth / retry).toFixed(2) : 0,

    avg_pipeline_latency: count
      ? +(data.pipelineLatency / count).toFixed(2)
      : 0,

    avg_ingestion_latency: count
      ? +(data.ingestionLatency / count).toFixed(2)
      : 0,

    avg_latency: count
      ? +(data.latency / count).toFixed(2)
      : 0,
  };
}

module.exports = {
  updateMetrics,
  getMetrics,
  addToSet,
  addToDLQ,
  getDLQStats,
  isDuplicate,
  markProcessed,
};