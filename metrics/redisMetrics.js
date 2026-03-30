const { client } = require('../config/redisClient');

// 🔴 UPDATED: fully consistent metric ingestion
async function updateMetrics({
  service,
  window,
  isRetry,
  isFirstAttempt,
  retryCount,
  failed,
  latency,
  pipelineLatency,
  ingestionLatency
}) {
  const key = `metrics:${service}:${window}`;

  // 🔴 TRACK WINDOW (CRITICAL FIX)
  await client.sAdd(`metrics:windows:${service}`, window);

  await client.hIncrBy(key, 'total_attempts', 1);

  if (isFirstAttempt) {
    await client.hIncrBy(key, 'original_messages', 1);
  }

  if (isRetry) {
    await client.hIncrBy(key, 'retry_attempts', 1);
    await client.hIncrBy(key, 'total_retry_depth', retryCount);
  }

  if (failed) {
    await client.hIncrBy(key, 'total_failures', 1);

    if (isFirstAttempt) {
      await client.hIncrBy(key, 'first_attempt_failures', 1);
    } else {
      await client.hIncrBy(key, 'retry_failures', 1);
    }
  } else {
    if (isRetry) {
      await client.hIncrBy(key, 'retry_successes', 1);
    }
  }

  await client.hIncrBy(key, 'total_latency', latency || 0);
  await client.hIncrBy(key, 'total_pipeline_latency', pipelineLatency || 0);
  await client.hIncrBy(key, 'total_ingestion_latency', ingestionLatency || 0);

  await client.expire(key, 3600);
}

// 🔴 DUPLICATE CHECK
async function isDuplicate(id) {
  try {
    return await client.exists(`log:processed:${id}`);
  } catch (err) {
    throw { type: 'REDIS_ERROR', message: err.message };
  }
}

// 🔴 MARK PROCESSED
async function markProcessed(id) {
  try {
    await client.set(`log:processed:${id}`, 1, { EX: 3600 });
  } catch (err) {
    throw { type: 'REDIS_ERROR', message: err.message };
  }
}

// 🔴 SIMPLE SET TRACKING (DLQ / DEBUG)
async function addToSet(key, value) {
  try {
    await client.sAdd(key, value);
  } catch (err) {
    throw { type: 'REDIS_ERROR', message: err.message };
  }
}

// 🔴 DLQ STORAGE (FULL CONTEXT)
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

    await client.hSet(dlqKey, {
      id: entry.id,
      service: entry.service || '',
      reason: entry.reason || '',
      retry_count: String(entry.retry_count),
      pipeline_latency: String(entry.pipeline_latency),
      total_time_in_pipeline: String(entry.total_time_in_pipeline),
      failed_at: String(entry.failed_at),
      retry_history: JSON.stringify(entry.retry_history),
    });

    await client.expire(dlqKey, 86400);

    await client.sAdd('dlq_ids', log.id);

  } catch (err) {
    throw { type: 'REDIS_ERROR', message: err.message };
  }
}

// 🔴 DLQ ANALYTICS
async function getDLQStats() {
  const ids = await client.sMembers('dlq_ids');
  if (!ids.length) return { count: 0, entries: [] };

  const entries = [];

  for (const id of ids.slice(-50)) {
    try {
      const data = await client.hGetAll(`dlq:entry:${id}`);

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

  const avgTimeInPipeline = entries.length
    ? entries.reduce((s, e) => s + e.total_time_in_pipeline, 0) / entries.length
    : 0;

  const byReason = entries.reduce((acc, e) => {
    acc[e.reason] = (acc[e.reason] || 0) + 1;
    return acc;
  }, {});

  return {
    count: ids.length,
    avg_retries_before_dlq: parseFloat(avgRetries.toFixed(2)),
    avg_time_in_pipeline_ms: Math.round(avgTimeInPipeline),
    by_reason: byReason,
    recent: entries.slice(-10),
  };
}

// 🔴 METRICS FETCH
async function getMetrics(service, window) {
  const key = `metrics:${service}:${window}`;
  const data = await client.hGetAll(key);

  if (!data || Object.keys(data).length === 0) return null;

  const total = +data.total_attempts || 0;
  const original = +data.original_messages || 0;
  const retry = +data.retry_attempts || 0;
  const failures = +data.total_failures || 0;
  const retryDepth = +data.total_retry_depth || 0;

  return {
    total_attempts: total,
    original_messages: original,
    retry_attempts: retry,
    total_failures: failures,

    retry_amplification: original ? +(retry / original).toFixed(2) : 0,
    avg_retry_depth: retry ? +(retryDepth / retry).toFixed(2) : 0,

    initial_failure_rate: original ? +(data.first_attempt_failures / original).toFixed(2) : 0,
    overall_failure_rate: total ? +(failures / total).toFixed(2) : 0,

    retry_success_rate: retry ? +(data.retry_successes / retry).toFixed(2) : 0,
    retry_failure_rate: retry ? +(data.retry_failures / retry).toFixed(2) : 0,

    avg_pipeline_latency: total ? +(data.total_pipeline_latency / total).toFixed(2) : 0,
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