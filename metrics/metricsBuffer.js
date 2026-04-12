const { redisClient } = require('../config/redisClient');

const metricsBuffer = [];

// 🔥 CONTROL LIMITS (UNCHANGED)
const MAX_BUFFER_SIZE = 5000;
const FLUSH_BATCH_SIZE = 200;
const FLUSH_INTERVAL = 200;

// 🔥 NEW: PARALLEL WORKERS
const WORKERS = 3;

// ----------------------
function pushMetric(metric) {
  console.log("📥 PUSH METRIC", metric.service);
  // 🔥 MEMORY PROTECTION (FIXED: DROP NEW, NOT OLD)
  if (metricsBuffer.length >= MAX_BUFFER_SIZE) {
    // do NOT shift old data → preserve history integrity
    return;
  }

  metricsBuffer.push(metric);
}

// ----------------------
async function flushMetrics() {

  // 🔥 SIMPLE EXIT (NO TIME GATING, NO GLOBAL LOCK)
  if (metricsBuffer.length === 0) return;

  // 🔥 PROCESS ONLY ONE BATCH (CRITICAL FIX)
  const size = Math.min(metricsBuffer.length, FLUSH_BATCH_SIZE);
  const batch = metricsBuffer.splice(0, size);

  const aggregated = {};

  // ----------------------
  // 🔥 SAME AGGREGATION LOGIC (UNCHANGED)
  // ----------------------
  for (const m of batch) {
    const key = `${m.service}:${m.window}`;

    if (!aggregated[key]) {
      aggregated[key] = {
        service: m.service,
        window: m.window,
        count: 0,
        success: 0,
        failure: 0,
        latency: 0,
        pipelineLatency: 0,
        ingestionLatency: 0,
        original: 0,
        retry: 0
      };
    }

    aggregated[key].count++;

    if (m.success) aggregated[key].success++;
    if (m.failed) aggregated[key].failure++;

    if (m.isFirstAttempt) aggregated[key].original++;
    if (m.isRetry) aggregated[key].retry++;

    aggregated[key].latency += m.latency;
    aggregated[key].pipelineLatency += m.pipelineLatency || 0;
    aggregated[key].ingestionLatency += m.ingestionLatency || 0;
  }

  try {
    const pipeline = redisClient.multi();

    for (const key in aggregated) {
      const data = aggregated[key];

      pipeline.sAdd(`metrics:windows:${data.service}`, String(data.window));

      pipeline.hIncrBy(key, "count", data.count);
      pipeline.hIncrBy(key, "success", data.success);
      pipeline.hIncrBy(key, "failure", data.failure);

      pipeline.hIncrBy(key, "original", data.original);
      pipeline.hIncrBy(key, "retry", data.retry);

      pipeline.hIncrByFloat(key, "latency", data.latency);
      pipeline.hIncrByFloat(key, "pipelineLatency", data.pipelineLatency);
      pipeline.hIncrByFloat(key, "ingestionLatency", data.ingestionLatency);

      pipeline.expire(key, 3600);
    }

    await pipeline.exec();

  } catch (err) {
    console.error("❌ METRICS FLUSH FAILED", err.message);
  }
}

// ----------------------
// 🔥 PARALLEL FLUSH WORKERS (REAL FIX)
// ----------------------
for (let i = 0; i < WORKERS; i++) {
  setInterval(() => {
    flushMetrics();
  }, FLUSH_INTERVAL);
}

// ----------------------
module.exports = {
  pushMetric,
};