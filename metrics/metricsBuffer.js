const { redisClient } = require('../config/redisClient');

const metricsBuffer = [];

// 🔥 CONFIG
const MAX_BUFFER_SIZE = 5000;
const FLUSH_BATCH_SIZE = 200;
const FLUSH_INTERVAL = 200;
const WORKERS = 3;

// ----------------------
function pushMetric(metric) {
  if (metricsBuffer.length >= MAX_BUFFER_SIZE) {
    return;
  }

  metricsBuffer.push(metric);
}

// ----------------------
async function flushMetrics() {

  if (metricsBuffer.length === 0) return;

  const size = Math.min(metricsBuffer.length, FLUSH_BATCH_SIZE);
  const batch = metricsBuffer.splice(0, size);

  const aggregated = {};

  const safeNum = (v) => {
    const n = Number(v);
    return Number.isNaN(n) ? 0 : n;
  };

  // ----------------------
  // 🔥 AGGREGATION (UPDATED)
  // ----------------------
  for (const m of batch) {

    if (!m || typeof m !== "object") continue;
    if (typeof m.service !== "string" || !m.window) continue;

    const key = `${m.service}:${m.window}`;

    if (!aggregated[key]) {
      aggregated[key] = {
        service: m.service,
        window: m.window,

        total: 0,
        success: 0,
        failure: 0,
        temporary: 0,

        original: 0,
        retry: 0,

        retryDepthSum: 0,

        latencySum: 0,
        pipelineLatencySum: 0,
        endToEndLatencySum: 0,
        ingestionLatencySum: 0,

        // 🔥 NEW SIGNALS
        retryDelaySum: 0,
        queueDelaySum: 0,
        processingTimeSum: 0
      };
    }

    const agg = aggregated[key];

    // ----------------------
    // COUNTS
    // ----------------------
    agg.total++;

    if (m.success === true) agg.success++;
    if (m.failed === true) agg.failure++;
    if (m.isTemporaryFailure === true) agg.temporary++;

    // ----------------------
    // ATTEMPT TYPE
    // ----------------------
    if (m.isFirstAttempt === true) agg.original++;
    if (m.isRetry === true) agg.retry++;

    // ----------------------
    // RETRY DEPTH
    // ----------------------
    agg.retryDepthSum += safeNum(m.retryCount);

    // ----------------------
    // LATENCIES
    // ----------------------
    agg.latencySum += safeNum(m.latency);
    agg.pipelineLatencySum += safeNum(m.pipelineLatency);
    agg.endToEndLatencySum += safeNum(m.endToEndLatency);
    agg.ingestionLatencySum += safeNum(m.ingestionLatency);

    // ----------------------
    // 🔥 NEW METRICS (CRITICAL)
    // ----------------------
    agg.retryDelaySum += safeNum(m.retryDelay);
    agg.queueDelaySum += safeNum(m.queueDelay);
    agg.processingTimeSum += safeNum(m.processingTime);
  }

  // ----------------------
  // 🔥 REDIS WRITE (UPDATED)
  // ----------------------
  try {
    const pipeline = redisClient.multi();

    for (const key in aggregated) {
      const d = aggregated[key];

      pipeline.sAdd(`metrics:windows:${d.service}`, String(d.window));

      pipeline.hIncrBy(key, "total", d.total);
      pipeline.hIncrBy(key, "success", d.success);
      pipeline.hIncrBy(key, "failure", d.failure);
      pipeline.hIncrBy(key, "temporary", d.temporary);

      pipeline.hIncrBy(key, "original", d.original);
      pipeline.hIncrBy(key, "retry", d.retry);

      pipeline.hIncrBy(key, "retryDepthSum", d.retryDepthSum);

      pipeline.hIncrByFloat(key, "latencySum", d.latencySum);
      pipeline.hIncrByFloat(key, "pipelineLatencySum", d.pipelineLatencySum);
      pipeline.hIncrByFloat(key, "endToEndLatencySum", d.endToEndLatencySum);
      pipeline.hIncrByFloat(key, "ingestionLatencySum", d.ingestionLatencySum);

      // 🔥 NEW REDIS FIELDS
      pipeline.hIncrByFloat(key, "retryDelaySum", d.retryDelaySum);
      pipeline.hIncrByFloat(key, "queueDelaySum", d.queueDelaySum);
      pipeline.hIncrByFloat(key, "processingTimeSum", d.processingTimeSum);

      pipeline.expire(key, 3600);
    }

    await pipeline.exec();

  } catch (err) {
    console.error("❌ METRICS FLUSH FAILED", err.message);
  }
}

// ----------------------
// 🔥 WORKERS
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