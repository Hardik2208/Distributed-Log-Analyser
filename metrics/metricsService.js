const { redisClient } = require('../config/redisClient');
const { pushMetric } = require('../metrics/metricsBuffer');

const WINDOW_KEY = 'metrics_window';
const WINDOW_SIZE = 200;

const BASELINE_KEY = 'metrics:baseline_latency';
const DEFAULT_BASELINE = 150;

// ----------------------
async function safeExec(fn, fallback = null) {
  try {
    if (!redisClient || !redisClient.isOpen) {
      console.error('⚠️ Redis unavailable — metrics degraded');
      return fallback;
    }
    return await fn();
  } catch (err) {
    console.error('🔥 Redis error:', err.message);
    return fallback;
  }
}

// ----------------------
// 🔥 OPTIONAL STREAM (DEBUG)
async function recordMetrics({ status, latency }) {
  const entry = JSON.stringify({ status, latency, ts: Date.now() });

  await safeExec(() => redisClient.lPush(WINDOW_KEY, entry));
  await safeExec(() => redisClient.lTrim(WINDOW_KEY, 0, WINDOW_SIZE - 1));
}

// ----------------------
// 🔥 FINAL METRIC → BUFFER
async function updateMetrics(payload) {
  if (!payload?.window) {
    console.error('🔥 INVALID METRIC PAYLOAD');
    return;
  }

  pushMetric(payload);
}

// ----------------------
// 🔥 SYSTEM METRICS (UPDATED)
// ----------------------
async function getSystemMetrics() {

  let total = 0;
  let success = 0;
  let failure = 0;
  let temporary = 0;

  let pipelineSum = 0;
  let endToEndSum = 0;

  // 🔥 NEW SIGNALS
  let retryDelaySum = 0;
  let queueDelaySum = 0;
  let processingTimeSum = 0;

  let original = 0;
  let retry = 0;

  const windows = await safeExec(
    () => redisClient.sMembers(`metrics:windows:order`),
    []
  );

  if (!windows.length) return null;

  const recent = windows
    .map(Number)
    .sort((a, b) => b - a)
    .slice(0, 3);

  for (const w of recent) {
    const key = `order:${w}`;
    const data = await safeExec(() => redisClient.hGetAll(key), {});

    total += Number(data.total || 0);
    success += Number(data.success || 0);
    failure += Number(data.failure || 0);
    temporary += Number(data.temporary || 0);

    pipelineSum += Number(data.pipelineLatencySum || 0);
    endToEndSum += Number(data.endToEndLatencySum || 0);

    // 🔥 NEW FIELDS (CRITICAL)
    retryDelaySum += Number(data.retryDelaySum || 0);
    queueDelaySum += Number(data.queueDelaySum || 0);
    processingTimeSum += Number(data.processingTimeSum || 0);

    original += Number(data.original || 0);
    retry += Number(data.retry || 0);
  }

  if (total === 0) return null;

  // ----------------------
  // CORE RATES
  // ----------------------
  const failureRate = failure / total;
  const successRate = success / total;
  const temporaryFailureRate = temporary / total;

  // ----------------------
  // LATENCIES
  // ----------------------
  const avgPipelineLatency = pipelineSum / total;
  const avgEndToEndLatency = endToEndSum / total;

  // 🔥 NEW BREAKDOWN
  const avgRetryDelay = retryDelaySum / total;
  const avgQueueDelay = queueDelaySum / total;
  const avgProcessingTime = processingTimeSum / total;

  const retryAmplification =
    original === 0 ? 0 : total / original;

  // ----------------------
  // BASELINE
  // ----------------------
  const storedBaseline = await safeExec(
    () => redisClient.get(BASELINE_KEY),
    null
  );

  const baselineLatency = storedBaseline
    ? parseFloat(storedBaseline)
    : DEFAULT_BASELINE;

  // ----------------------
  // BASELINE UPDATE
  // ----------------------
  if (failureRate < 0.05 && avgPipelineLatency > 0) {
    await updateBaseline(avgPipelineLatency);
  }

  return {
    failure_rate: failureRate,
    success_rate: successRate,
    temporary_failure_rate: temporaryFailureRate,

    avg_pipeline_latency: avgPipelineLatency,
    avg_end_to_end_latency: avgEndToEndLatency,

    // 🔥 NEW INTELLIGENCE
    avg_retry_delay: avgRetryDelay,
    avg_queue_delay: avgQueueDelay,
    avg_processing_time: avgProcessingTime,

    retry_amplification: retryAmplification,
    baseline_latency: baselineLatency,
  };
}

// ----------------------
async function updateBaseline(currentAvg) {
  const alpha = 0.1;

  const stored = await safeExec(() => redisClient.get(BASELINE_KEY), null);
  const prev = stored ? parseFloat(stored) : currentAvg;

  const updated = alpha * currentAvg + (1 - alpha) * prev;

  await safeExec(() =>
    redisClient.set(BASELINE_KEY, updated.toFixed(2), { EX: 3600 })
  );
}

// ----------------------
// 🔥 DEBUG ONLY
// ----------------------
async function recordAmplification(isRetry) {
  const windowKey = `retry_amp:${Math.floor(Date.now() / 60000) * 60000}`;
  const field = isRetry ? 'retry' : 'original';

  await safeExec(async () => {
    const multi = redisClient.multi();
    multi.hIncrBy(windowKey, field, 1);
    multi.expire(windowKey, 180);
    await multi.exec();
  });
}

// ----------------------
module.exports = {
  recordMetrics,
  updateMetrics,
  getSystemMetrics,
  recordAmplification,
};