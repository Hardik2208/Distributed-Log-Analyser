const { redisClient } = require('../config/redisClient');
const { pushMetric } = require('../metrics/metricsBuffer'); // 🔥 FIXED IMPORT

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
// 🔥 LIGHTWEIGHT STREAM METRICS (OPTIONAL)
async function recordMetrics({ status, latency }) {
  const entry = JSON.stringify({ status, latency, ts: Date.now() });

  await safeExec(() => redisClient.lPush(WINDOW_KEY, entry));
  await safeExec(() => redisClient.lTrim(WINDOW_KEY, 0, WINDOW_SIZE - 1));
}

// ----------------------
// 🔥 FINAL METRICS ENTRY (SOURCE OF TRUTH)
async function updateMetrics({
  service,
  window,
  isRetry,
  isFirstAttempt,
  retryCount,
  failed,
  latency,
  pipelineLatency,
  ingestionLatency,
  success,
  isRetrySuccess,
  isRetryFailure,
}) {
  const serviceName = service || 'order';

  if (!window) {
    console.error('🔥 INVALID WINDOW', { serviceName, window });
    return;
  }

  // 🔥 REDIRECT TO BUFFER (NO REDIS HERE)
  pushMetric({
    service: serviceName,
    window,
    isRetry,
    isFirstAttempt,
    retryCount,
    failed,
    success,
    latency,
    pipelineLatency,
    ingestionLatency,
    isRetrySuccess,
    isRetryFailure
  });
}

// ----------------------
// 🔥 SYSTEM METRICS (USED BY CONTROL LOOP)
async function getSystemMetrics() {

  let totalSuccess = 0;
  let totalFailures = 0;
  let totalPipelineLatency = 0;
  let totalAttempts = 0;
  let totalOriginal = 0;
  let totalRetry = 0;

  // 🔥 FIX: USE SET INSTEAD OF SCAN
  const windows = await safeExec(
    () => redisClient.sMembers(`metrics:windows:order`),
    []
  );

  if (!windows.length) return null;

  // 🔥 FIX: ONLY LAST 3 WINDOWS (PREVENT OLD DATA POLLUTION)
  const recentWindows = windows
    .map(Number)
    .sort((a, b) => b - a)
    .slice(0, 3);

  for (const w of recentWindows) {
    const key = `order:${w}`;
    const data = await safeExec(() => redisClient.hGetAll(key), {});

    totalSuccess += Number(data.success || 0);
    totalFailures += Number(data.failure || 0);
    totalPipelineLatency += Number(data.pipelineLatency || 0);
    totalAttempts += Number(data.count || 0);

    totalOriginal += Number(data.original || 0);
    totalRetry += Number(data.retry || 0);
  }

  if (totalAttempts === 0) return null;

  // ----------------------
  // 🔥 FINAL METRICS
  // ----------------------
  const failureRate = totalFailures / totalAttempts;
  const successRate = totalSuccess / totalAttempts;
  const avgPipelineLatency = totalPipelineLatency / totalAttempts;

  const retryAmplification =
    totalOriginal === 0 ? 0 : (totalRetry + totalOriginal) / totalOriginal;

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
    avg_pipeline_latency: avgPipelineLatency,
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
// 🔥 ATTEMPT METRICS (NOT FINAL TRUTH)
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