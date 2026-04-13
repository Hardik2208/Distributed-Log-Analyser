const { redisClient } = require('../config/redisClient');

// ----------------------
async function fetchLatestMetrics(service = "order") {
  try {
    const windows = await redisClient.sMembers(`metrics:windows:${service}`);
    if (!windows.length) return null;

    // ----------------------
    // 🔥 SORT WINDOWS
    // ----------------------
    const sorted = windows.map(Number).sort((a, b) => b - a);

    const latestWindow = sorted[0];          // 🔴 FAST (CB)
    const recentWindows = sorted.slice(0, 3); // 🧠 SMOOTH (BP)

    // ----------------------
    // 🔴 FAST METRICS (LATEST WINDOW ONLY)
    // ----------------------
    let fast = {
      total: 0,
      success: 0,
      failure: 0,
      temporary: 0,
      pipelineSum: 0,
      endToEndSum: 0,
      original: 0,
      retry: 0,
    };

    if (latestWindow) {
      const data = await redisClient.hGetAll(`${service}:${latestWindow}`);
      if (data && Object.keys(data).length > 0) {
        fast.total += Number(data.total || 0);
        fast.success += Number(data.success || 0);
        fast.failure += Number(data.failure || 0);
        fast.temporary += Number(data.temporary || 0);
        fast.pipelineSum += Number(data.pipelineLatencySum || 0);
        fast.endToEndSum += Number(data.endToEndLatencySum || 0);
        fast.original += Number(data.original || 0);
        fast.retry += Number(data.retry || 0);
      }
    }

    // ----------------------
    // 🧠 SMOOTH METRICS (LAST 3 WINDOWS)
    // ----------------------
    let smooth = {
      total: 0,
      success: 0,
      failure: 0,
      temporary: 0,
      pipelineSum: 0,
      endToEndSum: 0,
      original: 0,
      retry: 0,
    };

    for (const window of recentWindows) {
      const key = `${service}:${window}`;
      const data = await redisClient.hGetAll(key);

      if (!data || Object.keys(data).length === 0) continue;

      smooth.total += Number(data.total || 0);
      smooth.success += Number(data.success || 0);
      smooth.failure += Number(data.failure || 0);
      smooth.temporary += Number(data.temporary || 0);
      smooth.pipelineSum += Number(data.pipelineLatencySum || 0);
      smooth.endToEndSum += Number(data.endToEndLatencySum || 0);
      smooth.original += Number(data.original || 0);
      smooth.retry += Number(data.retry || 0);
    }

    // ----------------------
    // 🔴 VALIDATION
    // ----------------------
    if (fast.total === 0 && smooth.total === 0) return null;

    // ----------------------
    // 🔥 COMPUTE METRICS
    // ----------------------

    // ⚡ FAST (FOR CIRCUIT BREAKER)
    const fastMetrics = fast.total > 0 ? {
      avg_pipeline_latency: fast.pipelineSum / fast.total,
      avg_end_to_end_latency: fast.endToEndSum / fast.total,

      success_rate: fast.success / fast.total,
      failure_rate: fast.failure / fast.total,
      temporary_failure_rate: fast.temporary / fast.total,

      retry_amplification:
        fast.original === 0 ? 0 : fast.total / fast.original
    } : null;

    // 🧠 SMOOTH (FOR BACKPRESSURE)
    const smoothMetrics = smooth.total > 0 ? {
      avg_pipeline_latency: smooth.pipelineSum / smooth.total,
      avg_end_to_end_latency: smooth.endToEndSum / smooth.total,

      success_rate: smooth.success / smooth.total,
      failure_rate: smooth.failure / smooth.total,
      temporary_failure_rate: smooth.temporary / smooth.total,

      retry_amplification:
        smooth.original === 0 ? 0 : smooth.total / smooth.original
    } : null;

    return {
      fast: fastMetrics,     // 🔴 USE THIS FOR CIRCUIT BREAKER
      smooth: smoothMetrics  // 🧠 USE THIS FOR BACKPRESSURE
    };

  } catch (err) {
    console.error("🔥 fetchLatestMetrics failed:", err.message);
    return null;
  }
}

module.exports = { fetchLatestMetrics };