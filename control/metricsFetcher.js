const { redisClient } = require('../config/redisClient');

// ----------------------
async function fetchLatestMetrics(service = "order") {
  try {
    const windows = await redisClient.sMembers(`metrics:windows:${service}`);
    if (!windows.length) return null;

    // 🔥 TAKE LAST 3 WINDOWS (STABILITY)
    const recentWindows = windows
      .map(Number)
      .sort((a, b) => b - a)
      .slice(0, 3);

    let total = 0;
    let pipelineSum = 0;
    let endToEndSum = 0;

    let success = 0;
    let failure = 0;
    let temporary = 0;

    let original = 0;
    let retry = 0;

    for (const window of recentWindows) {
      const key = `${service}:${window}`;

      const data = await redisClient.hGetAll(key);
      if (!data || Object.keys(data).length === 0) continue;

      total += Number(data.total || 0);

      success += Number(data.success || 0);
      failure += Number(data.failure || 0);
      temporary += Number(data.temporary || 0);

      pipelineSum += Number(data.pipelineLatencySum || 0);
      endToEndSum += Number(data.endToEndLatencySum || 0);

      original += Number(data.original || 0);
      retry += Number(data.retry || 0);
    }

    if (total === 0) return null;

    return {
      avg_pipeline_latency: pipelineSum / total,
      avg_end_to_end_latency: endToEndSum / total,

      success_rate: success / total,
      failure_rate: failure / total,
      temporary_failure_rate: temporary / total,

      retry_amplification:
        original === 0 ? 0 : total / original
    };

  } catch (err) {
    console.error("🔥 fetchLatestMetrics failed:", err.message);
    return null;
  }
}

module.exports = { fetchLatestMetrics };