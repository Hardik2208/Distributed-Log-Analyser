const { redisClient } = require('../config/redisClient');
const db = require('../db/db');

// ----------------------
async function flushMetrics(service = "order") {
  try {
    const windows = await redisClient.sMembers(`metrics:windows:${service}`);
    if (!windows.length) return;

    const batch = [];

    for (const window of windows) {
      const key = `${service}:${window}`;
      const data = await redisClient.hGetAll(key);

      if (!data || Object.keys(data).length === 0) continue;

      const total = Number(data.total || 0);
      if (total === 0) continue;

      const success = Number(data.success || 0);
      const failure = Number(data.failure || 0);
      const temporary = Number(data.temporary || 0);

      const original = Number(data.original || 0);
      const retry = Number(data.retry || 0);

      const retryDepthSum = Number(data.retryDepthSum || 0);

      const latencySum = Number(data.latencySum || 0);
      const pipelineSum = Number(data.pipelineLatencySum || 0);
      const endToEndSum = Number(data.endToEndLatencySum || 0);
      const ingestionSum = Number(data.ingestionLatencySum || 0);

      const record = {
        service,
        window_start: new Date(Number(window)),

        total_attempts: total,
        success_count: success,
        failure_count: failure,
        temporary_failure_count: temporary,

        original_count: original,
        retry_count: retry,

        retry_amplification:
          original === 0 ? 0 : total / original,

        avg_retry_depth:
          total === 0 ? 0 : (retryDepthSum / total),

        max_retry_depth: 0, // (optional: track separately later)

        avg_latency: total ? (latencySum / total) : 0,
        avg_pipeline_latency: total ? (pipelineSum / total) : 0,
        avg_end_to_end_latency: total ? (endToEndSum / total) : 0,
        avg_ingestion_latency: total ? (ingestionSum / total) : 0
      };

      batch.push(record);
    }

    // ----------------------
    // 🔥 WRITE FIRST, DELETE AFTER (CRITICAL FIX)
    // ----------------------
    if (batch.length > 0) {
      await db.bulkInsertMetrics(batch);
      console.log(`📦 Flushed ${batch.length} windows to DB`);

      // cleanup AFTER success
      for (const window of windows) {
        const key = `${service}:${window}`;
        await redisClient.del(key);
        await redisClient.sRem(`metrics:windows:${service}`, window);
      }
    }

  } catch (err) {
    console.error("🔥 METRICS FLUSH ERROR", err.message);
  }
}

module.exports = { flushMetrics };