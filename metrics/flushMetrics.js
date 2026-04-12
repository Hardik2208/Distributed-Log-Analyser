const { client } = require('../config/redisClient');
const db = require('../db/db');

async function flushMetrics(service = "order") {
  try {
    const windows = await client.sMembers(`metrics:windows:${service}`);
    if (!windows.length) return;

    const batch = [];

    for (const window of windows) {

      // ✅ FIXED KEY
      const key = `${service}:${window}`;

      const data = await client.hGetAll(key);

      if (!data || Object.keys(data).length === 0) continue;

      const total = +data.count || 0;
      if (total === 0) continue;

      const original = +data.original || 0;
      const retry = +data.retry || 0;

      const record = {
        service,
        window_start: new Date(parseInt(window)),

        request_count: total,
        error_count: +data.failure || 0,
        error_rate: total ? (+data.failure / total) : 0,

        avg_latency: total ? (+data.latency / total) : 0,
        avg_pipeline_latency: total ? (+data.pipelineLatency / total) : 0,
        avg_ingestion_latency: total ? (+data.ingestionLatency / total) : 0,

        // 🔥 FIXED
        retry_amplification:
          original === 0 ? 0 : (original + retry) / original,

        avg_retry_depth:
          retry === 0 ? 0 : (retry / total)
      };

      batch.push(record);

      // cleanup
      await client.del(key);
      await client.sRem(`metrics:windows:${service}`, window);
    }

    if (batch.length > 0) {
      await db.bulkInsertMetrics(batch);
      console.log(`📦 Flushed ${batch.length} windows to DB`);
    }

  } catch (err) {
    console.error("🔥 METRICS FLUSH ERROR", err);
  }
}

module.exports = { flushMetrics };