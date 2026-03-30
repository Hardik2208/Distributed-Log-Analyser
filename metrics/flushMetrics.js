const { client } = require('../config/redisClient');
const db = require('../db/db');

async function flushMetrics(service = "order") {
  try {
    const windows = await client.sMembers(`metrics:windows:${service}`);
    if (!windows.length) return;

    const batch = [];

    for (const window of windows) {

      const key = `metrics:${service}:${window}`;
      const data = await client.hGetAll(key);

      if (!data || Object.keys(data).length === 0) continue;

      const total = +data.total_attempts || 0;
      if (total === 0) continue;

      const record = {
        service,
        window_start: new Date(parseInt(window)),
        request_count: total,
        error_count: +data.total_failures || 0,
        error_rate: total ? (+data.total_failures / total) : 0,

        avg_latency: total ? (+data.total_latency / total) : 0,
        avg_pipeline_latency: total ? (+data.total_pipeline_latency / total) : 0,
        avg_ingestion_latency: total ? (+data.total_ingestion_latency / total) : 0,

        retry_amplification: data.original_messages
          ? (+data.retry_attempts / data.original_messages)
          : 0,

        avg_retry_depth: data.retry_attempts
          ? (+data.total_retry_depth / data.retry_attempts)
          : 0
      };

      batch.push(record);

      // 🔴 CLEANUP
      await client.del(key);
      await client.sRem(`metrics:windows:${service}`, window);
    }

    // 🔴 BULK INSERT
    await db.bulkInsertMetrics(batch);

    console.log(`📦 Flushed ${batch.length} windows to DB`);

  } catch (err) {
    console.error("🔥 METRICS FLUSH ERROR", err);
  }
}

module.exports = { flushMetrics };