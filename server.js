const express = require('express');
const { connectRedis } = require('./config/redisClient');
const { getMetrics } = require('./metrics/redisMetrics');
const { client } = require('./config/redisClient');

const app = express();

// 🔴 Get metrics by service + window
app.get('/metrics', async (req, res) => {
  try {
    const { service } = req.query;

    if (!service) {
      return res.status(400).json({
        error: "Missing service param"
      });
    }

    const keys = await client.keys(`metrics:${service}:*`);

    if (keys.length === 0) {
      return res.status(404).json({
        message: "No data found for this service"
      });
    }

    const results = [];

    for (const key of keys) {
      const data = await client.hGetAll(key);

      const requestCount = parseInt(data.request_count || 0);
      const errorCount = parseInt(data.error_count || 0);
      const totalLatency = parseInt(data.total_latency || 0);
      const totalIngestionLatency = parseInt(data.total_ingestion_latency || 0);
      const totalPipelineLatency = parseInt(data.total_pipeline_latency || 0);

      // 🔴 ADD THESE (CRITICAL)
      const originalCount = parseInt(data.original_count || 0);
      const retryCount = parseInt(data.retry_count || 0);

      const retryAmplification =
        originalCount > 0 ? retryCount / originalCount : 0;

      const window = key.split(':')[2];

      results.push({
        window,

        request_count: requestCount,
        error_count: errorCount,
        failure_rate: requestCount > 0 ? errorCount / requestCount : 0,

        avg_latency:
          requestCount > 0 ? totalLatency / requestCount : 0,

        avg_pipeline_latency:
          requestCount > 0 ? totalPipelineLatency / requestCount : 0,

        // 🔴 ADD THESE OUTPUTS
        original_count: originalCount,
        retry_count: retryCount,
        retry_amplification: retryAmplification,
        avg_ingestion_latency:
          requestCount > 0 ? totalIngestionLatency / requestCount : 0
      });
    }

    results.sort((a, b) => a.window - b.window);

    res.json({
      service,
      windows: results
    });

  } catch (err) {
    console.error("❌ METRICS ERROR:", err);
    res.status(500).json({ error: err.message || "Failed to fetch metrics" });
  }
});

// 🔴 Health check (basic discipline)
app.get('/health', (req, res) => {
  res.json({ status: "OK" });
});

async function start() {
  await connectRedis();

  app.listen(3000, () => {
    console.log("🚀 Server running on port 3000");
  });
}

start();