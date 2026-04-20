const express = require('express');
const { connectRedis, redisClient } = require('./config/redisClient');
const { getSystemMetrics } = require('./metrics/metricsService');
const { getDLQStats } = require('./metrics/redisMetrics');
const { startControlLoop } = require('./control/controlLoop');

const app = express();
app.use(express.json());

// --------------------------------
// ENV (DOCKER SAFE)
// --------------------------------
const PORT = process.env.PORT || 3000;
const KAFKA_BROKER = process.env.KAFKA_BROKER;

// 🔥 ADD THIS (visibility, no logic change)
console.log("🌐 ENV CONFIG →", {
  PORT,
  KAFKA_BROKER,
  REDIS_URL: process.env.REDIS_URL
});

// --------------------------------
// REDIS SAFE WRAPPER
// --------------------------------
async function safeRedis(action, fallback = null) {
  try {
    if (!redisClient || !redisClient.isOpen) {
      console.warn("⚠️ Redis unavailable");
      return fallback;
    }
    return await action();
  } catch (err) {
    console.error("🔥 Redis error:", err.message);
    return fallback;
  }
}

// --------------------------------
// DB CONTROL
// --------------------------------
app.post('/control/db/down', async (req, res) => {
  await safeRedis(() => redisClient.set('db:failure', '1'));
  res.json({ db: "DOWN" });
});

app.post('/control/db/up', async (req, res) => {
  await safeRedis(() => redisClient.set('db:failure', '0'));
  res.json({ db: "UP" });
});

// --------------------------------
// SYSTEM METRICS
// --------------------------------
app.get('/metrics/system', async (req, res) => {
  try {
    const metrics = await getSystemMetrics();
    const state = await safeRedis(() => redisClient.get("system:state"));

    res.json({
      success: true,
      state,
      ...(metrics || {})
    });

  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --------------------------------
// LATEST WINDOW METRICS
// --------------------------------
app.get('/metrics/latest', async (req, res) => {
  try {
    const service = req.query.service || 'order';

    const windows = await safeRedis(
      () => redisClient.sMembers(`metrics:windows:${service}`),
      []
    );

    if (!windows.length) {
      return res.status(404).json({ success: false });
    }

    const latest = Math.max(...windows.map(Number));
    const data = await safeRedis(
      () => redisClient.hGetAll(`${service}:${latest}`),
      {}
    );

    if (!data || Object.keys(data).length === 0) {
      return res.status(404).json({ success: false });
    }

    const total = Number(data.total || 0);
    if (total === 0) {
      return res.json({ success: true, window: latest, total_attempts: 0 });
    }

    res.json({
      success: true,
      window: latest,

      total_attempts: total,
      success: Number(data.success || 0),
      failures: Number(data.failure || 0),
      temporary_failures: Number(data.temporary || 0),

      retry_amplification:
        Number(data.original || 0) === 0
          ? 0
          : total / Number(data.original || 0),

      avg_pipeline_latency:
        Number(data.pipelineLatencySum || 0) / total,

      avg_end_to_end_latency:
        Number(data.endToEndLatencySum || 0) / total,

      avg_ingestion_latency:
        Number(data.ingestionLatencySum || 0) / total,

      avg_latency:
        Number(data.latencySum || 0) / total,

      avg_retry_delay:
        Number(data.retryDelaySum || 0) / total,

      avg_queue_delay:
        Number(data.queueDelaySum || 0) / total,

      avg_processing_time:
        Number(data.processingTimeSum || 0) / total
    });

  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --------------------------------
// WINDOW METRICS
// --------------------------------
app.get('/metrics/window', async (req, res) => {
  try {
    const service = req.query.service || 'order';

    const windows = await safeRedis(
      () => redisClient.sMembers(`metrics:windows:${service}`),
      []
    );

    const result = [];

    for (const window of windows.map(Number).sort((a,b)=>b-a).slice(0,10)) {
      const data = await safeRedis(
        () => redisClient.hGetAll(`${service}:${window}`),
        {}
      );

      if (!data || Object.keys(data).length === 0) continue;

      const total = Number(data.total || 0);

      result.push({
        window,
        total_attempts: total,
        success: Number(data.success || 0),
        failures: Number(data.failure || 0),
        temporary_failures: Number(data.temporary || 0),

        retry_amplification:
          Number(data.original || 0) === 0
            ? 0
            : total / Number(data.original || 0),

        avg_pipeline_latency:
          total ? Number(data.pipelineLatencySum || 0) / total : 0,

        avg_end_to_end_latency:
          total ? Number(data.endToEndLatencySum || 0) / total : 0,

        avg_ingestion_latency:
          total ? Number(data.ingestionLatencySum || 0) / total : 0,

        avg_latency:
          total ? Number(data.latencySum || 0) / total : 0,

        avg_retry_delay:
          total ? Number(data.retryDelaySum || 0) / total : 0,

        avg_queue_delay:
          total ? Number(data.queueDelaySum || 0) / total : 0,

        avg_processing_time:
          total ? Number(data.processingTimeSum || 0) / total : 0
      });
    }

    res.json({ success: true, windows: result });

  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --------------------------------
// ANOMALIES
// --------------------------------
app.get('/anomalies', async (req, res) => {
  try {
    const service = req.query.service || 'order';

    const data = await safeRedis(
      () => redisClient.lRange(`anomalies:${service}`, 0, 20),
      []
    );

    const parsed = data.map(d => {
      try { return JSON.parse(d); } catch { return null; }
    }).filter(Boolean);

    res.json({
      success: true,
      count: parsed.length,
      anomalies: parsed
    });

  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/anomalies/latest', async (req, res) => {
  try {
    const service = req.query.service || 'order';

    const data = await safeRedis(
      () => redisClient.lIndex(`anomalies:${service}`, 0),
      null
    );

    let parsed = null;
    try { parsed = JSON.parse(data); } catch {}

    res.json({
      success: true,
      anomaly: parsed
    });

  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --------------------------------
// DLQ
// --------------------------------
app.get('/dlq/stats', async (req, res) => {
  try {
    const stats = await getDLQStats();
    res.json({ success: true, stats });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --------------------------------
// HEALTH
// --------------------------------
app.get('/health', (req, res) => {
  res.json({
    status: "OK",
    redis: redisClient?.isOpen || false,
    timestamp: Date.now()
  });
});

// --------------------------------
// START SERVER
// --------------------------------
async function start() {
  try {
    console.log("🔌 Connecting to Redis...");
    await connectRedis();

    await redisClient.set('db:failure', '0');

    // 🔥 ONLY CHANGE: make control loop safe
    try {
      console.log("🎛️ Starting control loop...");
      startControlLoop("order");
    } catch (err) {
      console.error("⚠️ Control loop failed:", err.message);
    }

    app.listen(PORT, () => {
      console.log(`🚀 Server running on port ${PORT}`);
    });

  } catch (err) {
    console.error("🔥 SERVER START FAILED:", err);
    process.exit(1);
  }
}

// --------------------------------
// GRACEFUL SHUTDOWN
// --------------------------------
process.on("SIGTERM", async () => {
  console.log("🛑 Shutting down server...");
  try {
    if (redisClient?.isOpen) {
      await redisClient.quit();
    }
  } catch {}
  process.exit(0);
});

start();