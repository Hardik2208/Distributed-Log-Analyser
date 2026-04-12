const express = require('express');
const { connectRedis, redisClient } = require('./config/redisClient');
const { getSystemMetrics } = require('./metrics/metricsService');
const { getDLQStats } = require('./metrics/redisMetrics');
const { startControlLoop } = require('./control/controlLoop');

const app = express();
app.use(express.json());

// ----------------------
// 🔴 SAFE REDIS WRAPPER
// ----------------------
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

// ----------------------
// 🔴 DB FAILURE CONTROL
// ----------------------
app.post('/control/db/down', async (req, res) => {
  await safeRedis(() => redisClient.set('db:failure', '1'));
  console.log("🔥 DB FAILURE ENABLED");
  res.json({ db: "DOWN" });
});

app.post('/control/db/up', async (req, res) => {
  await safeRedis(() => redisClient.set('db:failure', '0'));
  console.log("✅ DB RESTORED");
  res.json({ db: "UP" });
});

// ----------------------
// 🔥 SYSTEM METRICS
// ----------------------
app.get('/metrics/system', async (req, res) => {
  try {
    const metrics = await getSystemMetrics();
    const state = await safeRedis(() => redisClient.get("system:state"));

    if (!metrics) {
      return res.json({
        success: true,
        state,
        message: "No metrics yet"
      });
    }

    res.json({
      success: true,
      state,
      ...metrics
    });

  } catch (err) {
    console.error("🔥 SYSTEM METRICS ERROR:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// ----------------------
// 🔥 LATEST METRICS
// ----------------------
app.get('/metrics/latest', async (req, res) => {
  try {
    const service = req.query.service || 'order';

    const windows = await safeRedis(
      () => redisClient.sMembers(`metrics:windows:${service}`),
      []
    );

    if (!windows.length) {
      return res.status(404).json({
        success: false,
        message: "No metrics yet"
      });
    }

    const latestWindow = Math.max(
      ...windows.map(w => Number(w)).filter(w => !isNaN(w))
    );

    const key = `${service}:${latestWindow}`;
    const data = await safeRedis(() => redisClient.hGetAll(key), {});

    if (!data || Object.keys(data).length === 0) {
      return res.status(404).json({
        success: false,
        message: "No metrics yet"
      });
    }

    const count = Number(data.count || 0);
    const latency = Number(data.latency || 0);
    const pipelineLatency = Number(data.pipelineLatency || 0);
    const ingestionLatency = Number(data.ingestionLatency || 0);

    res.json({
      success: true,
      service,
      timestamp: Date.now(),
      window: latestWindow,
      total_attempts: count,
      avg_latency: count ? latency / count : 0,
      avg_pipeline_latency: count ? pipelineLatency / count : 0,
      avg_ingestion_latency: count ? ingestionLatency / count : 0
    });

  } catch (err) {
    console.error("🔥 METRICS ERROR:", err.message);
    res.status(500).json({
      success: false,
      error: err.message
    });
  }
});

// ----------------------
// 🔥 WINDOW METRICS
// ----------------------
app.get('/metrics/window', async (req, res) => {
  try {
    const service = req.query.service || 'order';

    const windows = await safeRedis(
      () => redisClient.sMembers(`metrics:windows:${service}`),
      []
    );

    if (!windows.length) {
      return res.json({
        success: true,
        windows: []
      });
    }

    const sortedWindows = windows
      .map(w => Number(w))
      .filter(w => !isNaN(w))
      .sort((a, b) => b - a)
      .slice(0, 10);

    const result = [];

    for (const window of sortedWindows) {
      const key = `${service}:${window}`;
      const data = await safeRedis(() => redisClient.hGetAll(key), {});

      if (!data || Object.keys(data).length === 0) continue;

      const count = Number(data.count || 0);
      const success = Number(data.success || 0);
      const failure = Number(data.failure || 0);

      const latency = Number(data.latency || 0);
      const pipelineLatency = Number(data.pipelineLatency || 0);
      const ingestionLatency = Number(data.ingestionLatency || 0);

      const original = Number(data.original || 0);
      const retry = Number(data.retry || 0);

      const retry_amplification =
        original === 0 ? 0 : (retry / original);

      result.push({
        window,
        total_attempts: count,
        success,
        failures: failure,
        retry_amplification,
        avg_pipeline_latency: count ? pipelineLatency / count : 0,
        avg_ingestion_latency: count ? ingestionLatency / count : 0,
        avg_latency: count ? latency / count : 0
      });
    }

    res.json({
      success: true,
      service,
      windows: result
    });

  } catch (err) {
    console.error("🔥 WINDOW METRICS ERROR:", err.message);
    res.status(500).json({
      success: false,
      error: err.message
    });
  }
});

// ----------------------
// 🔥 DLQ STATS
// ----------------------
app.get('/dlq/stats', async (req, res) => {
  try {
    const stats = await getDLQStats();

    res.json({
      success: true,
      stats
    });

  } catch (err) {
    console.error("🔥 DLQ ERROR:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// ----------------------
// 🔥 HEALTH CHECK
// ----------------------
app.get('/health', async (req, res) => {
  res.json({
    status: "OK",
    redis: redisClient?.isOpen || false,
    timestamp: Date.now()
  });
});

// ----------------------
// 🚀 START SERVER
// ----------------------
async function start() {
  await connectRedis();

  // default state
  await redisClient.set('db:failure', '0');

  // 🔥 START CONTROL LOOP AFTER REDIS
  //startControlLoop("order");

  app.listen(3000, () => {
    console.log("🚀 Server running on 3000");

    console.log("📊 System:");
    console.log("http://localhost:3000/metrics/system");

    console.log("📊 Latest:");
    console.log("http://localhost:3000/metrics/latest?service=order");

    console.log("📊 Window:");
    console.log("http://localhost:3000/metrics/window?service=order");

    console.log("💀 DLQ:");
    console.log("http://localhost:3000/dlq/stats");
  });
}

start();