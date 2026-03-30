const express = require('express');
const { connectRedis, client } = require('./config/redisClient');
const { getMetrics, getDLQStats } = require('./metrics/redisMetrics');
const { startControlLoop } = require('./control/controlLoop');
const { flushMetrics } = require('./metrics/metricsFlusher');
const db = require('./db/db'); // 🔴 REAL DB (not fakeDb)

const fakeDb = require('./db/fakeDb'); // 🔥 failure simulation

const app = express();
app.use(express.json());

const DEFAULT_SERVICE = "order";

// --------------------------------
// 🔴 START CONTROL LOOP
// --------------------------------
startControlLoop(DEFAULT_SERVICE);

// --------------------------------
// 🔴 START METRICS FLUSHER
// --------------------------------
setInterval(() => {
  flushMetrics(DEFAULT_SERVICE);
}, 5000);

// --------------------------------
// 🔥 DB FAILURE CONTROL
// --------------------------------
app.post('/control/db/down', async (req, res) => {
  try {
    await fakeDb.enableFailure();
    console.log("🔥 DB FAILURE ENABLED");
    res.json({ db: "DOWN" });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/control/db/up', async (req, res) => {
  try {
    await fakeDb.disableFailure();
    console.log("✅ DB RECOVERY");
    res.json({ db: "UP" });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --------------------------------
// 🔴 REAL-TIME METRICS (REDIS)
// --------------------------------

// GET /metrics?service=order
app.get('/metrics', async (req, res) => {
  try {
    const service = req.query.service || DEFAULT_SERVICE;

    // 🔴 NO KEYS — use tracked windows
    const windows = await client.sMembers(`metrics:windows:${service}`);
    if (!windows.length) {
      return res.status(404).json({ message: 'No data found' });
    }

    const results = [];

    for (const window of windows) {
      const metrics = await getMetrics(service, window);

      if (metrics) {
        results.push({ window: Number(window), ...metrics });
      }
    }

    results.sort((a, b) => a.window - b.window);

    res.json({ type: "realtime", service, windows: results });

  } catch (err) {
    console.error('❌ METRICS ERROR:', err);
    res.status(500).json({ error: err.message });
  }
});

// --------------------------------
// 🔴 HISTORICAL METRICS (MYSQL)
// --------------------------------

// GET /metrics/db?service=order&limit=50
app.get('/metrics/db', async (req, res) => {
  try {
    const service = req.query.service || DEFAULT_SERVICE;
    const limit = parseInt(req.query.limit) || 50;

    const [rows] = await db.pool.query(
      `
      SELECT *
      FROM metrics_window
      WHERE service = ?
      ORDER BY window_start DESC
      LIMIT ?
      `,
      [service, limit]
    );

    res.json({
      type: "historical",
      service,
      count: rows.length,
      data: rows
    });

  } catch (err) {
    console.error('❌ DB METRICS ERROR:', err);
    res.status(500).json({ error: err.message });
  }
});

// --------------------------------
// 🔴 DLQ METRICS
// --------------------------------
app.get('/metrics/dlq', async (req, res) => {
  try {
    const stats = await getDLQStats();
    res.json(stats);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --------------------------------
// 🔴 BACKPRESSURE CONTROLS
// --------------------------------
app.post('/control/throttle', (req, res) => {
  if (typeof global.throttleRetryConsumer === 'function') {
    global.throttleRetryConsumer();
    return res.json({ throttled: true });
  }
  res.status(503).json({ error: 'Retry consumer not running' });
});

app.post('/control/unthrottle', (req, res) => {
  if (typeof global.unthrottleRetryConsumer === 'function') {
    global.unthrottleRetryConsumer();
    return res.json({ throttled: false });
  }
  res.status(503).json({ error: 'Retry consumer not running' });
});

// --------------------------------
// 🔴 MAIN CONSUMER CONTROL
// --------------------------------
app.post('/control/pause', async (req, res) => {
  const ms = parseInt(req.query.ms) || 15000;

  if (typeof global.pauseMainConsumer === 'function') {
    await global.pauseMainConsumer();

    setTimeout(() => {
      global.resumeMainConsumer?.();
    }, ms);

    return res.json({ paused: true, resumesIn: ms });
  }

  res.status(503).json({ error: 'Main consumer not running' });
});

app.post('/control/resume', async (req, res) => {
  if (typeof global.resumeMainConsumer === 'function') {
    await global.resumeMainConsumer();
    return res.json({ paused: false });
  }

  res.status(503).json({ error: 'Main consumer not running' });
});

// --------------------------------
// 🔴 HEALTH
// --------------------------------
app.get('/health', (req, res) => {
  res.json({ status: 'OK', ts: Date.now() });
});

// --------------------------------
// 🔴 START SERVER
// --------------------------------
async function start() {
  await connectRedis();

  app.listen(3000, () => {
    console.log('🚀 Server running on http://localhost:3000');

    console.log('\n📊 REAL-TIME METRICS (Redis)');
    console.log('GET  /metrics?service=order');

    console.log('\n📊 HISTORICAL METRICS (MySQL)');
    console.log('GET  /metrics/db?service=order&limit=50');

    console.log('\n📊 DLQ');
    console.log('GET  /metrics/dlq');

    console.log('\n⚙️ CONTROL');
    console.log('POST /control/throttle');
    console.log('POST /control/unthrottle');
    console.log('POST /control/pause?ms=15000');
    console.log('POST /control/resume');

    console.log('\n🔥 FAILURE TEST');
    console.log('POST /control/db/down');
    console.log('POST /control/db/up');
  });
}

start();