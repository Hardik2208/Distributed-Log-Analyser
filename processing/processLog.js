const { isDuplicate, markProcessed, updateMetrics } = require('../metrics/redisMetrics');
const { validate } = require('./validator');

const getWindow = (timestamp) => {
    return Math.floor(timestamp / 60000) * 60000;
};

// 🔴 CONTROL FAILURE RATE (CHANGE DURING TESTING)
const FAILURE_RATE = 0.1; // 10%
const ENABLE_FAILURE = true;

const processLog = async (log) => {

    console.log("🚨 INSIDE PROCESS LOG");

    // 🔴 1. Validate
    validate(log);

    // 🔴 2. Idempotency FIRST
    const isAlreadyProcessed = await isDuplicate(log.id);
    if (isAlreadyProcessed) {
        console.log("⚠️ DUPLICATE SKIPPED");
        return;
    }

    const service = log.service;
    const status = log.status_code;
    const latency = log.latency_ms;
    const timestamp = log.timestamp;
    const window = getWindow(timestamp);
    const ingestionLatency = log.first_processed_at - log.timestamp;
const endToEndLatency = Date.now() - log.timestamp;

    // 🔴 3. ALWAYS TRACK ATTEMPT
    await updateMetrics({
  service,
  status,
  latency,
  pipelineLatency: endToEndLatency, // existing
  ingestionLatency,                 // 🔴 NEW
  window,
  isRetry: log.source === "RETRY"
});

    // 🔴 4. CONTROLLED FAILURE INJECTION
    if (ENABLE_FAILURE && Math.random() < FAILURE_RATE) {
        console.log("🔥 SIMULATED FAILURE");

        throw {
            type: "TEMPORARY",
            message: "Simulated processing failure"
        };
    }

    // 🔴 5. SUCCESS PATH
    console.log("✅ PROCESS SUCCESS:", log.id);

    // 🔴 CRITICAL: mark processed ONLY after success
    await markProcessed(log.id);
};

module.exports = { processLog };