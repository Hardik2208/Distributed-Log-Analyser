// control/controlLoop.js

const { fetchLatestMetrics } = require('./metricsFetcher');
const { getSystemState } = require('./stateDetector');
const { applyBackpressure } = require('./actions');

function startControlLoop(service = "auth") {
  let currentState = "HEALTHY";

  setInterval(async () => {
    try {
      const metrics = await fetchLatestMetrics(service);
      if (!metrics) return;

      const newState = getSystemState(metrics, currentState);

      // 🔁 State transition log (only for visibility)
      if (newState !== currentState) {
        console.log(`🔁 STATE CHANGE: ${currentState} → ${newState}`);
        currentState = newState;
      }

      // 🔴 APPLY CONTROL EVERY CYCLE (CRITICAL FIX)
      await applyBackpressure(currentState, metrics);

      console.log("📊 METRICS:", metrics, "STATE:", currentState);

    } catch (err) {
      console.error("❌ CONTROL LOOP ERROR:", err);
    }
  }, 2000);
}

module.exports = { startControlLoop };