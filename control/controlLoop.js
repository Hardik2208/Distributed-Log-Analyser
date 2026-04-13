const { getSystemState } = require('./stateDetector');
const { applyBackpressure } = require('./actions');
const { updateCircuit } = require('./circuitBreaker');
const { fetchLatestMetrics } = require('./metricsFetcher'); // 🔴 UPDATED

const { connectRedis, redisClient } = require('../config/redisClient');

const { Kafka } = require('kafkajs');

const kafka = new Kafka({ brokers: ['localhost:9092'] });
const producer = kafka.producer();

// ----------------------
async function startControlLoop(service = "order") {
  let currentState = "HEALTHY";
  let prevStoredState = null;
  let prevCircuitState = null;
  let prevLatency = null;

  await connectRedis();
  await producer.connect();

  setInterval(async () => {
    try {

      const metricsBundle = await fetchLatestMetrics(service);
      if (!metricsBundle) return;

      const { fast, smooth } = metricsBundle;

      if (!fast && !smooth) return;

      // ======================================================
      // 🔴 1. CIRCUIT BREAKER (FAST METRICS)
      // ======================================================
      const circuitState = await updateCircuit(fast || smooth);

      // ======================================================
      // 🔴 2. SYSTEM STATE (SMOOTH METRICS)
      // ======================================================
      const newState = getSystemState(smooth || fast, currentState);

      // 🔴 FORCE COUPLING (CRITICAL)
      let effectiveState = newState;

      if (circuitState === "OPEN") {
        effectiveState = "OVERLOADED";
      }

      // ----------------------
      if (effectiveState !== currentState) {
        console.log(`🔁 STATE: ${currentState} → ${effectiveState}`);
        currentState = effectiveState;
      }

      // ======================================================
      // 🔥 3. STORE GLOBAL STATE
      // ======================================================
      if (prevStoredState !== currentState) {
        await redisClient.set("system:state", currentState);
        prevStoredState = currentState;
      }

      if (prevCircuitState !== circuitState) {
        await redisClient.set("circuit:state", circuitState);
        prevCircuitState = circuitState;
      }

      if (prevLatency !== smooth?.avg_pipeline_latency) {
        await redisClient.set(
          "system:avg_latency",
          smooth?.avg_pipeline_latency || 0
        );
        prevLatency = smooth?.avg_pipeline_latency;
      }

      // ======================================================
      // 🔥 4. BACKPRESSURE (ENFORCED)
      // ======================================================
      await applyBackpressure(currentState, smooth || fast);

      // ======================================================
      // 🔴 5. PRODUCER CONTROL (STRONGER + COUPLED)
      // ======================================================
      let factor = 1.0;

      if (circuitState === "OPEN") {
        factor = 0.05; // 🔴 HARD THROTTLE
      } else if (currentState === "OVERLOADED") {
        factor = 0.1;  // 🔴 stronger than before
      } else if (currentState === "PRESSURED") {
        factor = 0.5;
      }

      await producer.send({
        topic: 'control-signals',
        messages: [{
          value: JSON.stringify({
            type: "RATE_ADJUST",
            factor
          })
        }]
      });

      // ======================================================
      // 🔥 DEBUG (UPGRADED)
      // ======================================================
      console.log("📊", {
        state: currentState,
        circuit: circuitState,
        tempFail: (smooth?.temporary_failure_rate ?? 0).toFixed(2),
        failure: (smooth?.failure_rate ?? 0).toFixed(2),
        latency: Math.round(smooth?.avg_pipeline_latency ?? 0),
        retryAmp: (smooth?.retry_amplification ?? 0).toFixed(2)
      });

    } catch (err) {
      console.error("❌ CONTROL LOOP ERROR:", err.message);
    }
  }, 2000);
}

module.exports = { startControlLoop };