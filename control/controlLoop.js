const { getSystemState } = require('./stateDetector');
const { applyBackpressure } = require('./actions');
const { updateCircuit } = require('./circuitBreaker');
const { fetchLatestMetrics } = require('./metricsFetcher');

const { detectAnomaly } = require('./anomalyDetector');
const { buildWindowSeries } = require('../metrics/windowBuilder');

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

  let lastAnomaly = null;

  await connectRedis();
  await producer.connect();

  setInterval(async () => {
    try {

      // ======================================================
      // 🔴 FETCH METRICS
      // ======================================================
      const metricsBundle = await fetchLatestMetrics(service);
      if (!metricsBundle) return;

      const { fast, smooth } = metricsBundle;
      if (!fast && !smooth) return;

      // ======================================================
      // 🔴 BUILD WINDOW SERIES
      // ======================================================
      let windows = [];

      try {
        windows = await buildWindowSeries(service);
      } catch (err) {
        console.error("⚠️ windowBuilder failed:", err.message);
      }

      if (!windows || windows.length === 0) {
        console.warn("⚠️ no window data");
        return;
      }

      // ======================================================
      // 🔴 ANOMALY DETECTION (FIXED)
      // ======================================================
      // ✅ ALWAYS run (fast signals need 1 window)
      const anomaly = detectAnomaly(windows);

      // ======================================================
      // 🔴 CIRCUIT BREAKER (MOVED UP)
      // ======================================================
      const circuitState = await updateCircuit(fast || smooth);

      // ======================================================
      // 🔴 SYSTEM STATE
      // ======================================================
      const newState = getSystemState(smooth || fast, currentState);

      let effectiveState = newState;

      if (circuitState === "OPEN") {
        effectiveState = "OVERLOADED";
      }

      if (effectiveState !== currentState) {
        console.log(`🔁 STATE: ${currentState} → ${effectiveState}`);
        currentState = effectiveState;
      }

      // ======================================================
      // 🔴 PRODUCER CONTROL (MOVED UP FOR CONTEXT)
      // ======================================================
      let factor = 1.0;

      if (circuitState === "OPEN") {
        factor = 0.05;
      } else if (currentState === "OVERLOADED") {
        factor = 0.1;
      } else if (currentState === "PRESSURED") {
        factor = 0.5;
      }

      // ======================================================
      // 🔴 STORE ANOMALY (FIXED LOGIC)
      // ======================================================
      if (
        anomaly &&
        (
          anomaly.anomaly_type !== null ||
          (anomaly.fast_signals && anomaly.fast_signals.length > 0)
        ) &&
        (
          !lastAnomaly ||
          anomaly.anomaly_type !== lastAnomaly.anomaly_type ||
          anomaly.severity !== lastAnomaly.severity ||
          JSON.stringify(anomaly.fast_signals) !== JSON.stringify(lastAnomaly.fast_signals)
        )
      ) {
        lastAnomaly = anomaly;

        const enriched = {
          ...anomaly,
          service,
          timestamp: Date.now(),

          metrics_snapshot: {
            latency: Math.round(smooth?.avg_pipeline_latency ?? 0),
            ingestion: Math.round(smooth?.avg_ingestion_latency ?? 0),
            retry: +(smooth?.retry_amplification ?? 0).toFixed(2),
            state: currentState
          },

          context: {
            window_series: windows.slice(-3),
            control: {
              state: currentState,
              circuit: circuitState,
              factor
            }
          }
        };

        console.log("🚨 ANOMALY:", enriched);

        try {
          await redisClient.lPush(
            `anomalies:${service}`,
            JSON.stringify(enriched)
          );

          await redisClient.lTrim(`anomalies:${service}`, 0, 100);
        } catch (err) {
          console.error("⚠️ anomaly store failed:", err.message);
        }
      }

      // ======================================================
      // 🔴 DEBUG (FIXED)
      // ======================================================
      if (
        !anomaly ||
        (
          anomaly.anomaly_type === null &&
          (!anomaly.fast_signals || anomaly.fast_signals.length === 0)
        )
      ) {
        console.log("ℹ️ No anomaly → system stable or controlled");
      }

      // ======================================================
      // 🔴 STORE GLOBAL STATE
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
      // 🔴 BACKPRESSURE
      // ======================================================
      await applyBackpressure(currentState, smooth || fast);

      // ======================================================
      // 🔴 PRODUCER CONTROL SEND
      // ======================================================
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
      // 🔴 DEBUG METRICS
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