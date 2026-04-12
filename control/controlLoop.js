const { getSystemState } = require('./stateDetector');
const { applyBackpressure } = require('./actions');
const { updateCircuit } = require('./circuitBreaker');
const { connectRedis, redisClient } = require('../config/redisClient');

const { Kafka } = require('kafkajs');

const kafka = new Kafka({ brokers: ['localhost:9092'] });
const producer = kafka.producer();

// ----------------------
// 🔥 SMOOTHING BUFFER
// ----------------------
let latencyHistory = [];
const MAX_HISTORY = 5;

// ----------------------
// 🔥 METRICS FETCH (FIXED)
// ----------------------
async function getLatestMetrics(service) {
  const windows = await redisClient.sMembers(`metrics:windows:${service}`);
  if (!windows.length) return null;

  const sorted = windows.map(Number).sort((a, b) => b - a);
  const recent = sorted.slice(0, 3); // last 3 windows

  let total = 0, failure = 0, latency = 0, pipelineLatency = 0, retry = 0, original = 0;

  for (const w of recent) {
    const data = await redisClient.hGetAll(`${service}:${w}`);
    if (!data) continue;

    total += Number(data.count || 0);
    failure += Number(data.failure || 0);
    latency += Number(data.latency || 0);
    pipelineLatency += Number(data.pipelineLatency || 0);
    retry += Number(data.retry || 0);
    original += Number(data.original || 0);
  }

  if (total === 0) return null;

  // ----------------------
  // 🔥 SMOOTH LATENCY
  // ----------------------
  const currentLatency = pipelineLatency / total;

  latencyHistory.push(currentLatency);
  if (latencyHistory.length > MAX_HISTORY) latencyHistory.shift();

  const smoothedLatency =
    latencyHistory.reduce((a, b) => a + b, 0) / latencyHistory.length;

  return {
    total_attempts: total,
    failure_rate: failure / total,
    avg_latency: latency / total,
    avg_pipeline_latency: smoothedLatency,
    retry_amplification: original === 0 ? 0 : (original + retry) / original
  };
}

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

      const metrics = await getLatestMetrics(service);
      if (!metrics) return;

      // ----------------------
      // 🔥 1. CIRCUIT BREAKER
      // ----------------------
      const circuitState = await updateCircuit(metrics);

      // ----------------------
      // 🔥 2. SYSTEM STATE
      // ----------------------
      const newState = getSystemState(metrics, currentState);

      if (newState !== currentState) {
        console.log(`🔁 STATE: ${currentState} → ${newState}`);
        currentState = newState;
      }

      // ----------------------
      // 🔥 3. STORE GLOBAL STATE (ONLY IF CHANGED)
      // ----------------------
      if (prevStoredState !== currentState) {
        await redisClient.set("system:state", currentState);
        prevStoredState = currentState;
      }

      if (prevCircuitState !== circuitState) {
        await redisClient.set("circuit:state", circuitState);
        prevCircuitState = circuitState;
      }

      if (prevLatency !== metrics.avg_pipeline_latency) {
        await redisClient.set("system:avg_latency", metrics.avg_pipeline_latency);
        prevLatency = metrics.avg_pipeline_latency;
      }

      // ----------------------
      // 🔥 4. BACKPRESSURE ACTIONS
      // ----------------------
      await applyBackpressure(currentState, metrics);

      // ----------------------
      // 🔥 5. PRODUCER CONTROL
      // ----------------------
      let factor = 1.0;

      if (currentState === "OVERLOADED") factor = 0.3;
      else if (currentState === "PRESSURED") factor = 0.6;

      await producer.send({
        topic: 'control-signals',
        messages: [{
          value: JSON.stringify({
            type: "RATE_ADJUST",
            factor
          })
        }]
      });

      // ----------------------
      // 🔥 DEBUG (MEANINGFUL)
      // ----------------------
      console.log("📊", {
        state: currentState,
        circuit: circuitState,
        failure: metrics.failure_rate.toFixed(2),
        latency: Math.round(metrics.avg_pipeline_latency),
        retryAmp: metrics.retry_amplification.toFixed(2)
      });

    } catch (err) {
      console.error("❌ CONTROL LOOP ERROR:", err.message);
    }
  }, 2000);
}

module.exports = { startControlLoop };