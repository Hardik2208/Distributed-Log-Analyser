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
// 🔥 METRICS FETCH (FIXED + ALIGNED)
// ----------------------
async function getLatestMetrics(service) {
  const windows = await redisClient.sMembers(`metrics:windows:${service}`);
  if (!windows.length) return null;

  const sorted = windows.map(Number).sort((a, b) => b - a);
  const recent = sorted.slice(0, 3);

  let total = 0;
  let success = 0;
  let failure = 0;
  let temporary = 0;

  let latencySum = 0;
  let pipelineSum = 0;
  let endToEndSum = 0;

  let retry = 0;
  let original = 0;

  for (const w of recent) {
    const data = await redisClient.hGetAll(`${service}:${w}`);
    if (!data || Object.keys(data).length === 0) continue;

    total += Number(data.total || 0);

    success += Number(data.success || 0);
    failure += Number(data.failure || 0);
    temporary += Number(data.temporary || 0);

    latencySum += Number(data.latencySum || 0);
    pipelineSum += Number(data.pipelineLatencySum || 0);
    endToEndSum += Number(data.endToEndLatencySum || 0);

    retry += Number(data.retry || 0);
    original += Number(data.original || 0);
  }

  if (total === 0) return null;

  // ----------------------
  // 🔥 SMOOTH LATENCY
  // ----------------------
  const currentLatency = pipelineSum / total;

  latencyHistory.push(currentLatency);
  if (latencyHistory.length > MAX_HISTORY) latencyHistory.shift();

  const smoothedLatency =
    latencyHistory.reduce((a, b) => a + b, 0) / latencyHistory.length;

  return {
    total_attempts: total,

    success_rate: success / total,
    failure_rate: failure / total,
    temporary_failure_rate: temporary / total,

    avg_latency: latencySum / total,
    avg_pipeline_latency: smoothedLatency,
    avg_end_to_end_latency: endToEndSum / total,

    retry_amplification:
      original === 0 ? 0 : total / original
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
      // 🔥 3. STORE GLOBAL STATE
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
      // 🔥 4. BACKPRESSURE
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
      // 🔥 DEBUG (UPGRADED)
      // ----------------------
      console.log("📊", {
        state: currentState,
        circuit: circuitState,
        tempFail: metrics.temporary_failure_rate.toFixed(2),
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