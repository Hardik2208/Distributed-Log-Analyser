const { THRESHOLDS } = require('./backpressureConfig');

// ----------------------
function getSystemState(metrics, currentState) {

  const latency = metrics.avg_pipeline_latency || 0;
  const retryAmp = metrics.retry_amplification || 0;
  const failure = metrics.failure_rate || 0;
  const temporary = metrics.temporary_failure_rate || 0;

  // ======================================================
  // 🔴 NEW: LATENCY GROWTH DETECTION (PREDICTIVE SIGNAL)
  // ======================================================
  let prevLatency = global.prevLatency ?? latency;
  let growth = latency - prevLatency;

  global.prevLatency = latency;

  const isGrowingFast =
    growth > THRESHOLDS.OVERLOAD.latencyGrowth;

  const isGrowingModerate =
    growth > THRESHOLDS.ENABLE.latencyGrowth;

  // ======================================================
  // 🔥 PRIORITY SIGNALS (CRITICAL)
  // ======================================================

  const isOverloaded =
    temporary > THRESHOLDS.OVERLOAD.temporary ||
    retryAmp > THRESHOLDS.OVERLOAD.retryAmp ||
    latency > THRESHOLDS.OVERLOAD.latency ||
    failure > THRESHOLDS.OVERLOAD.failure ||
    isGrowingFast; // 🔴 NEW

  const isPressured =
    temporary > THRESHOLDS.ENABLE.temporary ||
    retryAmp > THRESHOLDS.ENABLE.retryAmp ||
    latency > THRESHOLDS.ENABLE.latency ||
    failure > THRESHOLDS.ENABLE.failure ||
    isGrowingModerate; // 🔴 NEW

  const isRecovered =
    temporary < THRESHOLDS.RECOVERY.temporary &&
    retryAmp < THRESHOLDS.RECOVERY.retryAmp &&
    latency < THRESHOLDS.RECOVERY.latency &&
    failure < THRESHOLDS.RECOVERY.failure &&
    growth < THRESHOLDS.RECOVERY.latencyGrowth; // 🔴 NEW

  // ======================================================
  // 🔥 STATE TRANSITIONS
  // ======================================================

  // ----------------------
  // FROM HEALTHY
  // ----------------------
  if (currentState === "HEALTHY") {

    if (isOverloaded) return "OVERLOADED";
    if (isPressured) return "PRESSURED";

    return "HEALTHY";
  }

  // ----------------------
  // FROM PRESSURED
  // ----------------------
  if (currentState === "PRESSURED") {

    if (isOverloaded) return "OVERLOADED";
    if (isRecovered) return "HEALTHY";

    return "PRESSURED";
  }

  // ----------------------
  // FROM OVERLOADED
  // ----------------------
  if (currentState === "OVERLOADED") {

    // 🔴 STEP-DOWN ONLY (NO DIRECT HEALTHY JUMP)
    if (
      temporary < THRESHOLDS.ENABLE.temporary &&
      retryAmp < THRESHOLDS.ENABLE.retryAmp &&
      latency < THRESHOLDS.ENABLE.latency &&
      failure < THRESHOLDS.ENABLE.failure &&
      growth < THRESHOLDS.ENABLE.latencyGrowth
    ) {
      return "PRESSURED";
    }

    return "OVERLOADED";
  }

  // ----------------------
  return "HEALTHY";
}

module.exports = { getSystemState };