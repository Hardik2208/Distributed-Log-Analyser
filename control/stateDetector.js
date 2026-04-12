const { THRESHOLDS } = require('./backpressureConfig');

// ----------------------
function getSystemState(metrics, currentState) {

  const latency = metrics.avg_pipeline_latency || 0;
  const retryAmp = metrics.retry_amplification || 0;
  const failure = metrics.failure_rate || 0;

  // ----------------------
  // 🔥 FROM HEALTHY
  // ----------------------
  if (currentState === "HEALTHY") {

    // 🔴 Direct jump to OVERLOADED
    if (
      latency > THRESHOLDS.OVERLOAD.latency ||
      retryAmp > THRESHOLDS.OVERLOAD.retryAmp ||
      failure > THRESHOLDS.OVERLOAD.failure
    ) {
      return "OVERLOADED";
    }

    // 🟡 Move to PRESSURED
    if (
      latency > THRESHOLDS.ENABLE.latency ||
      retryAmp > THRESHOLDS.ENABLE.retryAmp ||
      failure > THRESHOLDS.ENABLE.failure
    ) {
      return "PRESSURED";
    }

    return "HEALTHY";
  }

  // ----------------------
  // 🔥 FROM PRESSURED
  // ----------------------
  if (currentState === "PRESSURED") {

    // 🔴 Escalate
    if (
      latency > THRESHOLDS.OVERLOAD.latency ||
      retryAmp > THRESHOLDS.OVERLOAD.retryAmp ||
      failure > THRESHOLDS.OVERLOAD.failure
    ) {
      return "OVERLOADED";
    }

    // 🟢 Recover (lower thresholds → hysteresis)
    if (
      latency < THRESHOLDS.RECOVERY.latency &&
      retryAmp < THRESHOLDS.RECOVERY.retryAmp &&
      failure < THRESHOLDS.RECOVERY.failure
    ) {
      return "HEALTHY";
    }

    return "PRESSURED";
  }

  // ----------------------
  // 🔥 FROM OVERLOADED
  // ----------------------
  if (currentState === "OVERLOADED") {

    // 🟡 Step down (NOT directly to HEALTHY)
    if (
      latency < THRESHOLDS.ENABLE.latency &&
      retryAmp < THRESHOLDS.ENABLE.retryAmp &&
      failure < THRESHOLDS.ENABLE.failure
    ) {
      return "PRESSURED";
    }

    return "OVERLOADED";
  }

  // ----------------------
  // 🔥 DEFAULT SAFE STATE
  // ----------------------
  return "HEALTHY";
}

module.exports = { getSystemState };