const { THRESHOLDS } = require('./backpressureConfig');

// ----------------------
function getSystemState(metrics, currentState) {

  const latency = metrics.avg_pipeline_latency || 0;
  const retryAmp = metrics.retry_amplification || 0;
  const failure = metrics.failure_rate || 0;
  const temporary = metrics.temporary_failure_rate || 0;

  // ----------------------
  // 🔥 PRIORITY SIGNALS (CRITICAL)
  // ----------------------

  const isOverloaded =
    temporary > THRESHOLDS.OVERLOAD.temporary ||
    retryAmp > THRESHOLDS.OVERLOAD.retryAmp ||
    latency > THRESHOLDS.OVERLOAD.latency ||
    failure > THRESHOLDS.OVERLOAD.failure;

  const isPressured =
    temporary > THRESHOLDS.ENABLE.temporary ||
    retryAmp > THRESHOLDS.ENABLE.retryAmp ||
    latency > THRESHOLDS.ENABLE.latency ||
    failure > THRESHOLDS.ENABLE.failure;

  const isRecovered =
    temporary < THRESHOLDS.RECOVERY.temporary &&
    retryAmp < THRESHOLDS.RECOVERY.retryAmp &&
    latency < THRESHOLDS.RECOVERY.latency &&
    failure < THRESHOLDS.RECOVERY.failure;

  // ----------------------
  // 🔥 FROM HEALTHY
  // ----------------------
  if (currentState === "HEALTHY") {

    if (isOverloaded) return "OVERLOADED";
    if (isPressured) return "PRESSURED";

    return "HEALTHY";
  }

  // ----------------------
  // 🔥 FROM PRESSURED
  // ----------------------
  if (currentState === "PRESSURED") {

    if (isOverloaded) return "OVERLOADED";
    if (isRecovered) return "HEALTHY";

    return "PRESSURED";
  }

  // ----------------------
  // 🔥 FROM OVERLOADED
  // ----------------------
  if (currentState === "OVERLOADED") {

    // step-down logic (no direct jump to healthy)
    if (
      temporary < THRESHOLDS.ENABLE.temporary &&
      retryAmp < THRESHOLDS.ENABLE.retryAmp &&
      latency < THRESHOLDS.ENABLE.latency &&
      failure < THRESHOLDS.ENABLE.failure
    ) {
      return "PRESSURED";
    }

    return "OVERLOADED";
  }

  // ----------------------
  return "HEALTHY";
}

module.exports = { getSystemState };