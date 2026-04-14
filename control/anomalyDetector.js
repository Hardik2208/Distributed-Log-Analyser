// ----------------------
// 🔴 DIVERGENCE CHECK (SLOW SIGNAL)
// ----------------------
function isSystemDiverging(windows) {
  if (!windows || windows.length < 3) return false;

  let latencyIncreases = 0;
  let queueIncreases = 0;

  for (let i = 1; i < windows.length; i++) {
    const prev = windows[i - 1];
    const curr = windows[i];

    const prevQueue =
      prev.avg_ingestion_latency / prev.avg_pipeline_latency;

    const currQueue =
      curr.avg_ingestion_latency / curr.avg_pipeline_latency;

    if (curr.avg_pipeline_latency > prev.avg_pipeline_latency) {
      latencyIncreases++;
    }

    if (currQueue > prevQueue) {
      queueIncreases++;
    }
  }

  return latencyIncreases >= 2 && queueIncreases >= 2;
}

// ----------------------
// 🔴 MAIN DETECTOR
// ----------------------
function detectAnomaly(windows) {
  if (!windows || windows.length === 0) return null;

  const latest = windows[windows.length - 1];

  const pipelineLatency = latest.avg_pipeline_latency;
  const ingestionLatency = latest.avg_ingestion_latency;

  if (!pipelineLatency || pipelineLatency === 0) return null;

  const queuePressure = ingestionLatency / pipelineLatency;

  const latencyHigh = pipelineLatency > 500;
  const queuePressureHigh = queuePressure >= 0.7;
  const retryHigh = latest.retry_amplification > 1.2;

  // ======================================================
  // 🔥 FAST SIGNALS (INSTANT — NO TREND REQUIRED)
  // ======================================================
  const fastSignals = [];

  if (queuePressureHigh) {
    fastSignals.push("HIGH_QUEUE_PRESSURE");
  }

  if (latencyHigh) {
    fastSignals.push("LATENCY_SPIKE");
  }

  if (retryHigh) {
    fastSignals.push("RETRY_SPIKE");
  }

  // ======================================================
  // 🔥 SLOW SIGNALS (CONFIRMED INSTABILITY)
  // ======================================================
  const diverging = isSystemDiverging(windows);

  const slowSignals = [];

  if (diverging && latencyHigh) {
    slowSignals.push("LATENCY_GROWTH");
  }

  if (diverging && queuePressureHigh) {
    slowSignals.push("QUEUE_PRESSURE");
  }

  if (diverging && retryHigh) {
    slowSignals.push("RETRY_INSTABILITY");
  }

  // ======================================================
  // 🔴 NO SIGNALS
  // ======================================================
  if (fastSignals.length === 0 && slowSignals.length === 0) {
    return null;
  }

  // ======================================================
  // 🔴 SEVERITY LOGIC
  // ======================================================
  let severity = "LOW";

  // HIGH = confirmed instability
  if (
    diverging &&
    (pipelineLatency > 1500 || queuePressureHigh)
  ) {
    severity = "HIGH";
  }
  // MEDIUM = strong stress or partial instability
  else if (
    diverging ||
    pipelineLatency > 800 ||
    queuePressureHigh
  ) {
    severity = "MEDIUM";
  }

  // ======================================================
  // 🔴 FINAL OUTPUT
  // ======================================================
  return {
    anomaly_type: slowSignals.length
      ? slowSignals.join(", ")
      : null, // only true anomalies

    fast_signals: fastSignals, // early warnings

    severity,

    reason: `lat=${Math.round(pipelineLatency)}ms queue=${queuePressure.toFixed(
      2
    )} retry=${latest.retry_amplification.toFixed(
      2
    )} diverging=${diverging}`,

    window: latest.window
  };
}

module.exports = { detectAnomaly };