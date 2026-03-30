async function applyBackpressure(state, metrics) {

  const latency = metrics.avg_pipeline_latency;

  if (state === "OVERLOADED") {

    console.log("🚨 OVERLOADED → HARD CONTROL");

    await global.pauseMainConsumer?.();
    global.throttleRetryConsumer?.(0.3); // aggressive

  } else if (state === "PRESSURED") {

    console.log("⚠️ PRESSURED → MODERATE CONTROL");

    await global.resumeMainConsumer?.();

    if (latency > 1200) {
      global.throttleRetryConsumer?.(0.5);
    } else {
      global.throttleRetryConsumer?.(0.7);
    }

  } else if (state === "HEALTHY") {

    console.log("✅ HEALTHY → NORMAL FLOW");

    await global.resumeMainConsumer?.();
    global.unthrottleRetryConsumer?.();
  }
}

module.exports = { applyBackpressure };