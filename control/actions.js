// control/actions.js

const { getCircuitState, STATES } = require('./circuitBreaker');

// ----------------------
async function applyBackpressure(state, metrics = {}) {

  try {
    const { state: circuitState } = await getCircuitState();

    // ======================================================
    // 🔴 HARD CONTROL: CIRCUIT OPEN (HIGHEST PRIORITY)
    // ======================================================
    if (circuitState === STATES.OPEN) {
      console.log("🚨 CB OPEN → HARD THROTTLE (PAUSE MAIN + STOP RETRIES)");

      // Stop retry pressure completely
      global.throttleRetryConsumer?.();

      // Aggressively reduce intake
      await global.pauseMainConsumer?.();

      return;
    }

    // ======================================================
    // 🔥 NORMAL STATE-BASED CONTROL
    // ======================================================

    // ----------------------
    // OVERLOADED
    // ----------------------
    if (state === "OVERLOADED") {
      console.log("🚨 OVERLOADED → PAUSE MAIN + THROTTLE RETRY");

      await global.pauseMainConsumer?.();
      global.throttleRetryConsumer?.();

      return;
    }

    // ----------------------
    // PRESSURED
    // ----------------------
    if (state === "PRESSURED") {
      console.log("⚠️ PRESSURED → THROTTLE RETRY (MAIN ACTIVE)");

      await global.resumeMainConsumer?.();
      global.throttleRetryConsumer?.();

      return;
    }

    // ----------------------
    // HEALTHY
    // ----------------------
    if (state === "HEALTHY") {
      console.log("✅ HEALTHY → NORMAL FLOW");

      await global.resumeMainConsumer?.();
      global.unthrottleRetryConsumer?.();

      return;
    }

  } catch (err) {
    console.error("🔥 BACKPRESSURE ACTION ERROR:", err.message);
  }
}

module.exports = { applyBackpressure };