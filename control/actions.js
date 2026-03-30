// control/actions.js

async function applyBackpressure(state) {

  if (state === "OVERLOADED") {
    console.log("🚨 OVERLOADED → PAUSE MAIN + THROTTLE RETRY");

    await global.pauseMainConsumer?.();
    global.throttleRetryConsumer?.();

  } else if (state === "PRESSURED") {
    console.log("⚠️ PRESSURED → THROTTLE RETRY");

    await global.resumeMainConsumer?.();
    global.throttleRetryConsumer?.();

  } else if (state === "HEALTHY") {
    console.log("✅ HEALTHY → NORMAL FLOW");

    await global.resumeMainConsumer?.();
    global.unthrottleRetryConsumer?.();
  }
}

module.exports = { applyBackpressure };