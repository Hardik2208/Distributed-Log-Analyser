const { redisClient } = require('../config/redisClient');

const CIRCUIT_KEY = 'retry_circuit_state';

const HALF_OPEN_WINDOW_MS = 5000;
const HALF_OPEN_SAMPLE_RATE = 0.2;

const STATES = {
  CLOSED: 'CLOSED',
  OPEN: 'OPEN',
  HALF_OPEN: 'HALF_OPEN',
};

// ----------------------
function getDefaultState() {
  return {
    state: STATES.CLOSED,
    lastOpenedAt: 0,
    halfOpenSuccess: 0,
    halfOpenFailure: 0,
    halfOpenAttempts: 0,
  };
}

// ----------------------
async function getCircuitState() {
  try {
    if (!redisClient || !redisClient.isOpen) {
      return getDefaultState();
    }

    const data = await redisClient.get(CIRCUIT_KEY);
    if (!data) return getDefaultState();

    return {
      ...getDefaultState(),
      ...JSON.parse(data),
    };

  } catch (err) {
    console.error("🔥 CIRCUIT STATE READ ERROR:", err.message);
    return getDefaultState();
  }
}

// ----------------------
// 🔥 UPDATED CORE LOGIC
// ----------------------
async function updateCircuit(metrics) {
  try {
    const stateObj = await getCircuitState();

    let {
      state,
      lastOpenedAt,
      halfOpenSuccess,
      halfOpenFailure,
      halfOpenAttempts,
    } = stateObj;

    const failureRate = metrics?.failure_rate || 0;
    const temporary = metrics?.temporary_failure_rate || 0;
    const latency = metrics?.avg_pipeline_latency || 0;
    const retryAmp = metrics?.retry_amplification || 0;

    // ----------------------
    // 🔴 EARLY OPEN (CRITICAL FIX)
    // ----------------------
    const shouldOpen =
      temporary > 0.3 ||
      retryAmp > 2.5 ||
      failureRate > 0.6 ||
      latency > 3000;

    // ----------------------
    // 1. CLOSED → OPEN
    // ----------------------
    if (state === STATES.CLOSED) {
      if (shouldOpen) {
        state = STATES.OPEN;
        lastOpenedAt = Date.now();

        console.log(
          `🚨 CIRCUIT OPEN | temp=${temporary.toFixed(2)} retryAmp=${retryAmp.toFixed(2)} latency=${Math.round(latency)}`
        );
      }
    }

    // ----------------------
    // 2. OPEN → HALF_OPEN
    // ----------------------
    else if (state === STATES.OPEN) {
      if (Date.now() - lastOpenedAt > HALF_OPEN_WINDOW_MS) {
        state = STATES.HALF_OPEN;

        halfOpenSuccess = 0;
        halfOpenFailure = 0;
        halfOpenAttempts = 0;

        console.log("🟡 CIRCUIT HALF-OPEN");
      }
    }

    // ----------------------
    // 3. HALF_OPEN LOGIC
    // ----------------------
    else if (state === STATES.HALF_OPEN) {

      // 🔴 FAST FAIL
      if (halfOpenFailure > 3) {
        state = STATES.OPEN;
        lastOpenedAt = Date.now();

        console.log("🔴 FAST FAIL → RE-OPEN");

        halfOpenAttempts = 0;
        halfOpenSuccess = 0;
        halfOpenFailure = 0;
      }

      // 🔥 DECISION WINDOW
      else if (halfOpenAttempts >= 10) {

        const successRate =
          halfOpenAttempts > 0
            ? halfOpenSuccess / halfOpenAttempts
            : 0;

        const isHealthy =
          successRate > 0.8 &&
          temporary < 0.1 &&
          retryAmp < 1.5 &&
          latency < 1500;

        if (isHealthy) {
          state = STATES.CLOSED;
          lastOpenedAt = 0;

          console.log("🟢 CIRCUIT CLOSED (recovered)");
        } else {
          state = STATES.OPEN;
          lastOpenedAt = Date.now();

          console.log("🔴 CIRCUIT RE-OPENED");
        }

        halfOpenAttempts = 0;
        halfOpenSuccess = 0;
        halfOpenFailure = 0;
      }
    }

    // ----------------------
    // SAVE STATE
    // ----------------------
    if (redisClient && redisClient.isOpen) {
      await redisClient.set(
        CIRCUIT_KEY,
        JSON.stringify({
          state,
          lastOpenedAt,
          halfOpenSuccess,
          halfOpenFailure,
          halfOpenAttempts,
        }),
        { EX: 300 }
      );
    }

    return state;

  } catch (err) {
    console.error("🔥 CIRCUIT UPDATE ERROR:", err.message);
    return STATES.CLOSED;
  }
}

// ----------------------
async function shouldRetry() {
  const { state } = await getCircuitState();

  if (state === STATES.OPEN) return false;

  if (state === STATES.HALF_OPEN) {
    return Math.random() < HALF_OPEN_SAMPLE_RATE;
  }

  return true;
}

// ----------------------
async function recordHalfOpenResult(success) {
  try {
    const stateObj = await getCircuitState();

    if (stateObj.state !== STATES.HALF_OPEN) return;

    stateObj.halfOpenAttempts += 1;

    if (success) stateObj.halfOpenSuccess += 1;
    else stateObj.halfOpenFailure += 1;

    if (redisClient && redisClient.isOpen) {
      await redisClient.set(
        CIRCUIT_KEY,
        JSON.stringify(stateObj),
        { EX: 300 }
      );
    }

  } catch (err) {
    console.error("🔥 HALF_OPEN TRACK ERROR:", err.message);
  }
}

// ----------------------
module.exports = {
  updateCircuit,
  shouldRetry,
  getCircuitState,
  recordHalfOpenResult,
  STATES,
};