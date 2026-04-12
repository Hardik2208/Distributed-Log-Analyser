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
// DEFAULT STATE
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
// GET STATE
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
// UPDATE CIRCUIT (CORE)
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
    const latency = metrics?.avg_pipeline_latency || 0;

    // ----------------------
    // 1. OPEN → HALF_OPEN
    // ----------------------
    if (state === STATES.OPEN) {
      if (Date.now() - lastOpenedAt > HALF_OPEN_WINDOW_MS) {
        state = STATES.HALF_OPEN;

        halfOpenSuccess = 0;
        halfOpenFailure = 0;
        halfOpenAttempts = 0;

        console.log("🟡 CIRCUIT HALF-OPEN");
      }
    }

    // ----------------------
    // 2. HALF_OPEN → CLOSED / OPEN
    // ----------------------
    else if (state === STATES.HALF_OPEN) {

      if (halfOpenAttempts >= 10) {

        const successRate =
          halfOpenAttempts > 0
            ? halfOpenSuccess / halfOpenAttempts
            : 0;

        if (
          successRate > 0.8 &&
          failureRate < 0.3 &&
          latency < 1500
        ) {
          state = STATES.CLOSED;
          lastOpenedAt = 0;

          console.log("🟢 CIRCUIT CLOSED (recovered)");
        } else {
          state = STATES.OPEN;
          lastOpenedAt = Date.now();

          console.log("🔴 CIRCUIT RE-OPENED");
        }

        halfOpenSuccess = 0;
        halfOpenFailure = 0;
        halfOpenAttempts = 0;
      }
    }
    if (halfOpenFailure > 5) {
  state = STATES.OPEN;
  lastOpenedAt = Date.now();

  console.log("🔴 FAST FAIL → RE-OPEN");

  halfOpenAttempts = 0;
  halfOpenSuccess = 0;
  halfOpenFailure = 0;
}
    // ----------------------
    // 3. CLOSED → OPEN
    // ----------------------
    else if (state === STATES.CLOSED) {
      if (failureRate > 0.6 || latency > 3000) {
        state = STATES.OPEN;
        lastOpenedAt = Date.now();

        console.log(
          `🚨 CIRCUIT OPEN | failure=${failureRate.toFixed(2)} latency=${Math.round(latency)}ms`
        );
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
// SHOULD RETRY
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
// RECORD HALF_OPEN RESULT
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