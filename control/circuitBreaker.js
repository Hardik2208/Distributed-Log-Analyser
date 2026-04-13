const { redisClient } = require('../config/redisClient');

const CIRCUIT_KEY = 'retry_circuit_state';
const HALF_OPEN_COUNTER_KEY = 'half_open_counter';

// ----------------------
const STATES = {
  CLOSED: 'CLOSED',
  OPEN: 'OPEN',
  HALF_OPEN: 'HALF_OPEN',
};

// ----------------------
// 🔥 TUNING (IMPORTANT)
const OPEN_WINDOW_MS = 5000;            // time before HALF_OPEN
const HALF_OPEN_MAX_ATTEMPTS = 20;      // controlled retry window
const HALF_OPEN_SUCCESS_THRESHOLD = 0.7;
const HALF_OPEN_FAILURE_LIMIT = 3;

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
    console.error("🔥 CB READ ERROR:", err.message);
    return getDefaultState();
  }
}

// ----------------------
async function saveState(stateObj) {
  if (redisClient && redisClient.isOpen) {
    await redisClient.set(
      CIRCUIT_KEY,
      JSON.stringify(stateObj),
      { EX: 300 }
    );
  }
}

// ----------------------
// 🔥 MAIN UPDATE FUNCTION
// ----------------------
async function updateCircuit(metrics = {}) {
  try {
    const stateObj = await getCircuitState();

    let {
      state,
      lastOpenedAt,
      halfOpenSuccess,
      halfOpenFailure,
      halfOpenAttempts,
    } = stateObj;

    const now = Date.now();

    const failureRate = metrics?.failure_rate || 0;
    const temporary = metrics?.temporary_failure_rate || 0;
    const latency = metrics?.avg_pipeline_latency || 0;
    const retryAmp = metrics?.retry_amplification || 0;

    // ======================================================
    // 🔴 HARD OPEN (FAST FAIL)
    // ======================================================
    if (temporary > 0.9 || failureRate > 0.9) {
      if (state !== STATES.OPEN) {
        state = STATES.OPEN;
        lastOpenedAt = now;

        halfOpenAttempts = 0;
        halfOpenSuccess = 0;
        halfOpenFailure = 0;

        console.log("🚨 CB HARD OPEN (system failing)");
      }
    }

    // ======================================================
    // 🔴 NORMAL OPEN CONDITIONS
    // ======================================================
    const shouldOpen =
      temporary > 0.4 ||
      retryAmp > 2.0 ||
      latency > 2000 ||
      failureRate > 0.6;

    // ----------------------
    // CLOSED → OPEN
    // ----------------------
    if (state === STATES.CLOSED) {
      if (shouldOpen) {
        state = STATES.OPEN;
        lastOpenedAt = now;

        halfOpenAttempts = 0;
        halfOpenSuccess = 0;
        halfOpenFailure = 0;

        console.log(
          `🚨 CB OPEN | temp=${temporary.toFixed(2)} latency=${Math.round(latency)}`
        );
      }
    }

    // ----------------------
    // OPEN → HALF_OPEN (RECOVERY WINDOW)
    // ----------------------
    else if (state === STATES.OPEN) {
      if (now - lastOpenedAt > OPEN_WINDOW_MS) {
        state = STATES.HALF_OPEN;

        halfOpenAttempts = 0;
        halfOpenSuccess = 0;
        halfOpenFailure = 0;

        if (redisClient?.isOpen) {
          await redisClient.del(HALF_OPEN_COUNTER_KEY);
        }

        console.log("🟡 CB HALF-OPEN (testing recovery)");
      }
    }

    // ----------------------
    // HALF_OPEN LOGIC
    // ----------------------
    else if (state === STATES.HALF_OPEN) {

      // 🔴 FAST FAIL BACK TO OPEN
      if (halfOpenFailure >= HALF_OPEN_FAILURE_LIMIT) {
        state = STATES.OPEN;
        lastOpenedAt = now;

        halfOpenAttempts = 0;
        halfOpenSuccess = 0;
        halfOpenFailure = 0;

        console.log("🔴 CB RE-OPEN (failures in half-open)");
      }

      // 🔥 DECISION WINDOW
      else if (halfOpenAttempts >= HALF_OPEN_MAX_ATTEMPTS) {

        const successRate =
          halfOpenAttempts > 0
            ? halfOpenSuccess / halfOpenAttempts
            : 0;

        const isHealthy =
          successRate > HALF_OPEN_SUCCESS_THRESHOLD &&
          temporary < 0.2 &&
          retryAmp < 1.5 &&
          latency < 1200;

        if (isHealthy) {
          state = STATES.CLOSED;
          lastOpenedAt = 0;

          console.log("🟢 CB CLOSED (recovered)");
        } else {
          state = STATES.OPEN;
          lastOpenedAt = now;

          console.log("🔴 CB RE-OPEN (not stable yet)");
        }

        halfOpenAttempts = 0;
        halfOpenSuccess = 0;
        halfOpenFailure = 0;
      }
    }

    // ----------------------
    await saveState({
      state,
      lastOpenedAt,
      halfOpenSuccess,
      halfOpenFailure,
      halfOpenAttempts,
    });

    return state;

  } catch (err) {
    console.error("🔥 CB UPDATE ERROR:", err.message);
    return STATES.CLOSED;
  }
}

// ----------------------
// 🔥 SHOULD RETRY (CONTROL SIGNAL)
// ----------------------
async function shouldRetry() {
  const stateObj = await getCircuitState();

  if (stateObj.state === STATES.OPEN) {
    return false;
  }

  if (stateObj.state === STATES.HALF_OPEN) {

    if (!redisClient?.isOpen) return false;

    const count = await redisClient.incr(HALF_OPEN_COUNTER_KEY);

    return count <= HALF_OPEN_MAX_ATTEMPTS;
  }

  return true;
}

// ----------------------
// 🔥 RECORD RESULT (FOR HALF_OPEN LEARNING)
// ----------------------
async function recordHalfOpenResult(success) {
  try {
    const stateObj = await getCircuitState();

    if (stateObj.state !== STATES.HALF_OPEN) return;

    stateObj.halfOpenAttempts += 1;

    if (success) stateObj.halfOpenSuccess += 1;
    else stateObj.halfOpenFailure += 1;

    await saveState(stateObj);

  } catch (err) {
    console.error("🔥 HALF_OPEN RECORD ERROR:", err.message);
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