module.exports = {
  CHAOS_MODE: true,          // master switch

  NO_BACKOFF: true,
  NO_JITTER: true,
  IMMEDIATE_RETRY: true,

  MAX_RETRIES: 3,           // or Infinity for real chaos

  FORCE_RETRY_SYNC: true,    // no delay spreading

  FAILURE_LATENCY_MS: 2000,  // simulate slow failure
};