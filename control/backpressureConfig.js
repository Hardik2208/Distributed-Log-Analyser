const THRESHOLDS = {
  OVERLOAD: {
    latency: 1000,        // was 1200 → faster reaction
    retryAmp: 1.6,        // was 1.8 → tighter control
    failure: 0.4,         // was 0.5 → earlier detection
    temporary: 0.5,       // 🔴 ADDED (was missing)
    
    // 🔴 NEW: growth-based trigger
    latencyGrowth: 150    // ms increase between intervals
  },

  ENABLE: {
    latency: 500,         // was 600 → earlier pressure detection
    retryAmp: 1.2,        // was 1.3
    failure: 0.2,         // was 0.25
    temporary: 0.25,      // 🔴 ADDED

    // 🔴 NEW
    latencyGrowth: 80
  },

  RECOVERY: {
    latency: 300,         // was 400 → stricter recovery
    retryAmp: 1.05,       // was 1.1
    failure: 0.08,        // was 0.1
    temporary: 0.1,       // 🔴 ADDED

    // 🔴 NEW
    latencyGrowth: 30
  }
};

module.exports = { THRESHOLDS };