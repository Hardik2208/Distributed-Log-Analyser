// control/backpressureConfig.js

const THRESHOLDS = {
  ENABLE: {
    latency: 800,        // enter pressure only when degradation is real
    retryAmp: 0.45
  },

  DISABLE: {
    latency: 500,        // clear recovery buffer (avoid flapping)
    retryAmp: 0.25
  },

  OVERLOAD: {
    latency: 1500,       // trigger before collapse (you saw 2500ms+)
    retryAmp: 0.6
  }
};

module.exports = { THRESHOLDS };