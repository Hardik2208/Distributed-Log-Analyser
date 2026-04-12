const THRESHOLDS = {
  OVERLOAD: {
    latency: 1200,
    retryAmp: 1.8,
    failure: 0.5
  },
  ENABLE: {
    latency: 600,
    retryAmp: 1.3,
    failure: 0.25
  },
  RECOVERY: {
    latency: 400,
    retryAmp: 1.1,
    failure: 0.1
  }
};

module.exports = { THRESHOLDS };