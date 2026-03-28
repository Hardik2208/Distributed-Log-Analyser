const { isDuplicate, markProcessed, updateMetrics } = require('../metrics/redisMetrics');
const {validate} = require('./validator');

const getWindow = (timestamp) => {
    return Math.floor(timestamp / 60000) * 60000;
};

const processLog = async (log) => {

    // 🔴 1. Validate (throws if invalid)
    validate(log);

    // 🔴 2. Idempotency check
    const isAlreadyProcessed = await isDuplicate(log.id);
    if (isAlreadyProcessed) return;

    // 🔴 3. Extract required fields
    const service = log.service;
    const status = log.status_code;
    const latency = log.latency_ms;
    const timestamp = log.timestamp;

    // 🔴 4. Assign window
    const window = getWindow(timestamp);

    // 🔴 5. Update metrics
    await updateMetrics({
        service,
        status,
        latency,
        pipelineLatency: log.pipeline_latency, // 🔴 MUST EXIST
        window
    });
    
    console.log("PIPELINE:", log.pipeline_latency);
    // 🔴 6. Mark processed (AFTER success)
    await markProcessed(log.id);
};

module.exports = { processLog };