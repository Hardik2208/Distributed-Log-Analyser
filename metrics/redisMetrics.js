const { client } = require('../config/redisClient');

async function updateMetrics({ service, status, latency, pipelineLatency, window }) {
  const key = `metrics:${service}:${window}`;

  await client.hIncrBy(key, "request_count", 1);

  if (status >= 500) {
    await client.hIncrBy(key, "error_count", 1);
  }

  await client.hIncrBy(key, "total_latency", latency);

  // 🔴 THIS IS YOUR MISSING LINE
  await client.hIncrBy(key, "total_pipeline_latency", pipelineLatency);

  await client.expire(key, 3600);
}

// 🔴 Fetch metrics (for debugging / UI later)
async function getMetrics(service, window) {
  const key = `metrics:${service}:${window}`;

  try {
    const data = await client.hGetAll(key);

    if (!data || Object.keys(data).length === 0) return null;

    const requestCount = parseInt(data.request_count || 0);
    const errorCount = parseInt(data.error_count || 0);
    const totalLatency = parseInt(data.total_latency || 0);

    const totalPipelineLatency = parseInt(data.total_pipeline_latency || 0);

const avgPipelineLatency = requestCount > 0
  ? totalPipelineLatency / requestCount
  : 0;

  
    return {
  request_count: requestCount,
  error_count: errorCount,
  failure_rate: errorCount / requestCount,
  avg_latency,
  avg_pipeline_latency: avgPipelineLatency
    };

  } catch (err) {
    throw {
      type: "REDIS_ERROR",
      message: err.message
    };
  }
}

// 🔴 Check duplicate (idempotency)
async function isDuplicate(id) {
  try {
    return await client.exists(`log:processed:${id}`);
  } catch (err) {
    throw {
      type: "REDIS_ERROR",
      message: err.message
    };
  }
}

// 🔴 Mark as processed (idempotency)
async function markProcessed(id) {
  try {
    await client.set(`log:processed:${id}`, 1, {
      EX: 3600 // 1 hour TTL
    });
  } catch (err) {
    throw {
      type: "REDIS_ERROR",
      message: err.message
    };
  }
}
// metricsService.js

async function getMetrics(service, window) {
  const key = `metrics:${service}:${window}`;

  const data = await client.hGetAll(key);

  if (!data || Object.keys(data).length === 0) return null;

  const requestCount = parseInt(data.request_count || 0);
  const errorCount = parseInt(data.error_count || 0);
  const totalLatency = parseInt(data.total_latency || 0);

  const failureRate = requestCount > 0
    ? errorCount / requestCount
    : 0;

  const avgLatency = requestCount > 0
    ? totalLatency / requestCount
    : 0;

  return {
    request_count: requestCount,
    error_count: errorCount,
    failure_rate: failureRate,
    avg_latency: avgLatency
  };
}

module.exports = {
  updateMetrics,
  getMetrics,
  isDuplicate,
  markProcessed,
  getMetrics
};