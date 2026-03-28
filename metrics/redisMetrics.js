const { client } = require('../config/redisClient');

async function updateMetrics({
  service,
  status,
  latency,
  pipelineLatency,
  window,
  isRetry
}) {
  const key = `metrics:${service}:${window}`;

  await client.hIncrBy(key, "request_count", 1);

  if (isRetry) {
    await client.hIncrBy(key, "retry_count", 1);
  } else {
    await client.hIncrBy(key, "original_count", 1);
  }

  if (status >= 500) {
    await client.hIncrBy(key, "error_count", 1);
  }

  await client.hIncrBy(key, "total_latency", latency);
  await client.hIncrBy(key, "total_pipeline_latency", pipelineLatency);

  await client.expire(key, 3600);
}

// 🔴 RESTORE THIS
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

// 🔴 RESTORE THIS
async function markProcessed(id) {
  try {
    await client.set(`log:processed:${id}`, 1, {
      EX: 3600
    });
  } catch (err) {
    throw {
      type: "REDIS_ERROR",
      message: err.message
    };
  }
}

// 🔴 KEEP THIS
async function addToSet(key, value) {
  try {
    await client.sAdd(key, value);
  } catch (err) {
    throw {
      type: "REDIS_ERROR",
      message: err.message
    };
  }
}

async function getMetrics(service, window) {
  const key = `metrics:${service}:${window}`;

  const data = await client.hGetAll(key);
  if (!data || Object.keys(data).length === 0) return null;

  const requestCount = parseInt(data.request_count || 0);
  const errorCount = parseInt(data.error_count || 0);
  const totalLatency = parseInt(data.total_latency || 0);
  const totalPipelineLatency = parseInt(data.total_pipeline_latency || 0);

  const originalCount = parseInt(data.original_count || 0);
  const retryCount = parseInt(data.retry_count || 0);

  return {
    request_count: requestCount,
    error_count: errorCount,
    failure_rate: requestCount > 0 ? errorCount / requestCount : 0,
    avg_latency: requestCount > 0 ? totalLatency / requestCount : 0,
    avg_pipeline_latency: requestCount > 0 ? totalPipelineLatency / requestCount : 0,
    original_count: originalCount,
    retry_count: retryCount,
    retry_amplification:
      originalCount > 0 ? retryCount / originalCount : 0
  };
}

module.exports = {
  updateMetrics,
  getMetrics,
  addToSet,
  isDuplicate,      // 🔴 ADD BACK
  markProcessed     // 🔴 ADD BACK
};