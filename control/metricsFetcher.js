const { client } = require('../config/redisClient');

async function fetchLatestMetrics(service) {
  const windows = await client.sMembers(`metrics:windows:${service}`);
  if (!windows.length) return null;

  const latestWindow = Math.max(...windows.map(Number));
  const key = `metrics:${service}:${latestWindow}`;

  const data = await client.hGetAll(key);
  if (!data || Object.keys(data).length === 0) return null;

  const total = +data.total_attempts || 1;
  const pipeline = +data.total_pipeline_latency || 0;
  const original = +data.original_messages || 0;
  const retry = +data.retry_attempts || 0;

  return {
    avg_pipeline_latency: pipeline / total,
    retry_amplification: original ? retry / original : 0
  };
}

module.exports = { fetchLatestMetrics };