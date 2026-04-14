const { redisClient } = require('../config/redisClient');

async function buildWindowSeries(service, count = 5) {
  const windows = await redisClient.sMembers(`metrics:windows:${service}`);

  if (!windows || windows.length === 0) return [];

  const sorted = windows
    .map(Number)
    .sort((a, b) => b - a)
    .slice(0, count);

  const series = [];

  for (const w of sorted.reverse()) {
    const data = await redisClient.hGetAll(`${service}:${w}`);

    if (!data || !data.total) continue;

    const total = Number(data.total);

    if (total === 0) continue;

    series.push({
      window: w,
      avg_pipeline_latency:
        Number(data.pipelineLatencySum || 0) / total,

      avg_ingestion_latency:
        Number(data.ingestionLatencySum || 0) / total,

      retry_amplification:
        Number(data.original || 0) === 0
          ? 0
          : total / Number(data.original || 0)
    });
  }

  return series;
}

module.exports = { buildWindowSeries };