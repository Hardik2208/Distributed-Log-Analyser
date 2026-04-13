const { bulkInsertMetrics } = require('./db');

// ----------------------
const queue = [];

// 🔥 CONFIG
const MAX_BATCH_SIZE = 50;
const FLUSH_INTERVAL = 100;
const DB_WORKERS = 3;
const MAX_QUEUE_SIZE = 5000;
const MAX_RETRIES = 3;

// 🔥 AGGREGATION STORE
const windowStore = new Map();

// ----------------------
function getKey(service, window) {
  return `${service}:${window}`;
}

// ----------------------
function initWindow(record) {
  return {
    service: record.service,
    window_start: record.window,

    total_attempts: 0,
    success_count: 0,
    failure_count: 0,
    temporary_failure_count: 0,

    original_count: 0,
    retry_count: 0,

    retry_depth_sum: 0,
    max_retry_depth: 0,

    latency_sum: 0,
    pipeline_latency_sum: 0,
    end_to_end_latency_sum: 0,
    ingestion_latency_sum: 0,
  };
}

// ----------------------
function pushToDBQueue(record) {

  const key = getKey(record.service, record.window);

  if (!windowStore.has(key)) {
    windowStore.set(key, initWindow(record));
  }

  const agg = windowStore.get(key);

  // ----------------------
  // 🔥 AGGREGATION
  // ----------------------
  agg.total_attempts++;

  if (record.success) agg.success_count++;
  if (record.failed) agg.failure_count++;
  if (record.isTemporaryFailure) agg.temporary_failure_count++;

  if (record.isRetry) agg.retry_count++;
  else agg.original_count++;

  agg.retry_depth_sum += record.retryCount || 0;
  agg.max_retry_depth = Math.max(agg.max_retry_depth, record.retryCount || 0);

  agg.latency_sum += record.latency || 0;
  agg.pipeline_latency_sum += record.pipelineLatency || 0;
  agg.end_to_end_latency_sum += record.endToEndLatency || 0;
  agg.ingestion_latency_sum += record.ingestionLatency || 0;
}

// ----------------------
function buildFinalRecord(agg) {

  const total = agg.total_attempts || 1;

  return {
    service: agg.service,
    window_start: agg.window_start,

    total_attempts: total,
    success_count: agg.success_count,
    failure_count: agg.failure_count,
    temporary_failure_count: agg.temporary_failure_count,

    original_count: agg.original_count,
    retry_count: agg.retry_count,

    retry_amplification:
      agg.original_count === 0 ? 0 :
      total / agg.original_count,

    avg_retry_depth: agg.retry_depth_sum / total,
    max_retry_depth: agg.max_retry_depth,

    avg_latency: agg.latency_sum / total,
    avg_pipeline_latency: agg.pipeline_latency_sum / total,
    avg_end_to_end_latency: agg.end_to_end_latency_sum / total,
    avg_ingestion_latency: agg.ingestion_latency_sum / total
  };
}

// ----------------------
function collectFlushBatch() {
  const batch = [];

  for (const [key, agg] of windowStore.entries()) {
    batch.push(buildFinalRecord(agg));
    windowStore.delete(key);
  }

  return batch;
}

// ----------------------
async function flushDB() {

  if (windowStore.size === 0) return;

  const batch = collectFlushBatch();

  const safeBatch = batch.slice(0, MAX_BATCH_SIZE);

  try {
    await bulkInsertMetrics(safeBatch);

  } catch (err) {
    console.error("❌ DB BULK INSERT FAILED", err.message);

    const retryBatch = [];

    for (const record of safeBatch) {
      if ((record.__retries || 0) < MAX_RETRIES) {
        retryBatch.push({
          ...record,
          __retries: (record.__retries || 0) + 1
        });
      } else {
        console.error("💀 DROPPING RECORD AFTER MAX RETRIES");
      }
    }

    queue.unshift(...retryBatch);
  }
}

// ----------------------
// 🔥 PARALLEL WORKERS
for (let i = 0; i < DB_WORKERS; i++) {
  setInterval(() => {
    flushDB();
  }, FLUSH_INTERVAL);
}

// ----------------------
module.exports = {
  pushToDBQueue,
};