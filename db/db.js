const mysql = require('mysql2/promise');

const pool = mysql.createPool({
  host: 'localhost',
  user: 'root',
  password: 'Hardik@123',
  database: 'log_system',

  waitForConnections: true,
  connectionLimit: 30,
  queueLimit: 0
});

// 🔥 CONTROL LIMITS
const MAX_BATCH_SIZE = 500;
const DB_TIMEOUT_MS = 2000;

// ----------------------
// 🔴 SINGLE INSERT (fallback)
// ----------------------
async function insertMetric(record) {
  const start = Date.now();

  const query = `
    INSERT INTO metrics_window (
      service,
      window_start,

      total_attempts,
      success_count,
      failure_count,
      temporary_failure_count,

      original_count,
      retry_count,

      retry_amplification,
      avg_retry_depth,
      max_retry_depth,

      avg_latency,
      avg_pipeline_latency,
      avg_end_to_end_latency,
      avg_ingestion_latency
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `;

  const values = [
    record.service,
    record.window_start,

    record.total_attempts,
    record.success_count,
    record.failure_count,
    record.temporary_failure_count,

    record.original_count,
    record.retry_count,

    record.retry_amplification,
    record.avg_retry_depth,
    record.max_retry_depth,

    record.avg_latency,
    record.avg_pipeline_latency,
    record.avg_end_to_end_latency,
    record.avg_ingestion_latency
  ];

  try {
    await pool.execute(query, values);

    console.log(`🧱 SINGLE INSERT OK | time=${Date.now() - start}ms`);

  } catch (err) {
    console.error(`❌ SINGLE INSERT FAILED | time=${Date.now() - start}ms | err=${err.message}`);
    throw err;
  }
}

// ----------------------
// 🔴 BULK INSERT (PRIMARY PATH)
// ----------------------
async function bulkInsertMetrics(records) {
  if (!records.length) return;

  const start = Date.now();

  const safeRecords = records.slice(0, MAX_BATCH_SIZE);
  const batchSize = safeRecords.length;

  const connection = await pool.getConnection();

  try {
    console.log(`🧱 DB BATCH START | size=${batchSize}`);

    await connection.beginTransaction();

    const query = `
      INSERT INTO metrics_window (
        service,
        window_start,

        total_attempts,
        success_count,
        failure_count,
        temporary_failure_count,

        original_count,
        retry_count,

        retry_amplification,
        avg_retry_depth,
        max_retry_depth,

        avg_latency,
        avg_pipeline_latency,
        avg_end_to_end_latency,
        avg_ingestion_latency
      ) VALUES ?
    `;

    const values = safeRecords.map(r => [
      r.service,
      r.window_start,

      r.total_attempts,
      r.success_count,
      r.failure_count,
      r.temporary_failure_count,

      r.original_count,
      r.retry_count,

      r.retry_amplification,
      r.avg_retry_depth,
      r.max_retry_depth,

      r.avg_latency,
      r.avg_pipeline_latency,
      r.avg_end_to_end_latency,
      r.avg_ingestion_latency
    ]);

    const timeout = new Promise((_, reject) =>
      setTimeout(() => reject(new Error("DB_TIMEOUT")), DB_TIMEOUT_MS)
    );

    await Promise.race([
      connection.query(query, [values]),
      timeout
    ]);

    await connection.commit();

    console.log(
      `✅ DB BATCH SUCCESS | size=${batchSize} | time=${Date.now() - start}ms`
    );

  } catch (err) {
    try {
      await connection.rollback();
    } catch {}

    console.error(
      `❌ DB BATCH FAILED | size=${batchSize} | time=${Date.now() - start}ms | err=${err.message}`
    );

    throw err;

  } finally {
    connection.release();
  }
}

// ----------------------
// 🔁 RETRY WRAPPER
// ----------------------
async function bulkInsertWithRetry(records, retries = 3) {
  const batchSize = records.length;

  for (let i = 0; i < retries; i++) {
    try {
      console.log(`🔁 DB TRY ${i + 1} | size=${batchSize}`);

      await bulkInsertMetrics(records);

      if (i > 0) {
        console.log(`🟢 DB RECOVERED after retry ${i}`);
      }

      return;

    } catch (err) {
      console.error(`❌ DB RETRY ${i + 1} FAILED | err=${err.message}`);

      if (i === retries - 1) {
        console.error(`💀 DB FINAL FAILURE | size=${batchSize}`);
        throw err;
      }

      const delay = (50 * (i + 1)) + Math.random() * 50;
      await new Promise(res => setTimeout(res, delay));
    }
  }
}

module.exports = {
  insertMetric,
  bulkInsertMetrics,
  bulkInsertWithRetry
};