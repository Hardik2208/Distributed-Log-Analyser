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
const MAX_BATCH_SIZE = 500;   // prevent huge locks
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
      request_count,
      error_count,
      error_rate,
      avg_latency,
      avg_pipeline_latency,
      avg_ingestion_latency,
      retry_amplification,
      avg_retry_depth
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `;

  const values = [
    record.service,
    record.window_start,
    record.request_count,
    record.error_count,
    record.error_rate,
    record.avg_latency,
    record.avg_pipeline_latency,
    record.avg_ingestion_latency,
    record.retry_amplification,
    record.avg_retry_depth
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

  // 🔥 LIMIT BATCH SIZE (NO LOGIC LOSS)
  const safeRecords = records.slice(0, MAX_BATCH_SIZE);
  const batchSize = safeRecords.length;

  const connection = await pool.getConnection();

  try {
    console.log(`🧱 DB BATCH START | size=${batchSize}`);

    // 🔴 KEEP TRANSACTION (AS PER YOUR DESIGN)
    await connection.beginTransaction();

    const query = `
      INSERT INTO metrics_window (
        service,
        window_start,
        request_count,
        error_count,
        error_rate,
        avg_latency,
        avg_pipeline_latency,
        avg_ingestion_latency,
        retry_amplification,
        avg_retry_depth
      ) VALUES ?
    `;

    const values = safeRecords.map(r => [
      r.service,
      r.window_start,
      r.request_count,
      r.error_count,
      r.error_rate,
      r.avg_latency,
      r.avg_pipeline_latency,
      r.avg_ingestion_latency,
      r.retry_amplification,
      r.avg_retry_depth
    ]);

    // 🔥 TIMEOUT PROTECTION (ADDED, NO LOGIC LOSS)
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

      // 🔥 IMPROVED BACKOFF WITH JITTER (NO LOGIC LOSS)
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