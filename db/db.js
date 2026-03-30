const mysql = require('mysql2/promise');

const pool = mysql.createPool({
  host: 'localhost',
  user: 'root',
  password: 'your_password',   // change this
  database: 'log_system',

  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

// 🔴 SINGLE INSERT (fallback)
async function insertMetric(record) {
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

  await pool.execute(query, values);
}

// 🔴 BULK INSERT (PRIMARY PATH)
async function bulkInsertMetrics(records) {
  if (!records.length) return;

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

  const values = records.map(r => [
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

  await pool.query(query, [values]);
}

module.exports = {
  insertMetric,
  bulkInsertMetrics
};