const { redisClient } = require('../config/redisClient');
const { incrementDLQ } = require('./counters');

// ----------------------
// 🔥 SAFE SERIALIZER (STRICT)
// ----------------------
function safeSerialize(value) {
  if (value === null || value === undefined) return "";

  if (typeof value === "string") return value;

  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }

  // 🔥 FORCE stringify everything else
  try {
    return JSON.stringify(value);
  } catch {
    return "[UNSERIALIZABLE]";
  }
}

// ----------------------
// 🔥 ADD TO DLQ (FINAL SAFE VERSION)
// ----------------------
async function addToDLQ(log, meta = {}) {
  if (!redisClient || !redisClient.isOpen) {
    throw new Error("Redis not connected");
  }

  if (!log || typeof log.id !== "string") {
    console.error("❌ INVALID DLQ LOG:", log);
    return;
  }

  const key = `dlq:data:${log.id}`;
  const record = {};

  // ----------------------
  // 🔥 FLATTEN LOG SAFELY
  // ----------------------
  for (const [k, v] of Object.entries(log)) {
    record[k] = safeSerialize(v);
  }

  // ----------------------
  // 🔥 META
  // ----------------------
  record.reason = safeSerialize(meta.reason || "UNKNOWN");
  record.source = safeSerialize(meta.source || "DLQ_CONSUMER");
  record.failed_at = String(Date.now());

  // ----------------------
  // 🔥 REDIS WRITE (SAFE MULTI)
  // ----------------------
  const multi = redisClient.multi();

  multi.hSet(key, record); // ✅ CORRECT
  multi.expire(key, 86400);

  multi.sAdd("dlq:index", String(log.id));

  if (log.service) {
    multi.sAdd(`dlq:service:${String(log.service)}`, String(log.id));
  }

  if (meta.reason) {
    multi.sAdd(`dlq:reason:${String(meta.reason)}`, String(log.id));
  }

  await multi.exec();

  await incrementDLQ().catch(() => {});
}

// ----------------------
// GET ENTRY
// ----------------------
async function getDLQEntry(id) {
  if (!redisClient || !redisClient.isOpen) return null;

  const data = await redisClient.hGetAll(`dlq:data:${id}`);
  return Object.keys(data).length ? data : null;
}

// ----------------------
// LIST DLQ
// ----------------------
async function listDLQ(cursor = 0, count = 100) {
  if (!redisClient || !redisClient.isOpen) {
    return { items: [], cursor: 0 };
  }

  const result = await redisClient.sScan("dlq:index", cursor, { COUNT: count });

  const items = [];
  for (const id of result.members) {
    const entry = await getDLQEntry(id);
    if (entry) items.push(entry);
  }

  return { items, cursor: result.cursor };
}

// ----------------------
async function getDLQCount() {
  if (!redisClient || !redisClient.isOpen) return 0;
  return await redisClient.sCard("dlq:index");
}

// ----------------------
module.exports = {
  addToDLQ,
  getDLQEntry,
  listDLQ,
  getDLQCount,
};