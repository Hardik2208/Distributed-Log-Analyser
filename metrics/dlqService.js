const { redisClient } = require('../config/redisClient');
const { incrementDLQ } = require('./counters');

// ----------------------
// ADD TO DLQ
// ----------------------
async function addToDLQ(log) {
  if (!redisClient || !redisClient.isOpen) {
    throw new Error('Redis not connected');
  }

  const key = `dlq:data:${log.id}`;

  // Flatten all values to strings for hSet compatibility
  const record = {};
  for (const [k, v] of Object.entries(log)) {
    if (v === null || v === undefined) continue;
    record[k] = String(v);
  }

  const multi = redisClient.multi();
  multi.hSet(key, record);
  multi.expire(key, 86400); // TTL: 24h
  multi.sAdd('dlq:index', log.id);

  const res = await multi.exec();

  if (!res) {
    throw new Error('Redis MULTI failed for addToDLQ');
  }

  // Increment global DLQ counter (non-critical)
  await incrementDLQ().catch(() => {});

  return true;
}

// ----------------------
// GET DLQ ENTRY
// ----------------------
async function getDLQEntry(id) {
  if (!redisClient || !redisClient.isOpen) return null;
  const key = `dlq:data:${id}`;
  const data = await redisClient.hGetAll(key);
  return Object.keys(data).length > 0 ? data : null;
}

// ----------------------
// LIST DLQ (paginated via SSCAN)
// ----------------------
async function listDLQ(cursor = 0, count = 100) {
  if (!redisClient || !redisClient.isOpen) return { items: [], cursor: 0 };

  const result = await redisClient.sScan('dlq:index', cursor, { COUNT: count });

  const items = [];
  for (const id of result.members) {
    const entry = await getDLQEntry(id);
    if (entry) items.push(entry);
  }

  return { items, cursor: result.cursor };
}

// ----------------------
// COUNT
// ----------------------
async function getDLQCount() {
  if (!redisClient || !redisClient.isOpen) return 0;
  return await redisClient.sCard('dlq:index');
}

module.exports = {
  addToDLQ,
  getDLQEntry,
  listDLQ,
  getDLQCount,
};