const { redisClient } = require('../config/redisClient');

async function writeToDB(log) {

  const failureFlag = await redisClient.get("db:failure");

  // 🔴 FAIL FAST
  if (failureFlag === "1") {
    const err = new Error("DB_TEMPORARY_FAILURE");
    err.type = "TEMPORARY";
    throw err;
  }

  // ----------------------
  // 🔥 IDEMPOTENT WRITE (SIMULATED DB)
  // ----------------------
  const dbKey = `db:${log.id}`;

  const inserted = await redisClient.set(dbKey, "1", {
    NX: true
  });

  if (!inserted) {
    console.log(`🚫 DUPLICATE WRITE BLOCKED ${log.id}`);
    return false;
  }

  // ----------------------
  // 🔥 CONTROLLED LATENCY (NO SPIKES)
  // ----------------------
  const baseLatency = 10; // minimum cost
  const jitter = Math.random() * 20; // small variation

  await sleep(baseLatency + jitter);

  return true;
}

function sleep(ms) {
  return new Promise(res => setTimeout(res, ms));
}

module.exports = {
  writeToDB,
};