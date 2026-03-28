const SERVICE_MAP = {
  auth: ["/login", "/signup"],
  payment: ["/pay", "/refund"],
  order: ["/create", "/status"]
};

const fields = ['id', 'timestamp', 'service', 'endpoint', 'status_code', 'latency_ms'];

const fieldMap = {
  id: 'string',
  timestamp: 'number',
  service: 'string',
  endpoint: 'string',
  status_code: 'number',
  latency_ms: 'number'
};

function validate(log) {

  // 🔴 1. Required fields
  for (const field of fields) {
    if (!(field in log)) {
      throw {
        type: "SCHEMA_INVALID",
        message: `Missing field: ${field}`
      };
    }
  }

  // 🔴 2. Type validation
  for (const [field, type] of Object.entries(fieldMap)) {
    if (typeof log[field] !== type) {
      throw {
        type: "SCHEMA_INVALID",
        message: `Invalid type for ${field}. Expected ${type}, got ${typeof log[field]}`
      };
    }
  }

  // 🔴 3. Empty string checks
  if (!log.id.trim()) {
    throw {
      type: "SCHEMA_INVALID",
      message: "id cannot be empty"
    };
  }

  if (!log.service.trim()) {
    throw {
      type: "SCHEMA_INVALID",
      message: "service cannot be empty"
    };
  }

  // 🔴 4. NaN / finite checks
  if (!Number.isFinite(log.timestamp)) {
    throw {
      type: "SCHEMA_INVALID",
      message: "timestamp must be a valid number"
    };
  }

  if (!Number.isFinite(log.latency_ms)) {
    throw {
      type: "SCHEMA_INVALID",
      message: "latency_ms must be a valid number"
    };
  }

  // 🔴 5. Timestamp sanity
  if (log.timestamp > Date.now() + 1000) {
    throw {
      type: "SCHEMA_INVALID",
      message: "timestamp cannot be in the future"
    };
  }

  // 🔴 6. Status code validation
  if (log.status_code < 100 || log.status_code > 599) {
    throw {
      type: "SCHEMA_INVALID",
      message: "status_code must be between 100 and 599"
    };
  }

  // 🔴 7. Latency validation
  if (log.latency_ms < 0) {
    throw {
      type: "SCHEMA_INVALID",
      message: "latency_ms cannot be negative"
    };
  }

  // 🔴 8. Service validation
  if (!SERVICE_MAP[log.service]) {
    throw {
      type: "SCHEMA_INVALID",
      message: `Invalid service: ${log.service}`
    };
  }

  // 🔴 9. Endpoint validation
  if (!SERVICE_MAP[log.service].includes(log.endpoint)) {
    throw {
      type: "SCHEMA_INVALID",
      message: `Invalid endpoint ${log.endpoint} for service ${log.service}`
    };
  }

  return true;
}

module.exports = { validate };