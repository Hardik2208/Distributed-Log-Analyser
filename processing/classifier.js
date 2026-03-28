const ACTIONS = {
  DLQ: "DLQ",
  RETRY: "RETRY"
};

const ERROR_ACTION_MAP = {
  SCHEMA_INVALID: ACTIONS.DLQ,
  PARSE_ERROR: ACTIONS.DLQ,
  RETRY_EXHAUSTED: ACTIONS.DLQ,

  REDIS_ERROR: ACTIONS.RETRY,
  ES_ERROR: ACTIONS.RETRY,
};

const classifyError = (err) => {
  if (!err || !err.type) {
    return ACTIONS.RETRY; // safe fallback
  }

  return ERROR_ACTION_MAP[err.type] || ACTIONS.RETRY;
};

module.exports = { classifyError, ACTIONS };