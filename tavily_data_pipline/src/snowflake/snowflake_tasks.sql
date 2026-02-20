-- Snowflake Scheduled Tasks for Agent Metadata
-- Run these in Snowflake worksheet after agent_metadata table exists.

-- 1. Daily ingestion metrics aggregation
-- Creates a summary table of daily stats
CREATE TABLE IF NOT EXISTS daily_ingestion_metrics (
    metric_date DATE,
    total_executions INTEGER,
    success_count INTEGER,
    failure_count INTEGER,
    avg_latency_ms FLOAT,
    total_sources INTEGER,
    unique_queries INTEGER,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TASK daily_ingestion_metrics_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 1 * * * UTC'  -- Daily at 1 AM UTC
AS
  INSERT INTO daily_ingestion_metrics (
    metric_date,
    total_executions,
    success_count,
    failure_count,
    avg_latency_ms,
    total_sources,
    unique_queries
  )
  SELECT
    DATE(timestamp_utc) AS metric_date,
    COUNT(*) AS total_executions,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success_count,
    SUM(CASE WHEN status = 'failure' THEN 1 ELSE 0 END) AS failure_count,
    AVG(latency_ms) AS avg_latency_ms,
    SUM(num_sources) AS total_sources,
    COUNT(DISTINCT query) AS unique_queries
  FROM AGENT_METADATA_DB.PUBLIC.agent_metadata
  WHERE DATE(timestamp_utc) = DATEADD('day', -1, CURRENT_DATE())
  GROUP BY DATE(timestamp_utc);

-- 2. Session summary aggregation
CREATE TABLE IF NOT EXISTS session_summary (
    session_id VARCHAR(36),
    first_execution TIMESTAMP_NTZ,
    last_execution TIMESTAMP_NTZ,
    execution_count INTEGER,
    success_count INTEGER,
    failure_count INTEGER,
    avg_latency_ms FLOAT,
    unique_queries INTEGER,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TASK session_summary_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 30 * * * * UTC'  -- Every hour at :30
AS
  MERGE INTO session_summary t
  USING (
    SELECT
      session_id,
      MIN(timestamp_utc) AS first_execution,
      MAX(timestamp_utc) AS last_execution,
      COUNT(*) AS execution_count,
      SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success_count,
      SUM(CASE WHEN status = 'failure' THEN 1 ELSE 0 END) AS failure_count,
      AVG(latency_ms) AS avg_latency_ms,
      COUNT(DISTINCT query) AS unique_queries
    FROM AGENT_METADATA_DB.PUBLIC.agent_metadata
    WHERE timestamp_utc >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
    GROUP BY session_id
  ) s
  ON t.session_id = s.session_id
  WHEN MATCHED THEN
    UPDATE SET
      last_execution = s.last_execution,
      execution_count = s.execution_count,
      success_count = s.success_count,
      failure_count = s.failure_count,
      avg_latency_ms = s.avg_latency_ms,
      unique_queries = s.unique_queries
  WHEN NOT MATCHED THEN
    INSERT (session_id, first_execution, last_execution, execution_count, success_count, failure_count, avg_latency_ms, unique_queries)
    VALUES (s.session_id, s.first_execution, s.last_execution, s.execution_count, s.success_count, s.failure_count, s.avg_latency_ms, s.unique_queries);

-- Enable tasks (run after creating)
-- ALTER TASK daily_ingestion_metrics_task RESUME;
-- ALTER TASK session_summary_task RESUME;
