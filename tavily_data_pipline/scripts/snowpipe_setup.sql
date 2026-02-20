-- Snowpipe setup for Tavily agent pipeline
-- Run this in Snowflake (Worksheets or snowsql) after creating a database and schema.
-- Replace YOUR_BUCKET, YOUR_PREFIX, YOUR_AWS_ACCOUNT, YOUR_IAM_ROLE with your values.
-- Prerequisites: S3 bucket with Firehose delivery; IAM role with read access to the bucket.

-- ---------------------------------------------------------------------------
-- 1. Database and schema (adjust or skip if already created)
-- ---------------------------------------------------------------------------
-- CREATE DATABASE IF NOT EXISTS AGENT_METADATA_DB;
-- CREATE SCHEMA IF NOT EXISTS AGENT_METADATA_DB.PUBLIC;
-- USE SCHEMA AGENT_METADATA_DB.PUBLIC;

-- ---------------------------------------------------------------------------
-- 2. File format for JSON (one JSON object per line, e.g. from Firehose)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT agent_metadata_json_format
  TYPE = JSON
  STRIP_OUTER_ARRAY = FALSE
  DATE_FORMAT = 'AUTO'
  TIMESTAMP_FORMAT = 'AUTO';

-- ---------------------------------------------------------------------------
-- 3. External stage (S3) – replace placeholders with your bucket and path
-- ---------------------------------------------------------------------------
CREATE OR REPLACE STAGE agent_metadata_stage
  URL = 's3://YOUR_BUCKET/YOUR_PREFIX/'
  CREDENTIALS = (AWS_ROLE = 'arn:aws:iam::YOUR_AWS_ACCOUNT:role/YOUR_IAM_ROLE')
  FILE_FORMAT = agent_metadata_json_format;

-- ---------------------------------------------------------------------------
-- 4. Tables (target model: agent_runs, run_steps, api_calls)
-- ---------------------------------------------------------------------------

-- One row per agent run (company research job)
CREATE TABLE IF NOT EXISTS agent_runs (
  run_id             VARCHAR(36) NOT NULL,
  company_name       VARCHAR(500),
  industry           VARCHAR(200),
  status             VARCHAR(20),
  started_at         TIMESTAMP_NTZ,
  completed_at       TIMESTAMP_NTZ,
  total_latency_ms   FLOAT,
  total_api_calls    INTEGER,
  error_message      VARCHAR(1000),
  ingested_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY (run_id)
);

-- One row per step within a run (e.g. search_overview, search_competitors, summarize)
CREATE TABLE IF NOT EXISTS run_steps (
  step_id      VARCHAR(36) NOT NULL,
  run_id       VARCHAR(36) NOT NULL,
  step_name    VARCHAR(100),
  status       VARCHAR(20),
  latency_ms   FLOAT,
  error_message VARCHAR(1000),
  ingested_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY (step_id),
  FOREIGN KEY (run_id) REFERENCES agent_runs(run_id)
);

-- One row per API call (e.g. each Tavily request)
CREATE TABLE IF NOT EXISTS api_calls (
  call_id           VARCHAR(36) NOT NULL,
  run_id            VARCHAR(36) NOT NULL,
  query_used        VARCHAR(1000),
  results_returned  INTEGER,
  latency_ms        FLOAT,
  called_at         TIMESTAMP_NTZ,
  ingested_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY (call_id),
  FOREIGN KEY (run_id) REFERENCES agent_runs(run_id)
);

-- ---------------------------------------------------------------------------
-- 5. Snowpipes – auto-ingest from S3 when new files arrive
-- Firehose writes to S3 with a prefix; you can use one prefix per table
-- or one prefix with mixed JSON and use COPY with $1:field mapping.
-- Example: separate prefixes runs/, steps/, calls/ under YOUR_PREFIX
-- ---------------------------------------------------------------------------

-- Pipe for agent_runs (expects JSON with run_id, company_name, industry, status, started_at, completed_at, total_latency_ms, total_api_calls, error_message)
CREATE OR REPLACE PIPE agent_runs_pipe
  AUTO_INGEST = TRUE
  AS
  COPY INTO agent_runs (run_id, company_name, industry, status, started_at, completed_at, total_latency_ms, total_api_calls, error_message)
  FROM (
    SELECT
      $1:run_id::VARCHAR,
      $1:company_name::VARCHAR,
      $1:industry::VARCHAR,
      $1:status::VARCHAR,
      $1:started_at::TIMESTAMP_NTZ,
      $1:completed_at::TIMESTAMP_NTZ,
      $1:total_latency_ms::FLOAT,
      $1:total_api_calls::INTEGER,
      $1:error_message::VARCHAR
    FROM @agent_metadata_stage/runs/
  )
  FILE_FORMAT = agent_metadata_json_format;

-- Pipe for run_steps
CREATE OR REPLACE PIPE run_steps_pipe
  AUTO_INGEST = TRUE
  AS
  COPY INTO run_steps (step_id, run_id, step_name, status, latency_ms, error_message)
  FROM (
    SELECT
      $1:step_id::VARCHAR,
      $1:run_id::VARCHAR,
      $1:step_name::VARCHAR,
      $1:status::VARCHAR,
      $1:latency_ms::FLOAT,
      $1:error_message::VARCHAR
    FROM @agent_metadata_stage/steps/
  )
  FILE_FORMAT = agent_metadata_json_format;

-- Pipe for api_calls
CREATE OR REPLACE PIPE api_calls_pipe
  AUTO_INGEST = TRUE
  AS
  COPY INTO api_calls (call_id, run_id, query_used, results_returned, latency_ms, called_at)
  FROM (
    SELECT
      $1:call_id::VARCHAR,
      $1:run_id::VARCHAR,
      $1:query_used::VARCHAR,
      $1:results_returned::INTEGER,
      $1:latency_ms::FLOAT,
      $1:called_at::TIMESTAMP_NTZ
    FROM @agent_metadata_stage/calls/
  )
  FILE_FORMAT = agent_metadata_json_format;

-- ---------------------------------------------------------------------------
-- 6. Enable S3 event notifications for Snowpipe
-- After creating the pipes, run: SHOW PIPES; and note the NOTIFICATION_CHANNEL
-- for each pipe. Add that channel as an S3 event notification on your bucket
-- so that when Firehose (or your app) writes new files, Snowpipe is triggered.
-- ---------------------------------------------------------------------------
