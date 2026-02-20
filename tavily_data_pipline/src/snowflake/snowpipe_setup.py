"""
Snowpipe setup SQL scripts and configuration.

Contains SQL definitions for Snowpipe, external stage, file format,
and target table schema.
"""

# Target table schema
TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS agent_metadata (
    event_id VARCHAR(36) PRIMARY KEY,
    timestamp_utc TIMESTAMP_NTZ,
    query VARCHAR(500),
    query_length INTEGER,
    status VARCHAR(20),
    latency_ms FLOAT,
    response_size_chars INTEGER,
    num_sources INTEGER,
    session_id VARCHAR(36),
    agent_version VARCHAR(50),
    error_message VARCHAR(1000),
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
"""

# File format for JSON
FILE_FORMAT = """
CREATE OR REPLACE FILE FORMAT agent_metadata_json_format
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = FALSE
    DATE_FORMAT = 'AUTO'
    TIMESTAMP_FORMAT = 'AUTO';
"""

# Storage integration and stage (requires AWS role ARN)
# User must create storage integration in Snowflake UI first, or use IAM role
STAGE_TEMPLATE = """
CREATE OR REPLACE STAGE agent_metadata_stage
    URL = 's3://{bucket_name}/{prefix}'
    STORAGE_INTEGRATION = {storage_integration}
    FILE_FORMAT = agent_metadata_json_format;
"""

# Alternative: Use IAM role (no storage integration)
STAGE_WITH_CREDENTIALS = """
CREATE OR REPLACE STAGE agent_metadata_stage
    URL = 's3://{bucket_name}/{prefix}'
    CREDENTIALS = (AWS_KEY_ID = '{aws_key_id}' AWS_SECRET_KEY = '{aws_secret_key}')
    FILE_FORMAT = agent_metadata_json_format;
"""

# Snowpipe
SNOWPIPE_TEMPLATE = """
CREATE OR REPLACE PIPE agent_metadata_pipe
    AUTO_INGEST = TRUE
    AS
    COPY INTO agent_metadata
    FROM @agent_metadata_stage
    FILE_FORMAT = agent_metadata_json_format
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = 'CONTINUE';
"""

# Get Snowpipe ARN for S3 event notification
PIPE_ARN_QUERY = """
SHOW PIPES LIKE 'agent_metadata_pipe';
"""

# Database and schema creation
CREATE_DATABASE = """
CREATE DATABASE IF NOT EXISTS AGENT_METADATA_DB;
"""

CREATE_SCHEMA = """
CREATE SCHEMA IF NOT EXISTS PUBLIC;
"""


def get_schema_sql() -> str:
    """Return full setup SQL."""
    return TABLE_SCHEMA


def get_file_format_sql() -> str:
    """Return file format creation SQL."""
    return FILE_FORMAT


def get_stage_sql(bucket_name: str, prefix: str, storage_integration: str = "s3_integration") -> str:
    """Return stage creation SQL with storage integration."""
    return STAGE_TEMPLATE.format(
        bucket_name=bucket_name,
        prefix=prefix,
        storage_integration=storage_integration
    )


def get_snowpipe_sql() -> str:
    """Return Snowpipe creation SQL."""
    return SNOWPIPE_TEMPLATE
