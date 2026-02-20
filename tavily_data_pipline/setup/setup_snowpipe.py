#!/usr/bin/env python3
"""
Snowflake Snowpipe setup script.

Creates database, schema, agent_metadata table, file format, external stage, and Snowpipe.
Requires: Snowflake credentials in env. S3 bucket from Firehose setup.
Create storage integration in Snowflake UI first; then configure S3 event for Snowpipe.
"""

import os
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

try:
    from dotenv import load_dotenv
    load_dotenv(project_root / ".env")
except ImportError:
    pass

from src.snowflake.snowflake_client import SnowflakeClient
from src.snowflake.snowpipe_setup import (
    get_schema_sql,
    get_file_format_sql,
    get_stage_sql,
    get_snowpipe_sql,
    CREATE_DATABASE,
    CREATE_SCHEMA,
)


def main():
    bucket_name = os.getenv("S3_BUCKET_NAME") or "agent-metadata-firehose"
    prefix = "agent_metadata/"
    storage_integration = os.getenv("SNOWFLAKE_STORAGE_INTEGRATION") or "S3_AGENT_METADATA"
    print("Setting up Snowflake Snowpipe...")
    print(f"  S3 Bucket: {bucket_name}\n  Prefix: {prefix}")
    try:
        client = SnowflakeClient()
        client.connect()
        print("Creating database...")
        client.execute_ddl(CREATE_DATABASE)
        print("Creating schema...")
        client.execute_ddl(CREATE_SCHEMA)
        print("Creating agent_metadata table...")
        client.execute_ddl(get_schema_sql())
        print("Creating file format...")
        client.execute_ddl(get_file_format_sql())
        print("Creating external stage...")
        try:
            client.execute_ddl(get_stage_sql(bucket_name, prefix, storage_integration))
        except Exception as e:
            print(f"  Warning: Create storage integration in Snowflake UI first. Error: {e}")
        print("Creating Snowpipe...")
        try:
            client.execute_ddl(get_snowpipe_sql())
        except Exception as e:
            print(f"  Warning: Snowpipe creation failed: {e}")
        client.close()
        print("\nSetup complete. Configure S3 event notification to trigger Snowpipe (see SHOW PIPES for ARN).")
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
