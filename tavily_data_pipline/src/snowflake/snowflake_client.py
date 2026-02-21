"""
Snowflake client for querying agent metadata.

Provides connection management and query execution utilities
for the Snowflake data warehouse.
"""

import os
from typing import Dict, Any, List, Optional
import snowflake.connector
from snowflake.connector import DictCursor


class SnowflakeClient:
    """
    Snowflake client for agent metadata (3-table model: agent_runs, run_steps, api_calls).
    """
    
    def __init__(
        self,
        account: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None
    ):
        """
        Initialize Snowflake client.
        
        Args:
            account: Snowflake account identifier. Reads from SNOWFLAKE_ACCOUNT env if None.
            user: Snowflake user. Reads from SNOWFLAKE_USER env if None.
            password: Snowflake password. Reads from SNOWFLAKE_PASSWORD env if None.
            warehouse: Warehouse name. Reads from SNOWFLAKE_WAREHOUSE env if None.
            database: Database name. Reads from SNOWFLAKE_DATABASE env if None.
            schema: Schema name. Reads from SNOWFLAKE_SCHEMA env if None.
        """
        self.account = account or os.getenv("SNOWFLAKE_ACCOUNT")
        self.user = user or os.getenv("SNOWFLAKE_USER")
        self.password = password or os.getenv("SNOWFLAKE_PASSWORD")
        self.warehouse = warehouse or os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
        self.database = database or os.getenv("SNOWFLAKE_DATABASE", "AGENT_METADATA_DB")
        self.schema = schema or os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")

        if not all([self.account, self.user, self.password]):
            raise ValueError(
                "SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD must be set"
            )
        
        self.conn = None
    
    def connect(self):
        """Establish connection to Snowflake."""
        self.conn = snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema
        )
    
    def execute(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """
        Execute a query and return results as list of dicts.
        
        Args:
            query: SQL query string
            params: Optional query parameters
            
        Returns:
            List of row dictionaries
        """
        if self.conn is None:
            self.connect()
        
        cursor = self.conn.cursor(DictCursor)
        try:
            cursor.execute(query, params or ())
            return cursor.fetchall()
        finally:
            cursor.close()
    
    def execute_ddl(self, ddl: str) -> None:
        """
        Execute DDL statement (CREATE, ALTER, etc.).
        
        Args:
            ddl: DDL SQL statement
        """
        if self.conn is None:
            self.connect()
        
        cursor = self.conn.cursor()
        try:
            cursor.execute(ddl)
        finally:
            cursor.close()

    def get_agent_runs(
        self,
        limit: int = 500,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get agent_runs with optional date filter on started_at.
        Uses connection's database/schema (from SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA).
        """
        query = """
            SELECT run_id, company_name, industry, status, started_at, completed_at,
                   total_latency_ms, total_api_calls, error_message, ingested_at
            FROM agent_runs
            WHERE 1=1
        """
        params: List[Any] = []
        if date_from:
            query += " AND CAST(started_at AS DATE) >= %s"
            params.append(date_from)
        if date_to:
            query += " AND CAST(started_at AS DATE) <= %s"
            params.append(date_to)
        query += " ORDER BY started_at DESC LIMIT %s"
        params.append(limit)
        return self.execute(query, tuple(params))

    def get_run_steps(self, limit: int = 5000, run_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get run_steps, optionally filtered by run_id list."""
        if run_ids:
            placeholders = ",".join(["%s"] * len(run_ids))
            query = f"""
                SELECT step_id, run_id, step_name, status, latency_ms, error_message, ingested_at
                FROM run_steps
                WHERE run_id IN ({placeholders})
                ORDER BY ingested_at DESC
                LIMIT %s
            """
            return self.execute(query, (*run_ids, limit))
        query = """
            SELECT step_id, run_id, step_name, status, latency_ms, error_message, ingested_at
            FROM run_steps
            ORDER BY ingested_at DESC
            LIMIT %s
        """
        return self.execute(query, (limit,))

    def get_api_calls(self, limit: int = 5000, run_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get api_calls, optionally filtered by run_id list."""
        if run_ids:
            placeholders = ",".join(["%s"] * len(run_ids))
            query = f"""
                SELECT call_id, run_id, query_used, results_returned, latency_ms, called_at, ingested_at
                FROM api_calls
                WHERE run_id IN ({placeholders})
                ORDER BY called_at DESC
                LIMIT %s
            """
            return self.execute(query, (*run_ids, limit))
        query = """
            SELECT call_id, run_id, query_used, results_returned, latency_ms, called_at, ingested_at
            FROM api_calls
            ORDER BY called_at DESC
            LIMIT %s
        """
        return self.execute(query, (limit,))

    def close(self):
        """Close Snowflake connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
