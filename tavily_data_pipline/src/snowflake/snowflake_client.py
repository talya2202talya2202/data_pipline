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
    Snowflake client for agent metadata operations.
    
    Provides methods to execute queries and fetch metadata
    from the agent_metadata table.
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
    
    def get_recent_metadata(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get recent metadata entries from agent_metadata table.
        
        Args:
            limit: Maximum number of rows to return
            
        Returns:
            List of metadata documents
        """
        query = """
            SELECT event_id, timestamp_utc, query, query_length, status,
                   latency_ms, response_size_chars, num_sources, session_id,
                   agent_version, error_message, ingested_at
            FROM agent_metadata
            ORDER BY timestamp_utc DESC
            LIMIT %s
        """
        return self.execute(query, (limit,))
    
    def get_metadata_by_session(self, session_id: str, limit: int = 1000) -> List[Dict[str, Any]]:
        """
        Get metadata entries for a specific session.
        
        Args:
            session_id: Session identifier
            limit: Maximum number of rows
            
        Returns:
            List of metadata documents
        """
        query = """
            SELECT event_id, timestamp_utc, query, query_length, status,
                   latency_ms, response_size_chars, num_sources, session_id,
                   agent_version, error_message, ingested_at
            FROM agent_metadata
            WHERE session_id = %s
            ORDER BY timestamp_utc ASC
            LIMIT %s
        """
        return self.execute(query, (session_id, limit))
    
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
