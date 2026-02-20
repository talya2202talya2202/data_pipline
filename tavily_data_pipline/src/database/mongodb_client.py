"""
MongoDB client for storing and querying agent metadata.

This module provides a MongoDB client wrapper for saving and retrieving
agent execution metadata.
"""

import os
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import ConnectionFailure, OperationFailure


class MongoDBClient:
    """
    MongoDB client for agent metadata operations.
    
    Provides methods to:
    - Save metadata documents
    - Query recent metadata
    - Query metadata by session ID
    - Query metadata by date range
    """
    
    def __init__(
        self,
        connection_uri: Optional[str] = None,
        database_name: str = "agent_metadata_db",
        collection_name: str = "agent_metadata"
    ):
        """
        Initialize MongoDB client.
        
        Args:
            connection_uri: MongoDB connection URI. If None, reads from MONGODB_URI env var.
            database_name: Name of the database to use
            collection_name: Name of the collection to use
        """
        self.connection_uri = connection_uri or os.getenv("MONGODB_URI")
        if not self.connection_uri:
            raise ValueError("MONGODB_URI must be provided or set as environment variable")
        
        self.database_name = database_name
        self.collection_name = collection_name
        
        # Initialize connection
        try:
            self.client = MongoClient(self.connection_uri)
            # Test connection
            self.client.admin.command('ping')
        except ConnectionFailure as e:
            raise ConnectionError(f"Failed to connect to MongoDB: {str(e)}") from e
        
        # Get database and collection
        self.database: Database = self.client[self.database_name]
        self.collection: Collection = self.database[self.collection_name]
    
    def save_metadata(self, metadata: Dict[str, Any]) -> str:
        """
        Save metadata document to MongoDB.
        
        Args:
            metadata: Dictionary containing metadata fields:
                - event_id: Unique event identifier
                - timestamp_utc: ISO8601 timestamp
                - query: Research query string
                - query_length: Length of query
                - status: "success" or "failure"
                - latency_ms: Execution latency in milliseconds
                - response_size_chars: Response size in characters
                - num_sources: Number of sources
                - session_id: Session identifier
                - agent_version: Agent version
                - error_message: Error message if any
                
        Returns:
            Inserted document ID as string
        """
        try:
            # Ensure timestamp_utc is stored as datetime if it's a string
            if isinstance(metadata.get("timestamp_utc"), str):
                metadata["timestamp_utc"] = datetime.fromisoformat(
                    metadata["timestamp_utc"].replace("Z", "+00:00")
                )
            
            # Insert document
            result = self.collection.insert_one(metadata)
            return str(result.inserted_id)
        except OperationFailure as e:
            raise RuntimeError(f"Failed to save metadata to MongoDB: {str(e)}") from e
    
    def get_recent_metadata(
        self,
        limit: int = 100,
        hours: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get recent metadata entries.
        
        Args:
            limit: Maximum number of documents to return
            hours: Optional number of hours to look back. If None, returns most recent entries.
            
        Returns:
            List of metadata documents, sorted by timestamp_utc descending
        """
        query = {}
        
        # Add time filter if specified
        if hours:
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)
            query["timestamp_utc"] = {"$gte": cutoff_time}
        
        try:
            cursor = self.collection.find(query).sort("timestamp_utc", -1).limit(limit)
            results = list(cursor)
            
            # Convert ObjectId to string and datetime to ISO string
            for doc in results:
                doc["_id"] = str(doc["_id"])
                if isinstance(doc.get("timestamp_utc"), datetime):
                    doc["timestamp_utc"] = doc["timestamp_utc"].isoformat()
            
            return results
        except OperationFailure as e:
            raise RuntimeError(f"Failed to query recent metadata: {str(e)}") from e
    
    def get_metadata_by_session(
        self,
        session_id: str,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Get all metadata entries for a specific session.
        
        Args:
            session_id: Session identifier to filter by
            limit: Maximum number of documents to return
            
        Returns:
            List of metadata documents for the session, sorted by timestamp_utc ascending
        """
        query = {"session_id": session_id}
        
        try:
            cursor = self.collection.find(query).sort("timestamp_utc", 1).limit(limit)
            results = list(cursor)
            
            # Convert ObjectId to string and datetime to ISO string
            for doc in results:
                doc["_id"] = str(doc["_id"])
                if isinstance(doc.get("timestamp_utc"), datetime):
                    doc["timestamp_utc"] = doc["timestamp_utc"].isoformat()
            
            return results
        except OperationFailure as e:
            raise RuntimeError(f"Failed to query metadata by session: {str(e)}") from e
    
    def get_metadata_by_date_range(
        self,
        start_date: datetime,
        end_date: datetime,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Get metadata entries within a date range.
        
        Args:
            start_date: Start datetime (inclusive)
            end_date: End datetime (inclusive)
            limit: Maximum number of documents to return
            
        Returns:
            List of metadata documents in the date range, sorted by timestamp_utc ascending
        """
        query = {
            "timestamp_utc": {
                "$gte": start_date,
                "$lte": end_date
            }
        }
        
        try:
            cursor = self.collection.find(query).sort("timestamp_utc", 1).limit(limit)
            results = list(cursor)
            
            # Convert ObjectId to string and datetime to ISO string
            for doc in results:
                doc["_id"] = str(doc["_id"])
                if isinstance(doc.get("timestamp_utc"), datetime):
                    doc["timestamp_utc"] = doc["timestamp_utc"].isoformat()
            
            return results
        except OperationFailure as e:
            raise RuntimeError(f"Failed to query metadata by date range: {str(e)}") from e
    
    def get_metadata_by_status(
        self,
        status: str,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Get metadata entries filtered by status (success or failure).
        
        Args:
            status: "success" or "failure"
            limit: Maximum number of documents to return
            
        Returns:
            List of metadata documents with the specified status
        """
        query = {"status": status}
        
        try:
            cursor = self.collection.find(query).sort("timestamp_utc", -1).limit(limit)
            results = list(cursor)
            
            # Convert ObjectId to string and datetime to ISO string
            for doc in results:
                doc["_id"] = str(doc["_id"])
                if isinstance(doc.get("timestamp_utc"), datetime):
                    doc["timestamp_utc"] = doc["timestamp_utc"].isoformat()
            
            return results
        except OperationFailure as e:
            raise RuntimeError(f"Failed to query metadata by status: {str(e)}") from e
    
    def count_documents(self, query: Optional[Dict[str, Any]] = None) -> int:
        """
        Count documents matching the query.
        
        Args:
            query: Optional MongoDB query filter. If None, counts all documents.
            
        Returns:
            Number of matching documents
        """
        if query is None:
            query = {}
        
        try:
            return self.collection.count_documents(query)
        except OperationFailure as e:
            raise RuntimeError(f"Failed to count documents: {str(e)}") from e
    
    def close(self):
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
