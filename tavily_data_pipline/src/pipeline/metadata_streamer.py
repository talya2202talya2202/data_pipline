"""
Metadata Streamer - polls MongoDB for metadata and streams to AWS Firehose.

Transforms MongoDB documents to Firehose record format (JSON) and sends
in batches. Supports one-shot streaming of recent documents or continuous polling.
"""

import os
import time
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from src.database.mongodb_client import MongoDBClient
from src.pipeline.firehose_client import FirehoseClient


class MetadataStreamer:
    """
    Streams agent metadata from MongoDB to AWS Kinesis Firehose.
    
    Can run in two modes:
    1. One-shot: stream recent N documents
    2. Polling: stream documents since last run (tracks timestamp)
    """
    
    def __init__(
        self,
        mongo_client: Optional[MongoDBClient] = None,
        firehose_client: Optional[FirehoseClient] = None,
        batch_size: int = 25
    ):
        """
        Initialize metadata streamer.
        
        Args:
            mongo_client: MongoDB client. Creates one if None.
            firehose_client: Firehose client. Creates one if None (requires env vars).
            batch_size: Max records per Firehose batch (Firehose limit 500, use 25 for safety)
        """
        self.mongo_client = mongo_client
        self.firehose_client = firehose_client
        self.batch_size = batch_size
        
        if self.mongo_client is None:
            try:
                self.mongo_client = MongoDBClient()
            except ValueError:
                self.mongo_client = None
        
        if self.firehose_client is None:
            try:
                self.firehose_client = FirehoseClient()
            except ValueError:
                self.firehose_client = None
    
    def _prepare_record(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare MongoDB document for Firehose (ensure serializable)."""
        record = dict(doc)
        # Ensure timestamp is string for JSON
        if isinstance(record.get("timestamp_utc"), datetime):
            record["timestamp_utc"] = record["timestamp_utc"].isoformat()
        return record
    
    def stream_recent(self, limit: int = 100) -> int:
        """
        Stream the N most recent metadata documents from MongoDB to Firehose.
        
        Args:
            limit: Maximum number of documents to stream
            
        Returns:
            Number of records successfully sent to Firehose
        """
        if not self.mongo_client or not self.firehose_client:
            return 0
        
        docs = self.mongo_client.get_recent_metadata(limit=limit)
        if not docs:
            return 0
        
        records = [self._prepare_record(d) for d in docs]
        return self.firehose_client.send_batch(records)
    
    def stream_since(self, since: datetime) -> int:
        """
        Stream metadata documents since the given timestamp.
        
        Args:
            since: Start datetime (exclusive)
            
        Returns:
            Number of records successfully sent
        """
        if not self.mongo_client or not self.firehose_client:
            return 0
        
        end = datetime.utcnow()
        docs = self.mongo_client.get_metadata_by_date_range(since, end, limit=1000)
        if not docs:
            return 0
        
        records = [self._prepare_record(d) for d in docs]
        return self.firehose_client.send_batch(records)
    
    def stream_metadata(self, metadata: Dict[str, Any]) -> bool:
        """
        Stream a single metadata record to Firehose.
        
        Use this when you have metadata from run_agent (e.g. right after saving to MongoDB).
        
        Args:
            metadata: Single metadata dict from MetadataCollector
            
        Returns:
            True if sent successfully
        """
        if not self.firehose_client:
            return False
        
        record = self._prepare_record(metadata)
        return self.firehose_client.send_metadata(record)
