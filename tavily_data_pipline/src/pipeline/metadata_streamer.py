"""
Metadata Streamer - polls MongoDB for metadata and streams to AWS Firehose.

Sends records in the shapes expected by Snowflake: agent_run, run_step, api_call
so that the pipe -> raw -> procedure can insert into agent_runs, run_steps, api_calls.
"""

import os
import time
import uuid
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from src.database.mongodb_client import MongoDBClient
from src.pipeline.firehose_client import FirehoseClient


def _ensure_ts(ts: Any) -> str:
    """Return ISO string for timestamp (datetime or string)."""
    if isinstance(ts, datetime):
        return ts.isoformat()
    if ts is None:
        return datetime.utcnow().isoformat()
    return str(ts)


def _flat_to_agent_run(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert flat metadata to agent_run shape (record_type, run_id, company_name, ...).
    """
    ts = _ensure_ts(doc.get("timestamp_utc"))
    return {
        "record_type": "agent_run",
        "run_id": doc.get("event_id"),
        "company_name": doc.get("query"),
        "industry": None,
        "status": doc.get("status"),
        "started_at": ts,
        "completed_at": ts,
        "total_latency_ms": doc.get("latency_ms"),
        "total_api_calls": doc.get("num_sources", 0),
        "error_message": doc.get("error_message"),
    }


def _flat_to_run_step(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert flat metadata to one run_step record (step_id, run_id, step_name, status, ...).
    One synthetic step per run: the main "research" step.
    """
    return {
        "record_type": "run_step",
        "step_id": str(uuid.uuid4()),
        "run_id": doc.get("event_id"),
        "step_name": "research",
        "status": doc.get("status"),
        "latency_ms": doc.get("latency_ms"),
        "error_message": doc.get("error_message"),
    }


def _flat_to_api_call(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert flat metadata to one api_call record (call_id, run_id, query_used, ...).
    One synthetic API call per run: the Tavily research call.
    """
    ts = _ensure_ts(doc.get("timestamp_utc"))
    return {
        "record_type": "api_call",
        "call_id": str(uuid.uuid4()),
        "run_id": doc.get("event_id"),
        "query_used": doc.get("query"),
        "results_returned": doc.get("num_sources", 0),
        "latency_ms": doc.get("latency_ms"),
        "called_at": ts,
    }


def _flat_to_three_records(doc: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Build agent_run, run_step, and api_call records from one flat metadata doc."""
    return [
        _flat_to_agent_run(doc),
        _flat_to_run_step(doc),
        _flat_to_api_call(doc),
    ]


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
        """Prepare record for Firehose (no _id, datetimes as ISO strings)."""
        record = {k: v for k, v in doc.items() if k != "_id"}
        for key, value in list(record.items()):
            if isinstance(value, datetime):
                record[key] = value.isoformat()
        return record

    def _to_firehose_record(self, flat_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Convert flat metadata to Snowflake agent_run shape and prepare for JSON."""
        agent_run = _flat_to_agent_run(flat_metadata)
        return self._prepare_record(agent_run)

    def _to_firehose_records_three(self, flat_metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Convert flat metadata to [agent_run, run_step, api_call] and prepare for JSON."""
        return [self._prepare_record(r) for r in _flat_to_three_records(flat_metadata)]

    def stream_recent(self, limit: int = 100) -> int:
        """
        Stream the N most recent metadata documents from MongoDB to Firehose.
        Each doc is sent as three records: agent_run, run_step, api_call.
        
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
        
        records = [r for d in docs for r in self._to_firehose_records_three(d)]
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

        records = [r for d in docs for r in self._to_firehose_records_three(d)]
        return self.firehose_client.send_batch(records)

    def stream_metadata(self, metadata: Dict[str, Any]) -> bool:
        """
        Stream one run to Firehose as three records: agent_run, run_step, api_call.

        Use this when you have metadata from run_agent (e.g. right after saving to MongoDB).
        Snowflake pipe -> raw -> procedure will insert into agent_runs, run_steps, api_calls.

        Args:
            metadata: Single metadata dict from MetadataCollector (flat: event_id, query, ...)

        Returns:
            True if all three records sent successfully
        """
        if not self.firehose_client:
            return False

        records = self._to_firehose_records_three(metadata)
        sent = self.firehose_client.send_batch(records)
        return sent == len(records)
