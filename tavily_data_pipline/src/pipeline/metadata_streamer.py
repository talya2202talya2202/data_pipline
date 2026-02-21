"""
Metadata Streamer - reads metadata from MongoDB and streams to AWS Firehose.

Converts metadata documents into the record shapes expected by Snowflake:
agent_run, run_step, api_call.  Handles both enriched metadata (with real
steps/api_calls lists) and legacy flat metadata (synthetic single step/call).
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


# ------------------------------------------------------------------
# Record builders — enriched metadata (multi-step agent)
# ------------------------------------------------------------------

def _to_agent_run(doc: Dict[str, Any]) -> Dict[str, Any]:
    started = _ensure_ts(doc.get("started_at_utc") or doc.get("timestamp_utc"))
    completed = _ensure_ts(doc.get("completed_at_utc") or doc.get("timestamp_utc"))
    api_calls = doc.get("api_calls", [])
    return {
        "record_type": "agent_run",
        "run_id": doc.get("event_id"),
        "company_name": doc.get("company_name", doc.get("query")),
        "industry": doc.get("industry"),
        "status": doc.get("status"),
        "started_at": started,
        "completed_at": completed,
        "total_latency_ms": doc.get("latency_ms"),
        "total_api_calls": len(api_calls) if api_calls else doc.get("num_sources", 0),
        "error_message": doc.get("error_message"),
    }


def _step_to_run_step(doc: Dict[str, Any], step: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "record_type": "run_step",
        "step_id": str(uuid.uuid4()),
        "run_id": doc.get("event_id"),
        "step_name": step.get("step_name", "research"),
        "status": step.get("status", doc.get("status")),
        "latency_ms": step.get("latency_ms", doc.get("latency_ms")),
        "error_message": step.get("error"),
    }


def _call_to_api_call(doc: Dict[str, Any], call: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "record_type": "api_call",
        "call_id": str(uuid.uuid4()),
        "run_id": doc.get("event_id"),
        "query_used": call.get("query", doc.get("query")),
        "results_returned": call.get("results_returned", doc.get("num_sources", 0)),
        "latency_ms": call.get("latency_ms", doc.get("latency_ms")),
        "called_at": _ensure_ts(call.get("called_at") or doc.get("timestamp_utc")),
    }


# ------------------------------------------------------------------
# Legacy record builders — flat metadata (single-step agent)
# ------------------------------------------------------------------

def _flat_to_run_step(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Synthetic single step for legacy flat metadata."""
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
    """Synthetic single API call for legacy flat metadata."""
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


# ------------------------------------------------------------------
# Orchestrator — picks enriched or legacy path
# ------------------------------------------------------------------

def _metadata_to_records(doc: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Build all Firehose records from a metadata document.

    Handles both enriched metadata (with steps/api_calls lists from the
    multi-step agent) and legacy flat metadata (synthetic single step/call).
    """
    records: List[Dict[str, Any]] = [_to_agent_run(doc)]

    steps = doc.get("steps", [])
    if steps:
        for step in steps:
            records.append(_step_to_run_step(doc, step))
    else:
        records.append(_flat_to_run_step(doc))

    api_calls = doc.get("api_calls", [])
    if api_calls:
        for call in api_calls:
            records.append(_call_to_api_call(doc, call))
    else:
        records.append(_flat_to_api_call(doc))

    return records


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

    def _to_firehose_records(self, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Convert a metadata document into prepared Firehose records."""
        return [self._prepare_record(r) for r in _metadata_to_records(metadata)]

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
        
        records = [r for d in docs for r in self._to_firehose_records(d)]
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

        records = [r for d in docs for r in self._to_firehose_records(d)]
        return self.firehose_client.send_batch(records)

    def stream_metadata(self, metadata: Dict[str, Any]) -> bool:
        """
        Stream one run to Firehose as agent_run + run_step(s) + api_call(s).

        Args:
            metadata: Single metadata dict from MetadataCollector

        Returns:
            True if all records sent successfully
        """
        if not self.firehose_client:
            return False

        records = self._to_firehose_records(metadata)
        sent = self.firehose_client.send_batch(records)
        return sent == len(records)
