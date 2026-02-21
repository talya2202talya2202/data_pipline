"""
Metadata Collector for tracking agent execution metrics.

This module provides decorators and utilities to collect execution metadata
including event_id, timestamp, query, latency, status, and other metrics.
"""

import uuid
import time
import functools
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Callable, TypeVar, cast, List

F = TypeVar('F', bound=Callable[..., Any])


class MetadataCollector:
    """
    Collects execution metadata for agent operations.
    
    Tracks metrics such as:
    - event_id: Unique identifier for each execution
    - timestamp_utc: ISO8601 timestamp
    - query: The research query string
    - query_length: Length of the query
    - status: success or failure
    - latency_ms: Execution time in milliseconds
    - response_size_chars: Size of response in characters
    - num_sources: Number of sources returned
    - session_id: Unique session identifier
    - agent_version: Version of the agent
    - error_message: Error message if execution failed
    """
    
    def __init__(self, agent_version: str = "1.0.0", session_id: Optional[str] = None):
        """
        Initialize metadata collector.
        
        Args:
            agent_version: Version identifier for the agent
            session_id: Optional session ID. If None, generates a new UUID.
        """
        self.agent_version = agent_version
        self.session_id = session_id or str(uuid.uuid4())
        self.metadata_history: List[Dict[str, Any]] = []
    
    def generate_event_id(self) -> str:
        """Generate a unique event ID."""
        return str(uuid.uuid4())
    
    def get_current_timestamp(self) -> str:
        """Get current UTC timestamp in ISO8601 format."""
        return datetime.now(timezone.utc).isoformat()
    
    def collect_metadata(
        self,
        query: str,
        status: str,
        latency_ms: float,
        response_size_chars: int = 0,
        num_sources: int = 0,
        error_message: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Collect metadata for an execution.
        
        Args:
            query: The research query string
            status: "success" or "failure"
            latency_ms: Execution time in milliseconds
            response_size_chars: Size of response in characters
            num_sources: Number of sources returned
            error_message: Error message if execution failed
            
        Returns:
            Dictionary containing all metadata fields
        """
        metadata = {
            "event_id": self.generate_event_id(),
            "timestamp_utc": self.get_current_timestamp(),
            "query": query,
            "query_length": len(query),
            "status": status,
            "latency_ms": latency_ms,
            "response_size_chars": response_size_chars,
            "num_sources": num_sources,
            "session_id": self.session_id,
            "agent_version": self.agent_version,
            "error_message": error_message
        }
        
        self.metadata_history.append(metadata)
        return metadata
    
    def collect_from_research_state(
        self,
        query: str,
        state: Dict[str, Any],
        start_time: float,
        end_time: float
    ) -> Dict[str, Any]:
        """
        Collect metadata from a ResearchState result.
        
        Args:
            query: The research query string
            state: ResearchState dictionary from research() method
            start_time: Start time from time.time()
            end_time: End time from time.time()
            
        Returns:
            Dictionary containing all metadata fields
        """
        latency_ms = (end_time - start_time) * 1000
        
        if state.get("error"):
            status = "failure"
            error_message = state.get("error")
        else:
            status = "success"
            error_message = None
        
        sources = state.get("sources", [])
        response_size_chars = sum(
            len(str(source.get("title", ""))) +
            len(str(source.get("content", ""))) +
            len(str(source.get("url", "")))
            for source in sources
        )
        
        num_sources = len(sources)
        
        metadata = self.collect_metadata(
            query=query,
            status=status,
            latency_ms=latency_ms,
            response_size_chars=response_size_chars,
            num_sources=num_sources,
            error_message=error_message
        )

        metadata["company_name"] = state.get("company_name", query)
        metadata["industry"] = state.get("industry")
        metadata["steps"] = state.get("steps", [])
        metadata["api_calls"] = state.get("api_calls", [])
        metadata["started_at_utc"] = datetime.fromtimestamp(
            start_time, tz=timezone.utc
        ).isoformat()
        metadata["completed_at_utc"] = datetime.fromtimestamp(
            end_time, tz=timezone.utc
        ).isoformat()

        return metadata
    
    def get_latest_metadata(self) -> Optional[Dict[str, Any]]:
        """Get the most recent metadata entry."""
        return self.metadata_history[-1] if self.metadata_history else None
    
    def get_metadata_by_session(self) -> List[Dict[str, Any]]:
        """Get all metadata entries for the current session."""
        return [
            metadata for metadata in self.metadata_history
            if metadata.get("session_id") == self.session_id
        ]


def track_execution(collector: MetadataCollector):
    """
    Decorator to track execution metadata for agent methods.
    
    Usage:
        collector = MetadataCollector(agent_version="1.0.0")
        
        @track_execution(collector)
        def research(self, query: str):
            # method implementation
            pass
    
    Args:
        collector: MetadataCollector instance
        
    Returns:
        Decorator function
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Extract query from args or kwargs
            query = kwargs.get("query") or (args[1] if len(args) > 1 else "")
            
            start_time = time.time()
            error_message = None
            
            try:
                result = func(*args, **kwargs)
                end_time = time.time()
                
                # If result is a ResearchState, extract metadata from it
                if isinstance(result, dict) and "sources" in result:
                    metadata = collector.collect_from_research_state(
                        query=query,
                        state=result,
                        start_time=start_time,
                        end_time=end_time
                    )
                else:
                    # Generic metadata collection
                    latency_ms = (end_time - start_time) * 1000
                    response_size_chars = len(str(result)) if result else 0
                    
                    metadata = collector.collect_metadata(
                        query=query,
                        status="success",
                        latency_ms=latency_ms,
                        response_size_chars=response_size_chars
                    )
                
                return result
                
            except Exception as e:
                end_time = time.time()
                error_message = str(e)
                latency_ms = (end_time - start_time) * 1000
                
                collector.collect_metadata(
                    query=query,
                    status="failure",
                    latency_ms=latency_ms,
                    error_message=error_message
                )
                
                raise
        
        return cast(F, wrapper)
    return decorator
