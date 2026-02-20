# Tavily Data Pipeline Architecture

## Data Flow

```mermaid
flowchart LR
    subgraph agent [Agent Layer]
        A[Toy Agent]
        B[Metadata Collector]
    end
    
    subgraph storage [Storage]
        C[MongoDB]
        D[S3]
        E[Snowflake]
    end
    
    subgraph pipeline [Pipeline]
        F[Firehose]
        G[Snowpipe]
    end
    
    subgraph viz [Visualization]
        H[Streamlit Dashboard]
    end
    
    A --> B
    B --> C
    C --> F
    F --> D
    D --> G
    G --> E
    C --> H
    E --> H
```

## Component Interactions

### 1. Toy Agent
- **File**: `src/agent/toy_agent.py`
- **Role**: Executes company research via Tavily API
- **Output**: `ResearchState` dict with query, sources, status

### 2. Metadata Collector
- **File**: `src/agent/metadata_collector.py`
- **Role**: Captures execution metrics (event_id, timestamp, query, latency, status, num_sources, etc.)
- **Output**: Metadata dict for persistence

### 3. MongoDB
- **File**: `src/database/mongodb_client.py`
- **Collection**: `agent_metadata`
- **Role**: Primary storage for metadata; source for dashboard and Firehose streaming

### 4. Firehose Client
- **File**: `src/pipeline/firehose_client.py`
- **Role**: Streams metadata records to Kinesis Firehose delivery stream
- **Buffer**: 5MB or 60 seconds per Firehose config

### 5. Metadata Streamer
- **File**: `src/pipeline/metadata_streamer.py`
- **Role**: Polls MongoDB; transforms and sends to Firehose
- **Modes**: One-shot (recent N), or stream single record from run_agent

### 6. Snowpipe
- **Files**: `src/snowflake/snowpipe_setup.py`, `setup/setup_snowpipe.py`
- **Role**: Auto-ingests JSON from S3 into Snowflake `agent_metadata` table
- **Trigger**: S3 event notification

### 7. Streamlit Dashboard
- **File**: `src/dashboard/app.py`
- **Data source**: MongoDB (default)
- **Visualizations**: Execution timeline, query performance, source distribution, status overview

## Data Schema

### MongoDB Document

```json
{
  "event_id": "uuid",
  "timestamp_utc": "ISO8601",
  "query": "string",
  "query_length": 6,
  "status": "success|failure",
  "latency_ms": 1234.5,
  "response_size_chars": 5000,
  "num_sources": 5,
  "session_id": "uuid",
  "agent_version": "1.0.0",
  "error_message": null
}
```

### Snowflake Table

```sql
CREATE TABLE agent_metadata (
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
```

## Error Handling

| Component | Strategy |
|-----------|----------|
| MongoDB | Connection retry; `save_metadata` raises on failure |
| Firehose | Exponential backoff (3 retries); partial success allowed |
| Metadata Streamer | Graceful skip if Firehose not configured |
| run_agent | MongoDB and Firehose failures logged; agent continues |
| Dashboard | Error message on connection failure; empty state |

## Deployment Considerations

- **AWS free tier**: Firehose limits; monitor usage
- **Snowflake credits**: Snowpipe consumes credits; set alerts
- **MongoDB Atlas**: Free M0 limits; consider pause/resume for dev
- **Rate limiting**: Firehose batch size 25 records; avoid burst
