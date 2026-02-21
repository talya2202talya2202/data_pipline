# Architecture

Component internals, data-flow steps, and error handling. For the high-level overview and pipeline diagram see the [README](../README.md).

---

## Data flow (one run, end to end)

1. **`run_agent.py`** calls `CompanyResearcher.research(query)`.
2. The agent executes three steps: **search_overview** (Tavily), **search_competitors** (Tavily), **summarize** (OpenAI). Each step and API call is tracked with timing, status, and results.
3. **`MetadataCollector`** captures a metadata dict containing run-level metrics plus the full `steps` and `api_calls` lists from the agent.
4. The dict is saved to **MongoDB** (`agent_metadata` collection) — the single source of truth.
5. **`MetadataStreamer`** reads the dict, expands it into 1 `agent_run` + N `run_step` + M `api_call` records (each with a `record_type` field), and sends them to **Firehose**.
6. Firehose delivers newline-delimited JSON to **S3**. The `record_type` field enables prefix routing via Firehose dynamic partitioning (`runs/`, `steps/`, `calls/`).
7. **Snowpipe** (`AUTO_INGEST`) picks up new files from each prefix and loads typed rows into `agent_runs`, `run_steps`, and `api_calls`.
8. The **dashboard** queries the three tables from Snowflake.

---

## Components

| Component | File | Key behavior |
|-----------|------|-------------|
| Toy Agent | `src/agent/toy_agent.py` | Three steps: two Tavily `advanced` searches (overview + competitors, up to 5 results each), then an OpenAI `gpt-4o-mini` call that extracts `company_name`, `industry`, and `summary` from the combined sources. Tracks each step and API call in the returned `ResearchState`. |
| Metadata Collector | `src/agent/metadata_collector.py` | Produces a metadata dict per run. Includes run-level fields (`event_id`, `latency_ms`, `status`, etc.) and the agent's `steps`/`api_calls` lists. Also stores real `started_at_utc`/`completed_at_utc` timestamps. |
| MongoDB Client | `src/database/mongodb_client.py` | Insert and query metadata. Converts ISO-8601 strings ↔ `datetime` objects on save/read for proper date indexing. |
| Firehose Client | `src/pipeline/firehose_client.py` | Sends JSON records. Validates AWS credentials eagerly on init (STS call). Retries with exponential backoff (3 attempts). Batches of 25 records. |
| Metadata Streamer | `src/pipeline/metadata_streamer.py` | The transform layer. Reads `steps` and `api_calls` from the metadata doc and produces real records (not synthetic). Backward-compatible: legacy flat docs without these lists get a single synthetic step/call. |
| Snowflake Client | `src/snowflake/snowflake_client.py` | Query layer for the dashboard. Lazy connection. Supports date-range and run-id filters. |
| Dashboard | `src/dashboard/app.py` | Streamlit app. Reads from Snowflake. Four sections (Health, Performance, Usage, Cost) plus a raw-data viewer. |
| Orchestrator | `scripts/run_agent.py` | CLI entrypoint. Runs the full pipeline with flags (`--no-firehose`, `--backfill-firehose`, `--verify-snowflake`). Each stage can fail without stopping the next. |

---

## Error handling

| Component | Strategy |
|-----------|----------|
| Toy Agent | Each step catches its own exceptions and records status/error. If both Tavily searches fail, `research_complete = False`. If only the summarize step fails, the run is still successful (just without enriched fields). |
| MongoDB Client | `ConnectionFailure` → `ConnectionError` on init; `OperationFailure` → `RuntimeError` on save/query. |
| Firehose Client | Credential validation on init (fails fast with actionable error messages); exponential backoff on send (3 retries); partial-success tracking for batch sends. |
| Metadata Streamer | Returns `0` or `False` on failure — does not raise. Calling code (orchestrator) handles the result. |
| Orchestrator | Wraps each stage in its own try/except; logs outcome; continues to the next stage regardless. |
| Dashboard | Displays a Streamlit error and stops rendering if Snowflake is unreachable or returns no data. |
