#!/usr/bin/env python3
"""
Main script to run the full Tavily data pipeline (debug-friendly, step by step).

Pipeline steps:
1. Initialize agent + metadata collector
2. Run company research (Tavily)
3. Collect execution metadata
4. Save metadata to MongoDB
5. Stream this record to Firehose (-> S3)
6. Optional: backfill stream recent from MongoDB to Firehose
7. Optional: verify Snowflake has data (if configured)
"""

import argparse
import sys
import time
from pathlib import Path

# Add project root to path so imports work when run as script
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables before importing components
try:
    from dotenv import load_dotenv
    load_dotenv(project_root / ".env")
except ImportError:
    pass  # dotenv optional; use system env vars if not installed

from src.agent.toy_agent import CompanyResearcher
from src.agent.metadata_collector import MetadataCollector
from src.database.mongodb_client import MongoDBClient
from src.pipeline.metadata_streamer import MetadataStreamer


def run_research(
    query: str,
    agent_version: str = "1.0.0",
    max_sources: int = 5,
    stream_to_firehose: bool = True,
    backfill_firehose: bool = False,
    backfill_limit: int = 10,
    verify_snowflake: bool = False,
) -> dict:
    """
    Execute the full pipeline with clear steps for step-by-step debugging.
    """
    result = {
        "state": None,
        "metadata": None,
        "mongo_id": None,
        "mongo_error": None,
        "firehose_sent": False,
        "firehose_error": None,
        "backfill_sent": 0,
        "backfill_error": None,
        "snowflake_count": None,
        "snowflake_error": None,
    }

    # --- Step 1: Initialize components ---
    print("[Step 1] Initialize agent and metadata collector")
    collector = MetadataCollector(agent_version=agent_version)
    researcher = CompanyResearcher(agent_version=agent_version, max_sources=max_sources)

    # --- Step 2: Run research ---
    print(f"[Step 2] Run research for: {query}")
    start_time = time.time()
    state = researcher.research(query)
    end_time = time.time()
    result["state"] = state

    # --- Step 3: Collect metadata ---
    print("[Step 3] Collect execution metadata")
    metadata = collector.collect_from_research_state(
        query=query,
        state=state,
        start_time=start_time,
        end_time=end_time,
    )
    result["metadata"] = metadata

    # --- Step 4: Save to MongoDB ---
    print("[Step 4] Save metadata to MongoDB")
    try:
        mongo_client = MongoDBClient()
        result["mongo_id"] = mongo_client.save_metadata(metadata)
        mongo_client.close()
        print(f"  -> Saved (doc id: {result['mongo_id']})")
    except ValueError as e:
        result["mongo_error"] = str(e)
        print(f"  -> Skipped (not configured): {e}")
    except Exception as e:
        result["mongo_error"] = str(e)
        print(f"  -> Failed: {e}")

    # --- Step 5: Stream this record to Firehose (-> S3) ---
    print("[Step 5] Stream metadata to Firehose (-> S3)")
    if stream_to_firehose and metadata:
        try:
            streamer = MetadataStreamer()
            result["firehose_sent"] = streamer.stream_metadata(metadata)
            print(f"  -> Sent: {result['firehose_sent']}")
        except Exception as e:
            result["firehose_error"] = str(e)
            print(f"  -> Failed: {e}")
    else:
        print("  -> Skipped (--no-firehose or no metadata)")

    # --- Step 6: Optional backfill - stream recent from MongoDB to Firehose ---
    if backfill_firehose:
        print(f"[Step 6] Backfill: stream last {backfill_limit} records from MongoDB to Firehose")
        try:
            streamer = MetadataStreamer()
            result["backfill_sent"] = streamer.stream_recent(limit=backfill_limit)
            print(f"  -> Sent: {result['backfill_sent']} records")
        except Exception as e:
            result["backfill_error"] = str(e)
            print(f"  -> Failed: {e}")
    else:
        print("[Step 6] Backfill Firehose skipped (use --backfill-firehose to run)")

    # --- Step 7: Optional - verify Snowflake (3-table model: agent_runs, run_steps, api_calls) ---
    if verify_snowflake:
        print("[Step 7] Verify Snowflake (query recent agent_runs)")
        try:
            from src.snowflake.snowflake_client import SnowflakeClient
            client = SnowflakeClient()
            rows = client.get_agent_runs(limit=5)
            client.close()
            result["snowflake_count"] = len(rows)
            print(f"  -> Recent agent_runs in Snowflake: {len(rows)}")
        except Exception as e:
            result["snowflake_error"] = str(e)
            print(f"  -> Failed (Snowflake not configured?): {e}")
    else:
        print("[Step 7] Snowflake verify skipped (use --verify-snowflake to run)")

    return result


def main():
    parser = argparse.ArgumentParser(description="Run Tavily company research agent")
    parser.add_argument(
        "query",
        nargs="?",
        default="Nvidia",
        help="Company or topic to research (default: OpenAI)",
    )
    parser.add_argument(
        "--version",
        default="1.0.0",
        help="Agent version (default: 1.0.0)",
    )
    parser.add_argument(
        "--max-sources",
        type=int,
        default=5,
        help="Max sources to retrieve (default: 5)",
    )
    parser.add_argument(
        "--no-firehose",
        action="store_true",
        help="Skip streaming to Firehose",
    )
    parser.add_argument(
        "--backfill-firehose",
        action="store_true",
        help="Also stream recent MongoDB records to Firehose (Step 6)",
    )
    parser.add_argument(
        "--backfill-limit",
        type=int,
        default=10,
        help="Max records to backfill to Firehose (default: 10)",
    )
    parser.add_argument(
        "--verify-snowflake",
        action="store_true",
        help="Query Snowflake for recent rows (Step 7)",
    )
    args = parser.parse_args()

    try:
        result = run_research(
            query=args.query,
            agent_version=args.version,
            max_sources=args.max_sources,
            stream_to_firehose=not args.no_firehose,
            backfill_firehose=args.backfill_firehose,
            backfill_limit=args.backfill_limit,
            verify_snowflake=args.verify_snowflake,
        )

        # Summary
        state = result["state"]
        metadata = result["metadata"]
        print("\n--- Summary ---")
        if state.get("error"):
            print(f"Research: failed ({state['error']})")
        else:
            print(f"Research: OK | {len(state['sources'])} sources | {metadata['latency_ms']:.0f}ms")
        print(f"MongoDB:  {result['mongo_id'] or result['mongo_error'] or 'skipped'}")
        print(f"Firehose: {'sent' if result['firehose_sent'] else result['firehose_error'] or 'skipped'}")
        if result.get("backfill_sent") is not None and result["backfill_sent"] > 0:
            print(f"Backfill: {result['backfill_sent']} records")
        if result.get("snowflake_count") is not None:
            print(f"Snowflake: {result['snowflake_count']} recent rows")
        print("Dashboard: streamlit run src/dashboard/app.py")

        return 0 if not state.get("error") else 1

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
