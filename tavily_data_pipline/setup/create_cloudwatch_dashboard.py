#!/usr/bin/env python3
"""Create CloudWatch dashboard for Firehose monitoring. Deploys dashboard JSON to AWS."""

import json
import os
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

try:
    from dotenv import load_dotenv
    load_dotenv(project_root / ".env")
except ImportError:
    pass

import boto3
from botocore.exceptions import ClientError


def main():
    stream_name = os.getenv("FIREHOSE_STREAM_NAME") or "agent-metadata-stream"
    region = os.getenv("AWS_REGION") or "us-east-1"
    dashboard_path = Path(__file__).resolve().parent / "cloudwatch_firehose_dashboard.json"
    if not dashboard_path.exists():
        print(f"Dashboard config not found: {dashboard_path}")
        return 1
    with open(dashboard_path) as f:
        body = json.load(f)
    body_str = json.dumps(body).replace("agent-metadata-stream", stream_name).replace("us-east-1", region)
    try:
        boto3.client("cloudwatch", region_name=region).put_dashboard(
            DashboardName="FirehoseAgentMetadata",
            DashboardBody=body_str
        )
        print("Created CloudWatch dashboard: FirehoseAgentMetadata")
        return 0
    except ClientError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
