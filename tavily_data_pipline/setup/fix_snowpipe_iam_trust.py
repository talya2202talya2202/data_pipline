#!/usr/bin/env python3
"""
Fix Snowpipe IAM trust: fetch Snowflake storage integration details and update
AWS IAM role trust policy so Snowflake can assume the role.

Run from project root. .env: SNOWFLAKE_*, AWS_*, optional SNOWFLAKE_STORAGE_INTEGRATION, AWS_IAM_ROLE_NAME.
"""

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

import snowflake.connector
import boto3
from botocore.exceptions import ClientError


def get_integration_properties(integration_name: str) -> dict:
    """Return STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID from Snowflake."""
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE") or "COMPUTE_WH"
    if not all([account, user, password]):
        raise ValueError("Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD in .env")
    conn = snowflake.connector.connect(
        account=account, user=user, password=password, warehouse=warehouse,
    )
    try:
        cursor = conn.cursor()
        cursor.execute(f"DESC INTEGRATION {integration_name};")
        rows = cursor.fetchall()
        out = {}
        for row in rows:
            prop, _, value, _ = row
            if prop == "STORAGE_AWS_IAM_USER_ARN":
                out["iam_user_arn"] = value
            elif prop == "STORAGE_AWS_EXTERNAL_ID":
                out["external_id"] = value
        cursor.close()
        if "iam_user_arn" not in out or "external_id" not in out:
            raise ValueError("DESC INTEGRATION did not return expected properties.")
        return out
    finally:
        conn.close()


def build_trust_policy(iam_user_arn: str, external_id: str) -> dict:
    return {
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "",
            "Effect": "Allow",
            "Principal": {"AWS": iam_user_arn},
            "Action": "sts:AssumeRole",
            "Condition": {"StringEquals": {"sts:ExternalId": external_id}},
        }],
    }


def update_iam_trust_policy(role_name: str, iam_user_arn: str, external_id: str, region: str) -> bool:
    iam = boto3.client("iam", region_name=region)
    try:
        iam.update_assume_role_policy(
            RoleName=role_name,
            PolicyDocument=json.dumps(build_trust_policy(iam_user_arn, external_id)),
        )
        print(f"Updated trust policy for IAM role: {role_name}")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchEntity":
            raise SystemExit(f"IAM role '{role_name}' not found.") from e
        if e.response["Error"]["Code"] == "AccessDenied":
            return False
        raise


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Fix Snowpipe IAM trust policy")
    parser.add_argument("--write-policy", action="store_true", help="Only write policy JSON (no AWS update)")
    args = parser.parse_args()
    integration_name = os.getenv("SNOWFLAKE_STORAGE_INTEGRATION") or "S3_AGENT_METADATA"
    role_name = os.getenv("AWS_IAM_ROLE_NAME") or "snowflake-s3-agent-metadata"
    region = os.getenv("AWS_REGION") or "us-east-1"
    policy_path = project_root / "snowpipe_trust_policy.json"
    print("Fetching Snowflake storage integration details...")
    props = get_integration_properties(integration_name)
    iam_user_arn, external_id = props["iam_user_arn"], props["external_id"]
    print(f"  STORAGE_AWS_IAM_USER_ARN: {iam_user_arn}\n  EXTERNAL_ID length: {len(external_id)}")
    trust_policy = build_trust_policy(iam_user_arn, external_id)
    if args.write_policy:
        policy_path.write_text(json.dumps(trust_policy, indent=2))
        print(f"Wrote: {policy_path}. Apply manually in IAM → Roles → {role_name} → Trust relationships.")
        return 0
    print("Updating IAM role trust policy in AWS...")
    if update_iam_trust_policy(role_name, iam_user_arn, external_id, region):
        print("Done. You can run CREATE PIPE in Snowflake.")
        return 0
    policy_path.write_text(json.dumps(trust_policy, indent=2))
    print(f"Wrote: {policy_path}. Apply manually in IAM → Roles → {role_name} → Trust relationships.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
