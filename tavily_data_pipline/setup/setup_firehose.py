#!/usr/bin/env python3
"""
AWS Kinesis Firehose setup script.

Creates:
- S3 bucket for Firehose delivery
- IAM role for Firehose with S3 and Firehose permissions
- Kinesis Firehose delivery stream with S3 destination

Requires: AWS CLI configured or AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION in env.
"""

import json
import sys
import time
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

try:
    from dotenv import load_dotenv
    load_dotenv(project_root / ".env")
except ImportError:
    pass

import os
import boto3
from botocore.exceptions import ClientError


def create_s3_bucket(bucket_name: str, region: str) -> bool:
    """Create S3 bucket for Firehose delivery."""
    s3 = boto3.client("s3", region_name=region)
    try:
        if region == "us-east-1":
            s3.create_bucket(Bucket=bucket_name)
        else:
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region}
            )
        print(f"Created S3 bucket: {bucket_name}")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
            print(f"S3 bucket already exists: {bucket_name}")
            return True
        raise


def create_firehose_role(role_name: str, bucket_arn: str, region: str) -> str:
    """Create IAM role for Firehose. Returns role ARN."""
    iam = boto3.client("iam", region_name=region)
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "firehose.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }
    try:
        role = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description="Role for Kinesis Firehose to deliver to S3"
        )
        role_arn = role["Role"]["Arn"]
        print(f"Created IAM role: {role_arn}")
    except ClientError as e:
        if "EntityAlreadyExists" in str(e):
            role_arn = f"arn:aws:iam::{boto3.client('sts').get_caller_identity()['Account']}:role/{role_name}"
            print(f"IAM role already exists: {role_arn}")
        else:
            raise
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:PutObject", "s3:GetBucketLocation"],
                "Resource": [bucket_arn, f"{bucket_arn}/*"]
            },
            {"Effect": "Allow", "Action": ["logs:PutLogEvents"], "Resource": "*"}
        ]
    }
    try:
        iam.put_role_policy(
            RoleName=role_name,
            PolicyName=f"{role_name}-policy",
            PolicyDocument=json.dumps(policy)
        )
        print("Attached IAM policy to role")
    except ClientError:
        pass
    time.sleep(10)
    return role_arn


def create_firehose_stream(
    stream_name: str,
    bucket_name: str,
    prefix: str,
    role_arn: str,
    region: str
) -> bool:
    """Create Kinesis Firehose delivery stream."""
    firehose = boto3.client("firehose", region_name=region)
    try:
        firehose.create_delivery_stream(
            DeliveryStreamName=stream_name,
            DeliveryStreamType="DirectPut",
            ExtendedS3DestinationConfiguration={
                "RoleARN": role_arn,
                "BucketARN": f"arn:aws:s3:::{bucket_name}",
                "Prefix": prefix,
                "BufferingHints": {"SizeInMBs": 5, "IntervalInSeconds": 60},
                "CompressionFormat": "UNCOMPRESSED"
            }
        )
        print(f"Created Firehose delivery stream: {stream_name}")
        return True
    except ClientError as e:
        if "ResourceInUseException" in str(e):
            print(f"Firehose stream already exists: {stream_name}")
            return True
        raise


def main():
    bucket_name = os.getenv("S3_BUCKET_NAME") or "agent-metadata-firehose"
    stream_name = os.getenv("FIREHOSE_STREAM_NAME") or "agent-metadata-stream"
    region = os.getenv("AWS_REGION") or "us-east-1"
    role_name = "firehose-agent-metadata-role"
    prefix = "agent_metadata/"
    print("Setting up AWS Firehose...")
    print(f"  S3 Bucket: {bucket_name}\n  Stream: {stream_name}\n  Region: {region}")
    try:
        create_s3_bucket(bucket_name, region)
        bucket_arn = f"arn:aws:s3:::{bucket_name}"
        role_arn = create_firehose_role(role_name, bucket_arn, region)
        create_firehose_stream(stream_name, bucket_name, prefix, role_arn, region)
        print("\nSetup complete. Add to .env:")
        print(f"  FIREHOSE_STREAM_NAME={stream_name}\n  S3_BUCKET_NAME={bucket_name}")
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
