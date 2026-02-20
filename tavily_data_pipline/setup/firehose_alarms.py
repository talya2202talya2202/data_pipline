"""
CloudWatch alarms for Firehose: delivery errors, throttled records, data freshness.
"""

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


def create_firehose_alarms(
    stream_name: str,
    region: str,
    sns_topic_arn: str = None,
    error_threshold: int = 1,
    throttle_threshold: int = 1,
    freshness_threshold_seconds: int = 600,
):
    cloudwatch = boto3.client("cloudwatch", region_name=region)
    alarms = [
        {
            "AlarmName": f"{stream_name}-delivery-errors",
            "MetricName": "DeliveryToS3.Success", "Namespace": "AWS/Firehose", "Statistic": "Sum",
            "Period": 300, "EvaluationPeriods": 1, "Threshold": error_threshold,
            "ComparisonOperator": "LessThanThreshold",
            "Dimensions": [{"Name": "DeliveryStreamName", "Value": stream_name}],
            "TreatMissingData": "breaching",
        },
        {
            "AlarmName": f"{stream_name}-throttled-records",
            "MetricName": "ThrottledRecords", "Namespace": "AWS/Firehose", "Statistic": "Sum",
            "Period": 300, "EvaluationPeriods": 1, "Threshold": throttle_threshold,
            "ComparisonOperator": "GreaterThanThreshold",
            "Dimensions": [{"Name": "DeliveryStreamName", "Value": stream_name}],
        },
        {
            "AlarmName": f"{stream_name}-data-freshness",
            "MetricName": "DeliveryToS3.DataFreshness", "Namespace": "AWS/Firehose", "Statistic": "Maximum",
            "Period": 300, "EvaluationPeriods": 2, "Threshold": freshness_threshold_seconds,
            "ComparisonOperator": "GreaterThanThreshold",
            "Dimensions": [{"Name": "DeliveryStreamName", "Value": stream_name}],
        },
    ]
    for ac in alarms:
        params = {k: ac[k] for k in ["AlarmName", "MetricName", "Namespace", "Statistic", "Period", "EvaluationPeriods", "Threshold", "ComparisonOperator", "Dimensions"]}
        if "TreatMissingData" in ac:
            params["TreatMissingData"] = ac["TreatMissingData"]
        if sns_topic_arn:
            params["AlarmActions"] = [sns_topic_arn]
        try:
            cloudwatch.put_metric_alarm(**params)
            print(f"Created alarm: {ac['AlarmName']}")
        except ClientError as e:
            print(f"Failed {ac['AlarmName']}: {e}")


def main():
    stream_name = os.getenv("FIREHOSE_STREAM_NAME") or "agent-metadata-stream"
    region = os.getenv("AWS_REGION") or "us-east-1"
    create_firehose_alarms(stream_name=stream_name, region=region, sns_topic_arn=os.getenv("FIREHOSE_ALARM_SNS_TOPIC"))
    print("Firehose alarms created. Set FIREHOSE_ALARM_SNS_TOPIC for email notifications.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
