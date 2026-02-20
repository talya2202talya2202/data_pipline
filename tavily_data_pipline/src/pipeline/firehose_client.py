"""
AWS Kinesis Firehose client for streaming agent metadata to S3.

Provides methods to send metadata records to a Firehose delivery stream
with retry logic and batch processing.
"""

import json
import os
import time
from typing import Dict, Any, List, Optional
import boto3
from botocore.exceptions import ClientError
from botocore.exceptions import NoCredentialsError


class FirehoseClient:
    """
    Client for streaming metadata records to AWS Kinesis Firehose.
    
    Records are buffered by Firehose and delivered to S3 based on
    buffer size (5MB) or buffer interval (60 seconds).
    """
    
    def __init__(
        self,
        stream_name: Optional[str] = None,
        region: Optional[str] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ):
        """
        Initialize Firehose client.
        
        Args:
            stream_name: Firehose delivery stream name. Reads from FIREHOSE_STREAM_NAME env if None.
            region: AWS region. Reads from AWS_REGION env if None.
            max_retries: Maximum retry attempts for failed deliveries
            retry_delay: Base delay in seconds for exponential backoff
        """
        self.stream_name = stream_name or os.getenv("FIREHOSE_STREAM_NAME")
        self.region = region or os.getenv("AWS_REGION", "us-east-1")
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        if not self.stream_name:
            raise ValueError("FIREHOSE_STREAM_NAME must be provided or set as environment variable")

        # Validate credentials early with a cheap call (optional; boto3 will fail on first API call otherwise)
        self._ensure_credentials()

        self.client = boto3.client("firehose", region_name=self.region)

    def _ensure_credentials(self) -> None:
        """Verify AWS credentials are set and valid; raise with helpful message if not."""
        access = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY")
        secret = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_KEY")
        if not access or not secret:
            raise ValueError(
                "AWS credentials not set. In .env set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY. "
                "Create them in IAM → Users → Your user → Security credentials → Create access key."
            )
        try:
            sts = boto3.client("sts", region_name=self.region)
            sts.get_caller_identity()
        except NoCredentialsError:
            raise ValueError(
                "AWS credentials not found. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in .env."
            ) from None
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "UnrecognizedClientException" or "security token" in str(e).lower():
                raise ValueError(
                    "AWS rejected the security token (invalid or expired). "
                    "Fix: 1) Use IAM → Users → Your user → Create access key and put the new key/secret in .env. "
                    "2) Ensure .env has no typos and no extra spaces. "
                    "3) Use the same AWS account that owns the Firehose stream. "
                    "4) If using temporary credentials, refresh AWS_SESSION_TOKEN."
                ) from e
            raise
    
    def _record_to_firehose_format(self, record: Dict[str, Any]) -> bytes:
        """Convert metadata dict to Firehose record format (JSON bytes)."""
        # Remove _id if present (MongoDB ObjectId)
        clean = {k: v for k, v in record.items() if k != "_id"}
        return (json.dumps(clean) + "\n").encode("utf-8")
    
    def send_metadata(self, metadata: Dict[str, Any]) -> bool:
        """
        Send a single metadata record to Firehose.
        
        Args:
            metadata: Metadata dictionary (event_id, timestamp_utc, query, etc.)
            
        Returns:
            True if successful, False otherwise
        """
        return self.send_batch([metadata]) == 1
    
    def send_batch(self, records: List[Dict[str, Any]]) -> int:
        """
        Send a batch of metadata records to Firehose.
        
        Firehose limits: max 500 records per PutRecordBatch, max 5MB total.
        Uses batches of 25 records for safety.
        
        Args:
            records: List of metadata dictionaries
            
        Returns:
            Number of records successfully sent
        """
        if not records:
            return 0
        
        batch_size = 25
        sent = 0
        
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            firehose_records = [
                {"Data": self._record_to_firehose_format(r)}
                for r in batch
            ]
            
            for attempt in range(self.max_retries):
                try:
                    response = self.client.put_record_batch(
                        DeliveryStreamName=self.stream_name,
                        Records=firehose_records
                    )
                    failed = response.get("FailedPutCount", 0)
                    if failed == 0:
                        sent += len(batch)
                        break
                    # Retry failed records
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay * (2 ** attempt))
                    else:
                        # Log but don't raise - partial success
                        break
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code", "")
                    if error_code == "UnrecognizedClientException" or "security token" in str(e).lower():
                        raise ValueError(
                            "AWS rejected the security token (invalid or expired). "
                            "Fix: 1) IAM → Users → Your user → Security credentials → Create access key; "
                            "put Access key ID and Secret in .env as AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY. "
                            "2) No typos or extra spaces in .env. "
                            "3) Same AWS account as the Firehose stream. "
                            "4) If using temporary credentials, refresh AWS_SESSION_TOKEN."
                        ) from e
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay * (2 ** attempt))
                    else:
                        raise RuntimeError(f"Firehose put_record_batch failed: {e}") from e
        
        return sent
