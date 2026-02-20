# One-time setup scripts

These scripts are for initial infrastructure setup only. Run from the **tavily_data_pipline** directory.

| Script | Purpose |
|--------|---------|
| `setup_firehose.py` | Create S3 bucket, IAM role, and Kinesis Firehose delivery stream |
| `setup_firehose_alarms.py` | Create CloudWatch alarms for Firehose (optional) |
| `setup_snowpipe.py` | Create Snowflake database, table, stage, and Snowpipe |
| `create_cloudwatch_dashboard.py` | Deploy CloudWatch dashboard for Firehose metrics |
| `fix_snowpipe_iam_trust.py` | Update AWS IAM role trust policy for Snowflake storage integration |

**Run from project root:**

```bash
cd tavily_data_pipline
python3 setup/setup_firehose.py
python3 setup/setup_snowpipe.py
# etc.
```

Ensure `.env` is configured with the required variables (see main README).
