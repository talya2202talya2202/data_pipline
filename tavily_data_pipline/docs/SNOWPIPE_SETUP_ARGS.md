# Arguments Needed for Snowpipe / IAM Trust Setup

So the scripts can run without you doing manual steps in the AWS Console, have these in your **`.env`** (in `tavily_data_pipline/`):

## Required (no defaults)

| Variable | Where to get it | Example |
|----------|-----------------|--------|
| `SNOWFLAKE_ACCOUNT` | Snowflake URL or account locator | `HNSOKKI-MT40946` |
| `SNOWFLAKE_USER` | Your Snowflake login | `TALYASAKOV` |
| `SNOWFLAKE_PASSWORD` | Your Snowflake password | (your password) |
| `SNOWFLAKE_WAREHOUSE` | Snowflake warehouse name | `COMPUTE_WH` |
| `AWS_ACCESS_KEY_ID` | IAM user access key (same account as the role) | From IAM → Users → Security credentials |
| `AWS_SECRET_ACCESS_KEY` | IAM user secret key | Same place |
| `AWS_REGION` | Region where S3 bucket and IAM role live | `us-east-1` |

## Optional (have defaults)

| Variable | Default | When to set |
|----------|---------|-------------|
| `SNOWFLAKE_STORAGE_INTEGRATION` | `S3_AGENT_METADATA` | If your integration has another name |
| `AWS_IAM_ROLE_NAME` | `snowflake-s3-agent-metadata` | If your role has another name |
| `S3_BUCKET_NAME` | `agent-metadata-firehose` | Used by setup_snowpipe / stage URL |

## What the scripts do

1. **`setup/fix_snowpipe_iam_trust.py`**  
   - Connects to Snowflake and runs `DESC INTEGRATION <name>`.  
   - Reads `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID`.  
   - Updates the AWS IAM role trust policy so that Snowflake can assume the role.  
   - **You must run this before the pipe works** (fixes the “not authorized to perform: sts:AssumeRole” error).

2. **`setup/setup_snowpipe.py`**  
   - Creates DB/schema, table, file format, stage, and pipe in Snowflake.  
   - Run after the IAM trust is fixed.

## How to run (after .env is set)

```bash
cd tavily_data_pipline

# Use existing venv if you have one, or:
# python3 -m venv .venv && source .venv/bin/activate
# pip install -r requirements.txt

# 1. Fix IAM trust (required once)
python setup/fix_snowpipe_iam_trust.py
```

If you see **AccessDenied** when updating the role (your IAM user cannot call `iam:UpdateAssumeRolePolicy`), the script will write `snowpipe_trust_policy.json` and print instructions. Apply that policy manually in AWS Console: **IAM → Roles → snowflake-s3-agent-metadata → Trust relationships → Edit trust policy** → paste the JSON → Save.

Alternatively, run with **`--write-policy`** to only generate the JSON file and skip the AWS API call:

```bash
python setup/fix_snowpipe_iam_trust.py --write-policy
```

Then in Snowflake, run the pipe creation SQL (with `USE DATABASE` / `USE SCHEMA` first) and configure the S3 event notification using the pipe’s `notification_channel` ARN.
