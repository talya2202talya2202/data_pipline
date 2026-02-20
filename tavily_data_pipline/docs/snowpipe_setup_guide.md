# Snowpipe Setup Guide – Step by Step

You’ve finished **Configuring secure access to Cloud Storage** (Steps 1–5). Follow these steps from **“Determining the correct option”** to the end.

---

## Step A: Determine the correct option

**Question:** Is there already an S3 event notification for the path where Firehose writes?

- Your Firehose writes to: `s3://<your-bucket>/agent_metadata/`
- If you have **no** existing S3 event notification on that bucket/path → use **Option 1** (recommended for you).
- If another service already sends S3 events for that path → use **Option 2** (SNS) to avoid conflicts.

**For this pipeline:** Use **Option 1 – New S3 event notification.**

---

## Option 1: Create a new S3 event notification (your path)

### Step 1: Create the stage (if not already done)

1. In Snowflake (Snowsight or classic UI), use the same database/schema as your integration:
   ```sql
   USE DATABASE AGENT_METADATA_DB;
   USE SCHEMA PUBLIC;
   ```
2. Create the stage (replace `<your-bucket>` with your real S3 bucket name, e.g. `agent-metadata-firehose-talyas`):
   ```sql
   CREATE STAGE IF NOT EXISTS agent_metadata_stage
     URL = 's3://<your-bucket>/agent_metadata/'
     STORAGE_INTEGRATION = <your_storage_integration_name>;
   ```
   Example:
   ```sql
   CREATE STAGE IF NOT EXISTS agent_metadata_stage
     URL = 's3://agent-metadata-firehose-talyas/agent_metadata/'
     STORAGE_INTEGRATION = s3_integration;
   ```
3. Or run your script (after stage SQL exists):
   ```bash
   python setup/setup_snowpipe.py
   ```

---

### Step 2: Create the pipe with auto-ingest enabled

1. **Set the current database and schema** (required; otherwise Snowflake returns “This session does not have a current database”):
   ```sql
   USE DATABASE AGENT_METADATA_DB;
   USE SCHEMA PUBLIC;
   ```
2. Ensure the target table exists (your `setup_snowpipe.py` creates it).
3. Create the file format if not exists:
   ```sql
   CREATE OR REPLACE FILE FORMAT agent_metadata_json_format
     TYPE = 'JSON'
     STRIP_OUTER_ARRAY = FALSE
     DATE_FORMAT = 'AUTO'
     TIMESTAMP_FORMAT = 'AUTO';
   ```
4. Create the pipe with **AUTO_INGEST = TRUE**:
   ```sql
   CREATE OR REPLACE PIPE agent_metadata_pipe
     AUTO_INGEST = TRUE
     AS
     COPY INTO AGENT_METADATA_DB.PUBLIC.agent_metadata
     FROM @AGENT_METADATA_DB.PUBLIC.agent_metadata_stage
     FILE_FORMAT = agent_metadata_json_format
     MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
     ON_ERROR = 'CONTINUE';
   ```
5. Run:
   ```sql
   SHOW PIPES;
   ```
   In the result, find the row for `agent_metadata_pipe` and **copy the value in the `notification_channel` column** (the SQS queue ARN). You will need it in Step 4.

---

### Step 3: Configure security (privileges)

Grant the role that runs the pipe (e.g. your user’s role or a dedicated `snowpipe_role`) the required privileges:

```sql
USE ROLE SECURITYADMIN;  -- or ACCOUNTADMIN if you have it

-- Create a role for Snowpipe (optional but recommended)
CREATE ROLE IF NOT EXISTS snowpipe_role;

-- Database & schema
GRANT USAGE ON DATABASE AGENT_METADATA_DB TO ROLE snowpipe_role;
GRANT USAGE ON SCHEMA AGENT_METADATA_DB.PUBLIC TO ROLE snowpipe_role;

-- Target table
GRANT INSERT, SELECT ON TABLE AGENT_METADATA_DB.PUBLIC.agent_metadata TO ROLE snowpipe_role;

-- Stage
GRANT USAGE, READ ON STAGE AGENT_METADATA_DB.PUBLIC.agent_metadata_stage TO ROLE snowpipe_role;

-- If your stage uses a storage integration:
GRANT USAGE ON INTEGRATION <your_storage_integration_name> TO ROLE snowpipe_role;

-- Pipe ownership (pause first, then grant, then resume)
ALTER PIPE AGENT_METADATA_DB.PUBLIC.agent_metadata_pipe SET PIPE_EXECUTION_PAUSED = TRUE;
GRANT OWNERSHIP ON PIPE AGENT_METADATA_DB.PUBLIC.agent_metadata_pipe TO ROLE snowpipe_role;
GRANT ROLE snowpipe_role TO USER <your_snowflake_username>;
ALTER USER <your_snowflake_username> SET DEFAULT_ROLE = snowpipe_role;
ALTER PIPE AGENT_METADATA_DB.PUBLIC.agent_metadata_pipe SET PIPE_EXECUTION_PAUSED = FALSE;
```

If you prefer to keep using your current role (e.g. ACCOUNTADMIN), ensure that role has USAGE on database/schema, INSERT/SELECT on the table, and USAGE/READ on the stage and OWNERSHIP on the pipe.

---

### Step 4: Configure S3 event notification (critical for auto-ingest)

This connects S3 to the Snowpipe SQS queue so that when Firehose (or anything) puts files under `agent_metadata/`, Snowpipe is triggered.

1. **Get the SQS queue ARN**  
   From Step 2 you already have the `notification_channel` value from `SHOW PIPES` for `agent_metadata_pipe`. It looks like:
   `arn:aws:sqs:us-east-1:123456789012:sf-snowpipe-...`

2. **Open Amazon S3**
   - Go to [AWS Console → S3](https://s3.console.aws.amazon.com/).
   - Open the bucket where Firehose delivers (e.g. `agent-metadata-firehose-talyas`).

3. **Create event notification**
   - In the bucket, go to the **Properties** tab.
   - Scroll to **Event notifications** → **Create event notification**.

4. **Fill the form**
   - **Name:** e.g. `Auto-ingest-Snowflake`
   - **Prefix (optional):** `agent_metadata/`  
     (So only objects under this path trigger the notification. Matches your Firehose prefix.)
   - **Suffix (optional):** leave blank, or e.g. `.json` if you only want JSON files.
   - **Events:** choose **All object create events** (or “Object creation – All”).
   - **Destination:** **SQS Queue**.
   - **SQS Queue:** choose **Enter SQS queue ARN** and paste the **notification_channel** ARN from `SHOW PIPES`.

5. **Save** the event notification.

**Important:** If AWS says a conflicting notification already exists for this prefix, you cannot create a second one for the same path; then you must use **Option 2** (SNS) from the Snowflake doc.

---

### Step 5: Load historical files (optional)

If there are already files in `s3://<bucket>/agent_metadata/` from before the event notification was set up:

1. In Snowflake:
   ```sql
   COPY INTO AGENT_METADATA_DB.PUBLIC.agent_metadata
   FROM @AGENT_METADATA_DB.PUBLIC.agent_metadata_stage
   FILE_FORMAT = agent_metadata_json_format
   MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
   ON_ERROR = 'CONTINUE';
   ```
2. Or use the Snowflake doc section “Loading historic data” for more options (e.g. by file list).

---

### Step 6: Delete staged files (optional)

After data is loaded and you no longer need the raw files in S3, you can remove them. See Snowflake docs: “Deleting staged files after Snowpipe loads the data.” This is optional and often delayed for debugging or backup.

---

## Verify end-to-end

1. **Trigger the pipeline:** Run your toy agent so it sends metadata → MongoDB → Firehose → S3.
2. **Wait:** Firehose may buffer for up to 60 seconds; then S3 event → SQS → Snowpipe may take a short time.
3. **In Snowflake:**
   - Check pipe status:
     ```sql
     SELECT SYSTEM$PIPE_STATUS('AGENT_METADATA_DB.PUBLIC.agent_metadata_pipe');
     ```
   - Check loaded rows:
     ```sql
     SELECT * FROM AGENT_METADATA_DB.PUBLIC.agent_metadata ORDER BY ingested_at DESC LIMIT 10;
     ```
   - Check load history:
     ```sql
     SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
       TABLE_NAME => 'AGENT_METADATA_DB.PUBLIC.agent_metadata',
       START_TIME => DATEADD('hours', -1, CURRENT_TIMESTAMP())
     ));
     ```

---

## Quick reference

| Step | What you do |
|------|------------------|
| A    | Choose Option 1 (new S3 event notification). |
| 1    | Create stage pointing at `s3://<bucket>/agent_metadata/` with storage integration. |
| 2    | Create pipe with `AUTO_INGEST = TRUE`, run `SHOW PIPES`, copy `notification_channel` ARN. |
| 3    | Grant DB/schema/table/stage/pipe privileges to the role that owns the pipe. |
| 4    | In S3 bucket: create event notification (prefix `agent_metadata/`, all create events → SQS queue ARN from Step 2). |
| 5    | (Optional) Manually load existing files with `COPY INTO ... FROM @stage`. |
| 6    | (Optional) Delete staged files in S3 after load. |

After Step 4, new files written by Firehose to `agent_metadata/` will trigger Snowpipe automatically.

---

## Troubleshooting: "User ... is not authorized to perform: sts:AssumeRole"

**Error example:**  
`User: arn:aws:iam::629236738139:user/pqhh1000-s is not authorized to perform: sts:AssumeRole on resource: arn:aws:iam::801190828042:role/snowflake-s3-agent-metadata`

This means the **IAM role trust policy** in your AWS account does not allow Snowflake’s IAM user to assume the role. Fix it in AWS:

### 1. Get Snowflake’s identity in Snowflake

Run (use your storage integration name):

```sql
DESC INTEGRATION <your_storage_integration_name>;
```

Example: `DESC INTEGRATION s3_integration;`

From the result, copy:

- **STORAGE_AWS_IAM_USER_ARN** (e.g. `arn:aws:iam::629236738139:user/pqhh1000-s`)
- **STORAGE_AWS_EXTERNAL_ID** (long string like `MYACCOUNT_SFCRole=2_abc123...`)

### 2. Fix the role trust policy in AWS

1. Log in to **AWS** in the account where the role lives (e.g. **801190828042**).
2. Open **IAM** → **Roles**.
3. Open the role used by the integration (e.g. **snowflake-s3-agent-metadata**).
4. Go to the **Trust relationships** tab → **Edit trust policy**.
5. Replace the policy with (use the two values from step 1):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "AWS": "<paste STORAGE_AWS_IAM_USER_ARN here>"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "<paste STORAGE_AWS_EXTERNAL_ID here>"
        }
      }
    }
  ]
}
```

Example with your error’s user ARN:

- **Principal AWS:** `arn:aws:iam::629236738139:user/pqhh1000-s`
- **sts:ExternalId:** exact value from `DESC INTEGRATION` (no spaces, no changes)

6. Save the trust policy.

### 3. Retry in Snowflake

Run the pipe again (e.g. `CREATE OR REPLACE PIPE ...` or trigger a load). If the trust policy is correct, the assume-role error goes away.

### Still getting the assume-role error?

Check the following (per [Snowflake’s S3 automation doc](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-auto-s3)):

1. **Correct AWS account**  
   The role must be in the account that owns the S3 bucket. The error shows the role as `arn:aws:iam::801190828042:role/snowflake-s3-agent-metadata`, so you must be logged into AWS account **801190828042** when editing the role.

2. **Correct role and tab**  
   IAM → **Roles** → open **snowflake-s3-agent-metadata**. Edit the **Trust relationships** tab only (not Permissions). Replace the entire trust policy with the JSON from `snowpipe_trust_policy.json`.

3. **Exact Principal and External ID**  
   The trust policy must have:
   - **Principal → AWS:** exactly `arn:aws:iam::629236738139:user/pqhh1000-s` (no extra spaces).
   - **Condition → StringEquals → sts:ExternalId:** exactly the value from Snowflake `DESC INTEGRATION S3_AGENT_METADATA` (copy from `snowpipe_trust_policy.json`; include any trailing `=`).

4. **Save**  
   Click **Update policy** in the Trust relationships editor.

5. **Propagation**  
   Wait 2–3 minutes after saving, then run `CREATE PIPE` again. Snowflake may cache credentials for a short time.

6. **Validate from Snowflake (optional)**  
   After the trust policy is correct, you can validate the storage integration (replace `<bucket>` with your S3 bucket name, e.g. `agent-metadata-firehose-talyas`):

   ```sql
   SELECT SYSTEM$VALIDATE_STORAGE_INTEGRATION('S3_AGENT_METADATA', 's3://<bucket>/agent_metadata/', '', 'read');
   ```

   If the result shows `"status":"success"`, the integration can assume the role and read from S3. Then run `CREATE PIPE` again.

### Automated fix (script)

You can run the project script to apply the trust policy using values from Snowflake and your .env:

```bash
cd tavily_data_pipline
python setup/fix_snowpipe_iam_trust.py
```

**Required in .env:** `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_WAREHOUSE`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`.

**Optional in .env:** `SNOWFLAKE_STORAGE_INTEGRATION` (default `S3_AGENT_METADATA`), `AWS_IAM_ROLE_NAME` (default `snowflake-s3-agent-metadata`).
