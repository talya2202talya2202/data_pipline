# Tavily Data Pipeline

A data pipeline that runs a Tavily-based company research agent, collects execution metadata, and streams it through MongoDB → AWS Firehose → S3 → Snowflake Snowpipe, with a Streamlit dashboard for visualization.

## Architecture

```
┌─────────────────┐     ┌─────────────┐     ┌──────────────┐     ┌───────────┐     ┌────────────┐
│  Toy AI Agent   │────▶│  Metadata   │────▶│   MongoDB    │────▶│ Firehose  │────▶│     S3     │
│  (Tavily)       │     │  Collector  │     │   Atlas      │     │           │     │            │
└─────────────────┘     └─────────────┘     └──────────────┘     └───────────┘     └─────┬──────┘
                                                                                         │
                                                                                         ▼
┌─────────────────┐     ┌─────────────┐
│   Streamlit     │◀───│  Snowflake  │◀──── Snowpipe (auto-ingest)
│   Dashboard     │     │  Snowpipe   │
└─────────────────┘     └─────────────┘
```

1. **Toy AI Agent** – Executes company research using Tavily API
2. **Metadata Collector** – Captures execution metrics (latency, status, sources)
3. **MongoDB** – Stores metadata documents
4. **AWS Firehose** – Streams metadata from MongoDB to S3 (optional)
5. **Snowpipe** – Ingests S3 data into Snowflake (optional)
6. **Streamlit Dashboard** – Visualizes metadata from MongoDB

## Quick Start

### 1. Create virtual environment and install dependencies

```bash
cd tavily_data_pipline
python3 -m venv venv
source venv/bin/activate   # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure environment

Copy `.env.example` to `.env` and set your API keys:

```bash
cp .env.example .env
# Edit .env and set at minimum:
#   TAVILY_API_KEY=your_key
#   MONGODB_URI=your_mongodb_uri  (optional - agent runs without it)
```

### 3. Run the agent

```bash
# From tavily_data_pipline directory
python3 scripts/run_agent.py "OpenAI"

# Or with defaults
python3 scripts/run_agent.py

# Custom options
python3 scripts/run_agent.py "Anthropic" --max-sources 3 --version 1.0.0

# Skip Firehose streaming
python3 scripts/run_agent.py "Tesla" --no-firehose
```

The script will:
1. Run company research via Tavily API
2. Collect execution metadata (latency, status, sources count)
3. Save metadata to MongoDB (if `MONGODB_URI` is set)
4. Stream to Firehose (if AWS credentials and `FIREHOSE_STREAM_NAME` are set)

### 4. Run the dashboard

```bash
streamlit run src/dashboard/app.py
```

Open http://localhost:8501 to view visualizations.

## Setup Instructions

### MongoDB Atlas

1. Create account at [mongodb.com/atlas](https://www.mongodb.com/atlas)
2. Create a free M0 cluster
3. Create database user and allow network access
4. Get connection string from Connect → Drivers
5. Set `MONGODB_URI` in `.env`

### AWS Firehose (optional)

1. Configure AWS credentials (`aws configure` or env vars)
2. Run: `python3 setup/setup_firehose.py`
3. Add to `.env`: `FIREHOSE_STREAM_NAME`, `S3_BUCKET_NAME`, `AWS_REGION`

### Snowflake Snowpipe (optional)

1. Create Snowflake account and storage integration for S3 in Snowflake UI
2. Run: `python3 setup/setup_snowpipe.py`
3. Add to `.env`: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, etc.
4. Configure S3 event notification to trigger Snowpipe (use `SHOW PIPES` for ARN)

One-time setup scripts (Firehose, Snowpipe, CloudWatch dashboard/alarms) live in the **`setup/`** folder. See `setup/README.md`.

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| TAVILY_API_KEY | Yes | Tavily API key |
| MONGODB_URI | For MongoDB | MongoDB connection string |
| FIREHOSE_STREAM_NAME | For Firehose | Kinesis Firehose stream name |
| S3_BUCKET_NAME | For Firehose | S3 bucket for Firehose delivery |
| AWS_ACCESS_KEY_ID | For Firehose | AWS access key |
| AWS_SECRET_ACCESS_KEY | For Firehose | AWS secret key |
| AWS_REGION | For Firehose | AWS region |
| SNOWFLAKE_ACCOUNT | For Snowflake | Snowflake account identifier |
| SNOWFLAKE_USER | For Snowflake | Snowflake username |
| SNOWFLAKE_PASSWORD | For Snowflake | Snowflake password |
| SNOWFLAKE_WAREHOUSE | For Snowflake | Warehouse name |
| SNOWFLAKE_DATABASE | For Snowflake | Database name |
| SNOWFLAKE_SCHEMA | For Snowflake | Schema name |

## Security Practices

- Never commit `.env` (it is in `.gitignore`)
- Use `.env.example` with placeholders only
- Rotate API keys and credentials periodically
- Use IAM roles in production instead of access keys
- Restrict MongoDB and S3 network access

## Project Structure

```
tavily_data_pipline/
├── src/
│   ├── agent/           # Toy agent and metadata collector
│   ├── database/        # MongoDB client
│   ├── pipeline/        # Firehose client and metadata streamer
│   ├── snowflake/       # Snowflake client and Snowpipe helpers
│   └── dashboard/       # Streamlit app
├── scripts/
│   └── run_agent.py     # Main agent script
├── setup/               # One-time infra setup (Firehose, Snowpipe, CloudWatch)
├── config/              # Runtime config (config.yaml)
└── docs/
```

## Documentation

See [docs/architecture.md](docs/architecture.md) for detailed architecture, data flow, and error handling.
