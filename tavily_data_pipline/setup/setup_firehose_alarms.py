#!/usr/bin/env python3
"""Create CloudWatch alarms for Firehose. Set FIREHOSE_ALARM_SNS_TOPIC for notifications."""

import importlib.util
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

try:
    from dotenv import load_dotenv
    load_dotenv(project_root / ".env")
except ImportError:
    pass

spec = importlib.util.spec_from_file_location(
    "firehose_alarms",
    Path(__file__).resolve().parent / "firehose_alarms.py"
)
firehose_alarms = importlib.util.module_from_spec(spec)
spec.loader.exec_module(firehose_alarms)

if __name__ == "__main__":
    sys.exit(firehose_alarms.main() or 0)
