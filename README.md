# AWS Data Engineering Level 1 Pipeline

## Overview
- Generate fake orders locally
- Upload to S3 (raw)
- Clean data using PySpark
- Write parquet to S3 (clean)
- Config-driven and modular
- Ready for Glue/EMR

## How to Run Locally

1. Create virtual environment
```bash
python3.10 -m venv venv
source venv/bin/activate
pip install -r requirements.txt