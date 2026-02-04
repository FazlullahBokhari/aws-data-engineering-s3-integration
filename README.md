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
```

## docker commands
```
build the app: docker build -t data-engineering-pipeline .

generate orders: docker run --rm -it `
   -v "C:/Users/fazlu/Downloads/Data-Engineering-projects/aws-data-engineering-level1:/app" `                                                                                     
   -w /app `                                                                                                                                                                      
   data-engineering-pipeline `                                                                                                                                                    
   python3 ingestion/generate_orders.py
   
upload to s3: docker run --rm -it `
   --env-file "C:/Users/fazlu/aws-secrets/aws.env" `                                                                                                                              
   -v "C:/Users/fazlu/Downloads/Data-Engineering-projects/aws-data-engineering-level1:/app" `                                                                                     
   -w /app `                                                                                                                                                                      
   data-engineering-pipeline `                                                                                                                                                    
   python3 ingestion/upload_to_s3.py

clean and convert to parquet app: docker run --rm -it `
   --env-file "C:/Users/fazlu/aws-secrets/aws.env" `                                                                                                                              
   -v "C:/Users/fazlu/.ivy2:/root/.ivy2" `                                                                                                                                        
   -v "C:/Users/fazlu/Downloads/Data-Engineering-projects/aws-data-engineering-level1:/app" `                                                                                     
   -w /app `                                                                                                                                                                      
   pyspark-local:latest `                                                                                                                                                         
  python3 -m spark_jobs.clean_orders_job    

create order:  docker run --rm -it `
   -v "C:/Users/fazlu/Downloads/Data-Engineering-projects/aws-data-engineering-level1:/app" `                                                                                     
   -w /app `                                                                                                                                                                      
   pyspark-local:latest `                                                                                                                                                         
   python3 ingestion/generate_orders.py --num_records 1000000
```

