import os
from utils.s3_utils import upload_file
from utils.config_loader import load_config
from ingestion.generate_orders import generate_orders

def main():
    config = load_config("dev")
    bucket = config["aws"]["bucket"]
    region = config["aws"]["region"]
    raw_path = config["paths"]["raw_orders"]

    # Generate data
    local_csv = generate_orders()

    # Upload to S3
    file_name = os.path.basename(local_csv)
    s3_key = f"{raw_path}/{file_name}"
    upload_file(local_csv, bucket, s3_key, region=region)

if __name__ == "__main__":
    main()