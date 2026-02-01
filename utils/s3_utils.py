import boto3
from botocore.exceptions import ClientError

def upload_file(local_path, bucket, s3_key, region="ap-south-1"):
    """
    Uploads a local file to S3
    """
    s3 = boto3.client("s3", region_name=region)
    try:
        s3.upload_file(local_path, bucket, s3_key)
        print(f"Uploaded {local_path} to s3://{bucket}/{s3_key}")
    except ClientError as e:
        print(f"Failed to upload {local_path}: {e}")
        raise
