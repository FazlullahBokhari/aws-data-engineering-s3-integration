import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from utils.config_loader import load_config

# Load config
config = load_config("dev")
spark_conf = config["spark"]
aws_conf = config["aws"]

# Create SparkSession with fixes for Docker
builder = (
    SparkSession.builder
    .appName(spark_conf["app_name"])
    .master(spark_conf["master"])
    .config("spark.sql.shuffle.partitions", spark_conf["shuffle_partitions"])
    .config("spark.sql.adaptive.enabled", str(spark_conf["adaptive_enabled"]).lower())
    .config("spark.jars.packages", ",".join(spark_conf["jars_packages"]))
    .config("spark.driver.host", "127.0.0.1")  # Fix BlockManager NullPointer
)

# Hadoop / S3 configs from config file
for k, v in spark_conf["hadoop"].items():
    builder = builder.config(f"spark.hadoop.{k}", v)

# Optional: Use environment variables for AWS credentials
builder = builder.config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID"))
builder = builder.config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY"))
builder = builder.config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_conf['region']}.amazonaws.com")
builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")

# Create SparkSession
spark = builder.getOrCreate()

# Paths
bucket = aws_conf["bucket"]
raw_path = f"s3a://{bucket}/{config['paths']['raw_orders']}"
clean_path = f"s3a://{bucket}/{config['paths']['clean_orders']}"

# Read raw orders
df = spark.read.option("header", "true").csv(raw_path)

# Clean data: remove null amounts
df_clean = df.filter(col("amount").isNotNull())

# Write cleaned data to S3 in parquet partitioned by order_date
df_clean.write.mode("overwrite") \
    .partitionBy(spark_conf["partition_column"]) \
    .parquet(clean_path)

# Stop Spark
spark.stop()
