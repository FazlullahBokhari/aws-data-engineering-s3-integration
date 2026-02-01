import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from utils.config_loader import load_config

# Load config
config = load_config("dev")
spark_conf = config["spark"]
aws_conf = config["aws"]

# SIMPLIFIED Spark session builder - minimal configuration
builder = (
    SparkSession.builder
    .appName(spark_conf["app_name"])
    .master("local[*]")  # Force local[*] to avoid any master issues
    .config("spark.sql.shuffle.partitions", spark_conf["shuffle_partitions"])
    .config("spark.jars.packages", ",".join(spark_conf["jars_packages"]))
    # Minimal network configs for local mode
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "localhost")
)

# Hadoop / S3 configs
for k, v in spark_conf["hadoop"].items():
    builder = builder.config(f"spark.hadoop.{k}", v)

builder = builder.config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID"))
builder = builder.config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY"))
builder = builder.config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_conf['region']}.amazonaws.com")
builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")

spark = builder.getOrCreate()

# Suppress Spark INFO logs - keeping only ERROR logs
spark.sparkContext.setLogLevel("ERROR")

# Paths
bucket = aws_conf["bucket"]
raw_path = f"s3a://{bucket}/{config['paths']['raw_orders']}"
clean_path = f"s3a://{bucket}/{config['paths']['clean_orders']}"

# Read & clean data
print(f"Reading data from: {raw_path}")
df = spark.read.option("header", "true").csv(raw_path)
print(f"Total records read: {df.count()}")

# Clean data - remove null amounts
df_clean = df.filter(col("amount").isNotNull())
print(f"Records after cleaning: {df_clean.count()}")

# Write cleaned data
print(f"Writing cleaned data to: {clean_path}")
partition_column = spark_conf.get("partition_column", "date")  # Provide default if not in config
df_clean.write.mode("overwrite") \
    .partitionBy(partition_column) \
    .parquet(clean_path)

print("Data cleaning and writing completed successfully!")

spark.stop()