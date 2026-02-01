import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from utils.config_loader import load_config

# Load config
config = load_config("dev")
spark_conf = config["spark"]
aws_conf = config["aws"]

# COMPREHENSIVE Spark configuration to eliminate all errors
builder = (
    SparkSession.builder
    .appName(spark_conf["app_name"])
    .master("local[1]")  # Use local[1] for single core execution
    .config("spark.jars.packages", ",".join(spark_conf["jars_packages"]))

    # CRITICAL: Configurations to disable block manager and executor heartbeats
    .config("spark.driver.blockManager.port", "0")
    .config("spark.blockManager.port", "0")
    .config("spark.executor.instances", "1")
    .config("spark.executor.cores", "1")

    # Disable dynamic allocation and heartbeats
    .config("spark.dynamicAllocation.enabled", "false")
    .config("spark.shuffle.service.enabled", "false")
    .config("spark.executor.heartbeat.enabled", "false")

    # Memory settings optimized for local execution
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")

    # Disable UI and metrics to reduce overhead
    .config("spark.ui.enabled", "false")
    .config("spark.ui.showConsoleProgress", "false")
    .config("spark.metrics.conf", "/dev/null")

    # Hadoop/S3 configurations
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID"))
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_conf['region']}.amazonaws.com")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # Disable all metrics logging
    .config("spark.hadoop.fs.s3a.metrics.enabled", "false")
)

# Add any additional Hadoop configs from your config file
for k, v in spark_conf.get("hadoop", {}).items():
    builder = builder.config(f"spark.hadoop.{k}", v)

# Create Spark session
spark = builder.getOrCreate()

# EXTREME log suppression - suppress EVERYTHING except critical errors
spark.sparkContext.setLogLevel("OFF")  # Changed from ERROR to OFF

# Also suppress Python logging
import logging

logging.getLogger("py4j").setLevel(logging.CRITICAL)
logging.getLogger("pyspark").setLevel(logging.CRITICAL)

# Paths
bucket = aws_conf["bucket"]
raw_path = f"s3a://{bucket}/{config['paths']['raw_orders']}"
clean_path = f"s3a://{bucket}/{config['paths']['clean_orders']}"

# Read & clean data
print(f"📖 Reading data from: {raw_path}")
try:
    df = spark.read.option("header", "true").csv(raw_path)
    total_records = df.count()
    print(f"✅ Total records read: {total_records:,}")

    # Clean data - remove null amounts
    df_clean = df.filter(col("amount").isNotNull())
    cleaned_records = df_clean.count()
    print(f"🧹 Records after cleaning: {cleaned_records:,}")

    # Write cleaned data
    print(f"💾 Writing cleaned data to: {clean_path}")
    partition_column = spark_conf.get("partition_column", "date")

    df_clean.write.mode("overwrite") \
        .partitionBy(partition_column) \
        .parquet(clean_path)

    print("🎉 Data cleaning and writing completed successfully!")
    print(f"📊 Summary: {total_records:,} total → {cleaned_records:,} clean records")

except Exception as e:
    print(f"❌ Error during processing: {str(e)}")
    sys.exit(1)

finally:
    spark.stop()
    print("🔌 Spark session stopped cleanly")