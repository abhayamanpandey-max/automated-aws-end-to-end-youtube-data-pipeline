"""
Lambda: YouTube Data API Ingestion (Bronze Layer)
──────────────────────────────────────────────────
Triggered by EventBridge on a schedule (e.g., every 6 hours).
Pulls trending videos from the YouTube Data API for each configured region
and writes raw JSON responses to the Bronze S3 bucket.

This replaces the old "download from Kaggle and aws s3 cp" workflow
with a real, automated, live data ingestion pipeline.

Environment Variables:
    YOUTUBE_API_KEY       — Google API key with YouTube Data API v3 enabled
    S3_BUCKET_BRONZE      — Target S3 bucket for raw data
    YOUTUBE_REGIONS       — Comma-separated region codes (default: US,GB,CA,...)
    SNS_ALERT_TOPIC_ARN   — SNS topic for failure alerts
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql import functions as F
from pyspark.sql.window import Window

"""
Glue Job: Silver → Gold (Analytics Aggregations)
─────────────────────────────────────────────────
Reads cleansed statistics and reference data from Silver,
joins them, and produces business-level aggregations in the Gold layer.

Gold layer tables are optimized for analytics queries in Athena/QuickSight.

Gold tables produced:
  1. trending_analytics   — Daily trending summaries per region
  2. channel_analytics    — Channel performance metrics
  3. category_analytics   — Category-level trends over time

Job Parameters:
    --JOB_NAME              — Glue job name
    --silver_database       — Silver Glue catalog database
    --gold_bucket           — Gold S3 bucket
    --gold_database         — Gold Glue catalog database
"""

# ── Job Setup ────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "silver_database",
    "gold_bucket",
    "gold_database",
])
sc = SparkContext()
glueContext = GlueContext(sc)

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, explode, lit

# Initialize Glue and Spark Contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session # Securely sets up the data reader engine
job = Job(glueContext)

# Define your bucket and paths directly
input_path = "s3://abhayytdatabuket/youtube/raw_statistics/*/*/*/*.json"
output_path = "s3://abhayytdatabuket/youtube/cleansed_statistics/"

print("Starting Glue Job: Reading raw JSON data directly from S3...")
# Fixed line using the correct 'spark' session reader
raw_df = spark.read.json(input_path)

# 1. Break open the nested 'items' array from the YouTube API response
exploded_df = raw_df.withColumn("item", explode(col("items")))

# 2. Extract and flatten only the specific data columns we need for analytics
clean_df = exploded_df.select(
    col("item.id").alias("video_id"),
    col("item.snippet.title").alias("title"),
    col("item.snippet.channelTitle").alias("channel_title"),
    col("item.snippet.categoryId").alias("category_id"),
    col("item.snippet.publishedAt").alias("published_at"),
    col("item.statistics.viewCount").cast("bigint").alias("view_count"),
    col("item.statistics.likeCount").cast("bigint").alias("like_count"),
    col("item.statistics.commentCount").cast("bigint").alias("comment_count")
)

print("Data flattening complete. Sample data structure:")
clean_df.printSchema()

# 3. Write the polished data back to S3 in optimized Parquet format
print(f"Writing cleansed Parquet data to: {output_path}")
clean_df.write \
    .mode("overwrite") \
    .parquet(output_path)

print("Glue Job finished successfully!")
job.commit()



