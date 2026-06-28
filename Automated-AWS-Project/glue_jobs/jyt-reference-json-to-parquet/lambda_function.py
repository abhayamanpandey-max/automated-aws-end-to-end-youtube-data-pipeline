

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, explode

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Paths
input_path = "s3://abhayytdatabuket/youtube/raw_statistics_reference/*.json"
output_path = "s3://abhayytdatabuket/youtube/cleansed_category_reference/"

# Read nested reference JSON using multi-line configuration
df = spark.read.option("multiLine", True).json(input_path)

# Explode items array to extract ID and Title
exploded_df = df.withColumn("item", explode(col("items")))

clean_ref_df = exploded_df.select(
    col("item.id").cast("int").alias("category_id"),
    col("item.snippet.title").alias("category_name")
)

# Overwrite to clean silver reference folder as Parquet
clean_ref_df.write.mode("overwrite").parquet(output_path)
job.commit()






