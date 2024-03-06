from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DateType
from pyspark.sql.functions import col, count, when, split, lower, max as sparkMax
import os

# Create a SparkSession
spark = SparkSession.builder \
    .appName("GitHubPRProcessing") \
    .getOrCreate()

# Define the schema for the JSON data
schema3= StructType([
    StructField("url", StringType(), True),
    StructField("id", IntegerType(), True),
    StructField("node_id", StringType(), True),
    StructField("html_url", StringType(), True),
    StructField("diff_url", StringType(), True),
    StructField("patch_url", StringType(), True),
    StructField("issue_url", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("state", StringType(), True),
    StructField("locked", BooleanType(), True),
    StructField("title", StringType(), True),
    StructField("user", StructType([
        StructField("login", StringType(), True)
    ])),
    StructField("created_at", DateType(), True),
    StructField("updated_at", DateType(), True),
    StructField("closed_at", DateType(), True),
    StructField("merged_at", DateType(), True),
    StructField("merge_commit_sha", StringType(), True),
    StructField("base", StructType([
        StructField("repo", StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("full_name", StringType(), True),
            StructField("owner", StructType([
                StructField("login", StringType(), True)
            ]))
        ]))
    ]))
])

def transform_and_aggregate(json_path):
    # Read the JSON data and perform transformation
    transformed_df = spark.read.json(json_path, schema3).groupBy(
        split(col("base.repo.full_name"), "/")[0].alias("organization_name"),  # Splitting organization_name by "/"
        col("base.repo.id").alias("repository_id"),
        col("base.repo.name").alias("repository_name"),
        col("base.repo.owner.login").alias("repository_owner")
    ).agg(
        count("*").alias("num_prs"),
        count(when(col("state") == "merged", True)).alias("num_prs_merged"),
        sparkMax(col("merged_at")).alias("merged_at")
    ).withColumn(
        "is_compliant",
        (col("num_prs") == col("num_prs_merged")) &
        (lower(col("repository_owner")).contains("scytale"))
    )

    return transformed_df

# Load and transform JSON files into Spark DataFrame by calling transform_and_aggregate function
json_files = ["Files/json_files/" + file for file in os.listdir("json_files") if file.endswith(".json")]
final_df = transform_and_aggregate(json_files)

# Write final_df DataFrame to a Parquet file
final_df.write.mode("overwrite").parquet("Files/parquet_files")
