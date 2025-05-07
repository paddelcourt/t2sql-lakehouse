from pyspark.sql.types import StructType, StructField, LongType, StringType, ArrayType
from pyspark.sql import SparkSession
import argparse

# Initialize the SparkSession
spark = SparkSession.builder \
    .appName("Iceberg Ingestion Pipeline") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "s3a://goodwiki-bucket/warehouse/") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.region", "us-east-1") \
    .config(
            "spark.hadoop.fs.s3a.impl", \
            "org.apache.hadoop.fs.s3a.S3AFileSystem", \
        ).config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.7.0," + \
            "org.apache.hadoop:hadoop-aws:3.3.2," + \
            "org.postgresql:postgresql:42.7.3"
            
        ) \
    .getOrCreate() 

print(f"Active Spark Version: {spark.version}")

def ingest_schema(path):
    df = spark.read.parquet(path)
    df.printSchema()
    schema = StructType([
        StructField("pageid", LongType(), True),
        StructField("title", StringType(), True),
        StructField("revid", LongType(), True),
        StructField("description", StringType(), True),
        StructField("categories", ArrayType(StringType()), True),
        StructField("markdown", StringType(), True)
    ])

    if df.rdd.isEmpty():
        df = spark.createDataFrame([], schema)
    df.writeTo("local.goodwiki.ingestion_etl").createOrReplace()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        type=str,
        help="HDFS input",
        default="s3a://goodwiki-bucket/raw/goodwiki.parquet",
    )
    args = parser.parse_args()
    ingest_schema(args.input)

    