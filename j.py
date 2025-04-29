import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from dotenv import load_dotenv

load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")

# Initialize Spark Session with PostgreSQL JDBC driver
spark = SparkSession.builder \
    .appName("YouTube Data Transformation") \
    .config("spark.jars", "/path/to/postgresql-driver.jar") \
    .getOrCreate()

# Read raw JSON data
df_raw = spark.read.option("multiline", "true").json("/path/to/youtube_videos_raw.json")
df_raw.createOrReplaceTempView("youtube_videos_raw")

# Basic transformation - type conversion and filtering
sql_query = """
    SELECT
        videoId,
        title,
        publishedAt,
        CAST(viewCount AS INT) AS viewCount,
        CAST(likeCount AS INT) AS likeCount,
        CAST(commentCount AS INT) AS commentCount,
        extractionTimestamp
    FROM youtube_videos_raw
    WHERE viewCount IS NOT NULL
    ORDER BY viewCount DESC
"""
df_transformed = spark.sql(sql_query)

# Register transformed DataFrame for enrichment
df_transformed.createOrReplaceTempView("youtube_transformed")

# Enhanced enrichment - adding temporal dimensions and performance classification
enriched_query = """
    SELECT
        *,
        YEAR(TO_TIMESTAMP(publishedAt)) AS year,
        MONTH(TO_TIMESTAMP(publishedAt)) AS month,
        DAY(TO_TIMESTAMP(publishedAt)) AS day_of_month,
        DATE_FORMAT(TO_TIMESTAMP(publishedAt), 'EEEE') AS day_of_week,
        HOUR(TO_TIMESTAMP(publishedAt)) AS hour,
        WEEKOFYEAR(TO_TIMESTAMP(publishedAt)) AS week,
        CASE
            WHEN viewCount > 100000 THEN 'viral'
            WHEN viewCount > 5000 THEN 'high'
            ELSE 'normal'
        END AS performance_class
    FROM youtube_transformed
"""
df_enriched = spark.sql(enriched_query)
df_enriched.show()

# Configure PostgreSQL connection
jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
connection_properties = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# Save enriched data to PostgreSQL
df_enriched.write \
    .jdbc(
        url=jdbc_url,
        table="dataengineering.youtube_videos_enriched",
        mode="append",
        properties=connection_properties
    )

print("✅ Enriched data successfully appended to PostgreSQL.")
spark.stop()