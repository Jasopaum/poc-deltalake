from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = (
    SparkSession.builder.appName("quickstart")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# Read DB
df = spark.read.jdbc(
    "jdbc:postgresql://localhost:5432/delta-source",
    "fields",
    properties={"user": "delta", "password": "delta", "driver": "org.postgresql.Driver"},
)

# Hacky but ok
grouped_df = df.groupby("form_id").agg(F.min("date"), F.min("time"))

# Write to DB
grouped_df.write.jdbc(
    url="jdbc:postgresql://localhost:5432/delta-source",
    table="grouped_fields",
    mode="overwrite",
    properties={"user": "delta", "password": "delta", "driver": "org.postgresql.Driver"},
)
