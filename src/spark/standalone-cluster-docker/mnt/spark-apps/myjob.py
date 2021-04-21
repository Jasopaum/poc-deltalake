import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = (
    SparkSession.builder.master("spark://spark-master:7077")
    .appName("test_mimic")
    .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
    .getOrCreate()
)

DB_URI = spark.conf.get("spark.env.DB_URI")
DB_USER = spark.conf.get("spark.env.DB_USER")
DB_PASSWORD = spark.conf.get("spark.env.DB_PASSWORD")

df = spark.read.jdbc(
    DB_URI,
    "(SELECT row_id, subject_id, gender, dob FROM mimiciii.patients) as patients",
    properties={"user": DB_USER, "password": DB_PASSWORD, "driver": "org.postgresql.Driver"},
)

gender_df = df.withColumn(
    "fhir_gender", F.when(df["gender"] == "F", "female").when(df["gender"] == "M", "male").otherwise("unknown")
)
gender_df.write.mode("overwrite").parquet("/opt/spark-data/big_mimic/patients")
