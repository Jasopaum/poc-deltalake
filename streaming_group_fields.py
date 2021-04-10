from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import shutil


IN_STREAM_LOCATION = "/tmp/delta/events"
OUT_LOCATION = "/tmp/delta/grouped_events"
CHECKPOINT_LOCATION = "/tmp/delta/grouped_events"

# Clear any previous runs
try:
    shutil.rmtree(IN_STREAM_LOCATION)
    shutil.rmtree(OUT_LOCATION)
    shutil.rmtree(CHECKPOINT_LOCATION)
except:
    pass

spark = (
    SparkSession.builder.appName("quickstart")
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


def group_and_agg(df):
    tmp_df = df.groupby("form_id").agg(F.min("date").alias("first_date"), F.min("time").alias("first_time"))
    # FIXME is it the best way to build a timestamp?
    dt_tm = F.concat(tmp_df.first_date, F.lit(" "), F.split(tmp_df.first_time, " ")[1])
    tmp_df = tmp_df.withColumn("timestamp_field", dt_tm.cast("timestamp"))
    return tmp_df.drop("first_date", "first_time")


# Load init data to delta
grouped_df = group_and_agg(df)
grouped_df.write.format("delta").save(OUT_LOCATION)

# FIXME: hack for the schema
df.write.format("delta").save(IN_STREAM_LOCATION)

# Start listening to stream
events = spark.readStream.format("delta").load(IN_STREAM_LOCATION)
grouped_events = group_and_agg(events)

# Write stream
query = (
    grouped_events.writeStream.format("delta")
    .outputMode("complete")
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .start(OUT_LOCATION)
)

query.awaitTermination()
