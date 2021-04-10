from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, LongType, StructField, StructType, TimestampType

STREAM_LOCATION = "/tmp/delta/events"

spark = (
    SparkSession.builder.appName("quickstart")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .getOrCreate()
)


def emit_rows(rows, schema):
    spark.createDataFrame(rows, schema).write.format("delta").mode("append").save(STREAM_LOCATION)


schema = StructType(
    [
        StructField("id", LongType(), False),
        StructField("form_id", LongType(), False),
        StructField("date", DateType(), True),
        StructField("time", TimestampType(), True),
    ]
)


emit_rows(
    [
        {"id": 1, "form_id": 7, "date": datetime.strptime("05-07-2005", "%d-%m-%Y"), "time": None},
        {"id": 1, "form_id": 7, "date": None, "time": datetime.strptime("07:07", "%H:%M")},
    ],
    schema=schema,
)
