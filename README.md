# POC - deltalake

## Launch python file

```
spark-submit --packages io.delta:delta-core_2.12:0.8.0 --conf "spark.jars=postgresql-42.2.19.jar" group_fields.py
```

## Launch interactive shell

```
pyspark --packages io.delta:delta-core_2.12:0.8.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --conf "spark.jars=postgresql-42.2.19.jar"
```
