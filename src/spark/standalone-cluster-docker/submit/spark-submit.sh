#!/bin/bash

/spark/bin/spark-submit \
    --master ${SPARK_MASTER_URL} \
    --jars "/opt/spark-jars/postgresql-42.2.19.jar" \
    --conf spark.env.DB_URI=${DB_URI} \
    --conf spark.env.DB_USER=${DB_USER} \
    --conf spark.env.DB_PASSWORD=${DB_PASSWORD} \
    ${SPARK_APPLICATION_JAR_LOCATION} ${SPARK_APPLICATION_ARGS}
