from pyspark.sql.types import (
    StructType,
    StructField,
    FloatType,
    StringType,
    LongType,
    IntegerType,
    DoubleType,
)
from pyspark.sql.functions import (
    split,
    regexp_replace,
    current_date,
    unix_timestamp,
    lit,
    current_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    FloatType,
    StringType,
    LongType,
    IntegerType,
    DecimalType,
)
from pyspark.sql.functions import col, from_json, struct, to_json
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import PipelineModel

import pandas as pd
import pickle
import json
import time
import os

# os.environ[
#     "PYSPARK_SUBMIT_ARGS"
# ] = "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.postgresql:postgresql:42.1.1 pyspark-shell"

APP_NAME = os.getenv("APP_NAME", "spark-streaming-app")
MASTER = os.getenv("MASTER", "local[*]")

KAFKA_SERVERS = os.getenv(
    "KAFKA_SERVERS", "localhost:9092"
)  # "kafka-cluster-kafka-bootstrap:9092"
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "to_predict")

if __name__ == "__main__":

    spark = SparkSession.builder.appName("APP_NAME").master(MASTER).getOrCreate()

    # Reading from Kafka topic
    df_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # Defining schema
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("hour", IntegerType(), True),
            StructField("C1", IntegerType(), True),
            StructField("banner_pos", IntegerType(), True),
            StructField("site_id", StringType(), True),
            StructField("site_domain", StringType(), True),
            StructField("site_category", StringType(), True),
            StructField("app_id", StringType(), True),
            StructField("app_domain", StringType(), True),
            StructField("app_category", StringType(), True),
            StructField("device_id", StringType(), True),
            StructField("device_ip", StringType(), True),
            StructField("device_model", StringType(), True),
            StructField("device_type", IntegerType(), True),
            StructField("device_conn_type", IntegerType(), True),
            StructField("C14", IntegerType(), True),
            StructField("C15", IntegerType(), True),
            StructField("C16", IntegerType(), True),
            StructField("C17", IntegerType(), True),
            StructField("C18", IntegerType(), True),
            StructField("C19", IntegerType(), True),
            StructField("C20", IntegerType(), True),
            StructField("C21", IntegerType(), True),
        ]
    )

    # Parsing Kafka message
    df = (
        df_raw.selectExpr("CAST(value AS STRING)")
        .select(from_json("value", schema).alias("data"))
        .select("data.*")
    )

    df = df.withColumn("id", df.id.cast(DecimalType(38, 0)))

    # Reading machine learning model
    pipelineModel = PipelineModel.load("model/spark-logistic-regression-model")

    # Transforming the data
    results = pipelineModel.transform(df)

    results = results.withColumn("processed_at", current_timestamp())

    results = (
        results.withColumn("probability", results["probability"].cast("String"))
        .withColumn(
            "probabilityre",
            split(regexp_replace("probability", "^\[|\]", ""), ",")[1].cast(
                DoubleType()
            ),
        )
        .select("id", "probabilityre", "processed_at")
        .withColumnRenamed("probabilityre", "probability")
    )

    results_kafka = results.select(
        to_json(struct("id", "probability", "processed_at")).alias("value")
    )

    results_postgres = results.select("id", "probability", "processed_at")

    def foreach_batch_function(df, epoch_id):

        print(df.toPandas())

        df.write.format("jdbc").option(
            "url", "jdbc:postgresql://localhost:5432/postgres"
        ).option("driver", "org.postgresql.Driver").option(
            "dbtable", "predictions"
        ).option(
            "user", "postgres"
        ).option(
            "password", "postgres"
        ).mode(
            "append"
        ).save()

    # write_to_postgres =
    results_postgres.writeStream.foreachBatch(foreach_batch_function).option(
        "checkpointLocation", "/home/app/checkpointLocation"
    ).outputMode("update").start()

    # .awaitTermination()

    # write_to_postgres.awaitTermination()

    # Write to Kafka topic
    # results_kafka.writeStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", "localhost:9092") \
    #     .option("topic", "predictions") \
    #     .option("checkpointLocation", "/home/kayser/temp") \
    #     .outputMode("Append") \
    #     .start()
    #     # .awaitTermination()

    spark.streams.awaitAnyTermination()
