from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

import os

os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell"


MASTER = os.getenv("MASTER", "local")
NAMESPACE = os.getenv("NAMESPACE")

spark = SparkSession.builder.appName("Spark Streaming App").master(MASTER).getOrCreate()
print(spark)

# sparkConf = SparkConf()
# sparkConf.setMaster("k8s://kubernetes.default.svc.cluster.local")
# sparkConf.setAppName("Spark Streaming App")

# sparkConf.set(
#     "spark.kubernetes.container.image", "gcr.io/spark-operator/spark-py:v3.1.1-hadoop3"
# )
# sparkConf.set("spark.kubernetes.namespace", NAMESPACE)

# # # sparkConf.set("spark.submit.deployMode", "cluster")

# # sparkConf.set("spark.executor.instances", "3")
# # sparkConf.set("spark.executor.memory", "1g")
# # sparkConf.set("spark.executor.cores", "1")

# # # sparkConf.set("spark.driver.blockManager.port", "7777")
# # # sparkConf.set("spark.driver.port", "2222")
# # # sparkConf.set("spark.driver.host", "jupyter.spark.svc.cluster.local")
# # # sparkConf.set("spark.driver.bindAddress", "0.0.0.0")

# # spark = SparkSession.builder.config(conf=sparkConf).getOrCreate()

# # spark


#################################################

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka-cluster-kafka-bootstrap:9092")
    .option("subscribe", "app_messages")
    .option("startingOffsets", "latest")
    .load()
)
