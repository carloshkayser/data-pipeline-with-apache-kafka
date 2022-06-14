---
marp: true
theme: gaia
_class: lead
title: Data Stream Processing with Apache Kafka and Spark Structured Streaming
# description: 
author: "Carlos Henrique Kayser"
paginate: true
backgroundColor: #fff
# backgroundImage: url('https://marp.app/assets/hero-background.svg')

---

<style scoped>
h1 { font-size: 1.5rem }

p { font-size: 80% }
</style>

Pontifical Catholic University of Rio Grande do Sul<br>Graduate Program in Computer Science

# Data Stream Processing with Apache Kafka<br>and Spark Structured Streaming

Carlos Henrique Kayser<br>Email: carlos.kayser@edu.pucrs.br

Scalable Data Stream Processing<br>Prof. Dr. Dalvan Jair Griebler

May 15th, 2022

---

<style> 
h1 { font-size: 1rem }

p { font-size: 80% }

li { font-size: 80% }

pre code { font-size: 50% }

img[alt~="center"] {
  display: block;
  margin: 0 auto;
}

figcaption {
  display: block;
  margin: 0 auto;
}
</style>

# Introduction

- Introduction
  - 
- Kafka
- Apache Structured Streaming
- Demonstration
- readStream and writeStream



---
# Introduction
 



---

# System proposal

The proposed solution consists of applying a machine learning model (i.e., transformations) with Apache Spark in real time on the data stream comming from Apache Kafka;    

![w:900 center](figures/system-proposal.png)













---
# Apache Kafka

- Kafka is a distributed event streaming plataform
- The data is organized into _topics_ (e.g., tweets, orders)
  - To enable parallelism, they are split into _partitions_
- A _producer_ append data/messages to _topics_
- A _consumer_ read data from _topics_
  - They also can _subscribe_ to a _topic_ and receive incoming records as they arrive

<!-- 
![w:600 center](figures/kafka-topic.png)
*Fig.1 - 4K Mountains Wallpaper* 
 -->





---

# Apache Spark Structured Streaming

- Structured Streaming is a scalable and fault-tolerant stream processing engine
- It provides an unified batch and streaming API that enables us to interact with data published to Kafka as a DataFrame
  - i.e., it is possible to use the same code for batch or streaming
- It ensures end-to-end exactly-once fault-tolerance guarantees through _checkpointing_ and _write-ahead logs_
- Structured Streaming queries are processed using a _micro-batch_ processing engine
  - i.e., _micro-batch_ starts as soon as the previous one ends






---
# Reading data from Kafka


- To following PySpark code read data from `demo` Kafka topic (*subscribe*)
- It reads the data in a streaming way (*startingOffsets*)
  - "latest" to read only the new messages
  - "earliest" to read messages messages that have not been processed

```python
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", KAFKA_HOST) \
  .option("subscribe", "demo") \
  .option("startingOffsets", "latest") \
  .load()
```




---
# Writing data to Kafka

- When writing data, Apache Spark requires a `checkpointLocation` to store all data related to the execution
  - In case of failure or shutdown, it is possible to recover the previous progress and state
- The command below write data to `demo` Kafka topic. 

```python
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", KAFKA_HOST) \
  .option("topic", "demo") \
  .option("checkpointLocation", "checkpointLocation") \
  .start()
```



---
# Writing data to not supported output sink

- Foreach Sink
  - Allows custom write logic on every row
  - Fault-tolerant: at-least-once
- ForeachBatch Sink
  - Allows arbitrary operations and custom logic on the output of each micro-batch
  - Reuse existing batch data sources
  - Write to multiple locations
  - Fault-tolerant: Depends on the implementation






---
# Writing data to not supported output sink

Example:

```python
def foreach_batch_function(df, epoch_id):

    df.write.format("jdbc")
    .option("url", "jdbc:postgresql://localhost:5432/postgres")
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", "predictions")
    .option("user", "postgres")
    .option("password", "postgres")
    .mode("append")
    .save()

df \
  .writeStream \
  .foreachBatch(foreach_batch_function) \
  .option("checkpointLocation", "checkpointLocation") \
  .start()
```






<!--
TODO
- Triggers (não incluir)
- watermark ()
- Window Operations on Event Time
- 
- https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#recovering-from-failures-with-checkpointing
-->






---
# Demonstration

---

# References

<style scoped>
p { font-size: 65% }
</style>

[1] Armbrust, Michael, et al. "Structured streaming: A declarative api for real-time applications in apache spark." _Proceedings of the 2018 International Conference on Management of Data_. 2018.

[2] 



https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#overview

https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html

https://databricks.com/blog/2017/04/26/processing-data-in-apache-kafka-with-structured-streaming-in-apache-spark-2-2.html

https://kafka.apache.org/documentation/





---



<!-- <p>
    <img src="figures/kafka-topic.png" alt style="width:50%">
</p>
<p>
    <em>image_caption</em>
</p> -->


<!-- <figure class="image">
  <img src="figures/kafka-topic.png" alt="Descirp" style="width:50%">
  <figcaption>Kafka Topic</figcaption>
</figure> -->


<!-- 
---
<style>
.image-caption {
  text-align: center;
  font-size: .8rem;
  color: light-grey;
}
</style>

![w:600 center](figures/kafka-topic.png)

<figcaption>Kafka Topic</figcaption> -->


<!-- 
---

# Apache Spark Structured Streaming

[1] -->

