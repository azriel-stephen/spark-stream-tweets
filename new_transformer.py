#!/usr/bin/python3

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Stream_Process") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "tweets") \
    .load()

lines = df.selectExpr("CAST(value AS STRING)").alias("tweet")

transform = lines.select("tweet",
                         len(lines.tweet.split()).alias("words"),
                         len(lines.tweet).alias("length"))

query = transform.writeStream.format("console") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()
