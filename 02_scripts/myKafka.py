# DESCRIPTION: this library helps you to call functions related to kafka.

# importing libraries
from kafka.producer import KafkaProducer
from kafka.consumer import KafkaConsumer
import random
from time import sleep
import pyspark.sql.functions as fn


def my_producer(path = '../01_data/occupancy_data.csv', topic = 'test', low = 0.5, high = 1.5, limit = 0):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    rand = random.uniform(float(low), float(high))
    f = open(path, 'rt')
    for idx, line in enumerate(f):
        if idx == limit and limit != 0:
            break
        producer.send(topic, bytes(line, 'utf8'))
        sleep(rand)

from datetime import datetime

def apply_schema(df, schema):
    columns = []
    columns_schema = []
    for k, v in schema.items():
        columns.append(k)
        columns_schema.append(v)
    for i, col in enumerate(columns):
        if col in ["row", "date"]:
            df = df\
            .withColumn(col,
                        fn.regexp_replace(fn.col("array_value").getItem(i), '"', "")
                         .cast(columns_schema[i])
                       )
        elif col == "Occupancy":
            df = df\
            .withColumn(col,
                        fn.substring(fn.col("array_value").getItem(i), 0, 1)
                         .cast(columns_schema[i])
                       )
        else:
            df = df\
            .withColumn(col, fn.col("array_value").getItem(i).cast(columns_schema[i]))
    return df





