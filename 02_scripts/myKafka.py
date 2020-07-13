# DESCRIPTION: this library helps you to call functions related to kafka.

# importing libraries
from datetime import datetime
from kafka.producer import KafkaProducer
from kafka.consumer import KafkaConsumer
import random
from time import sleep
import pyspark.sql.functions as fn

## call this function to run the producer in the notebook
def my_producer(path = '../01_data/occupancy_data.csv', topic = 'test', low = 0.5, high = 1.5, limit = 0):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    rand = random.uniform(float(low), float(high))
    f = open(path, 'rt')
    for idx, line in enumerate(f):
        if idx == limit and limit != 0:
            break
        producer.send(topic, bytes(line, 'utf8'))
        sleep(rand)


## call this function to apply an scheme to the streaming dataframe
def apply_scheme(df, schema):
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

## call this function to get the difference between windows
def get_diff(df, epoch_id):
    w = Window.partitionBy().orderBy('date')
    df_w = (df.withColumn("prev_date", fn.lag("date").over(w))
              .withColumn("time_diff",
                        fn.col("date").cast("long") - fn.col("prev_date").cast("long")
                       )
            .withColumn("diff_seconds",
                         fn.col("date").cast("long") - fn.col("prev_date").cast("long"))
              .withColumn("type", fn.when(fn.col("time_diff") != 60, fn.lit("different_1_minute"))
                                .otherwise(fn.lit("1_minute"))
                      )
            )
    df_group = (df_w.where(fn.col('type') != '1_minute')
                    .groupBy('type')
                    .count())
    print(df_group.show())







