from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, get_json_object
from config import KAFKA_BROKER, KAFKA_TOPIC


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()


def parse_debezium(df: DataFrame) -> DataFrame:
    """
    Debezium envelope: { "payload": { "before": {}, "after": {}, "op": "c/u/r/d" } }
    op: c = insert, u = update, r = snapshot read, d = delete
    """
    value = col("value").cast("string")
    return df.select(
        get_json_object(value, "$.payload.op").alias("op"),
        get_json_object(value, "$.payload.after.id").cast("int").alias("id"),
        get_json_object(value, "$.payload.after.customer_name").alias("customer_name"),
        get_json_object(value, "$.payload.after.amount").cast("double").alias("amount"),
        get_json_object(value, "$.payload.after.status").alias("status"),
        get_json_object(value, "$.payload.after.updated_at").alias("updated_at")
    )