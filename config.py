import os

# Python env
os.environ['PYSPARK_PYTHON'] = r'C:\Users\yogen\AppData\Local\Programs\Python\Python310\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\yogen\AppData\Local\Programs\Python\Python310\python.exe'

# Kafka
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "cdc.sourcedb.orders"

# Bronze Paths
DELTA_PATH = "./delta/orders"
CHECKPOINT_PATH = "./checkpoints/orders"

# Silver paths
SILVER_PATH = "./silver_data/orders"
SILVER_REJECTED_PATH = "./silver_data/orders_rejected"
SILVER_CHECKPOINT_PATH = "./checkpoints/silver/orders"

# Spark packages
SPARK_PACKAGES = (
    "io.delta:delta-core_2.12:2.4.0,"
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
)

# Streaming
TRIGGER_INTERVAL = "10 seconds"