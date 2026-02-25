import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from spark_session import get_spark_session
from silver.silver_writer import process_silver_batch
from config import (
    DELTA_PATH,
    SILVER_CHECKPOINT_PATH,
    TRIGGER_INTERVAL
)


def main():
    spark = get_spark_session(app_name="CDC-Silver-Layer")

    # Read Bronze Delta table as a stream
    bronze_stream = spark.readStream \
        .format("delta") \
        .load(DELTA_PATH)

    query = bronze_stream.writeStream \
        .foreachBatch(process_silver_batch) \
        .option("checkpointLocation", SILVER_CHECKPOINT_PATH) \
        .trigger(processingTime=TRIGGER_INTERVAL) \
        .start()

    print("Silver streaming job started. Reading from Bronze Delta...")
    query.awaitTermination()


if __name__ == "__main__":
    main()