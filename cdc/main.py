import config  # loads os.environ first
from spark_session import get_spark_session
from kafka_reader import read_kafka_stream, parse_debezium
from delta_writer import upsert_to_delta
from config import CHECKPOINT_PATH, TRIGGER_INTERVAL


def main():
    spark = get_spark_session()

    raw_stream    = read_kafka_stream(spark)
    parsed_stream = parse_debezium(raw_stream)

    query = parsed_stream.writeStream \
        .foreachBatch(upsert_to_delta) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .trigger(processingTime=TRIGGER_INTERVAL) \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()