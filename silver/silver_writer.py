from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from delta.tables import DeltaTable
from silver.cleansing import standardize, deduplicate, split_valid_rejected
from config import SILVER_PATH, SILVER_REJECTED_PATH


def _upsert_valid(spark: SparkSession, valid_df: DataFrame) -> int:
    count = valid_df.count()
    if count == 0:
        return 0

    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        DeltaTable.forPath(spark, SILVER_PATH).alias("target").merge(
            valid_df.alias("source"),
            "target.id = source.id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    else:
        valid_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(SILVER_PATH)

    return count


def _append_rejected(spark: SparkSession, rejected_df: DataFrame) -> int:
    count = rejected_df.count()
    if count == 0:
        return 0

    # Rejected records are always appended — full audit history
    rejected_df.write \
        .format("delta") \
        .mode("append") \
        .save(SILVER_REJECTED_PATH)

    return count


def process_silver_batch(batch_df: DataFrame, batch_id: int) -> None:
    spark = SparkSession.getActiveSession()

    batch_df = batch_df.cache()

    if batch_df.count() == 0:
        batch_df.unpersist()
        return

    # Step 1 — standardize types and strings
    standardized = standardize(batch_df)

    # Step 2 — deduplicate within batch
    deduped = deduplicate(standardized)

    # Step 3 — split into valid and rejected
    valid_df, rejected_df = split_valid_rejected(deduped)

    # Step 4 — write both
    valid_count    = _upsert_valid(spark, valid_df)
    rejected_count = _append_rejected(spark, rejected_df)

    batch_df.unpersist()

    print(
        f"[Silver | Batch {batch_id}] "
        f"Valid: {valid_count} | Rejected: {rejected_count}"
    )