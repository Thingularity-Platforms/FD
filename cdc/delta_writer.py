from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from delta.tables import DeltaTable
from config import DELTA_PATH


def upsert_to_delta(batch_df: DataFrame, batch_id: int) -> None:
    spark = SparkSession.getActiveSession()

    batch_df = batch_df.filter(col("op").isNotNull())
    if batch_df.count() == 0:
        return

    upserts = batch_df.filter(col("op").isin("c", "u", "r")).drop("op")
    deletes  = batch_df.filter(col("op") == "d")

    upsert_count = upserts.count()
    delete_count = deletes.count()

    if DeltaTable.isDeltaTable(spark, DELTA_PATH):
        delta_table = DeltaTable.forPath(spark, DELTA_PATH)

        if upsert_count > 0:
            delta_table.alias("target").merge(
                upserts.alias("source"),
                "target.id = source.id"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()

        if delete_count > 0:
            delete_ids = [row.id for row in deletes.select("id").collect()]
            delta_table.delete(col("id").isin(delete_ids))

    else:
        # First run — bootstrap the Delta table
        if upsert_count > 0:
            upserts.write \
                .format("delta") \
                .mode("overwrite") \
                .save(DELTA_PATH)

    print(f"[Batch {batch_id}] Upserts: {upsert_count} | Deletes: {delete_count}")