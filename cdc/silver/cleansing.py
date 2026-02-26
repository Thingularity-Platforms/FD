from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, trim, when, lit, current_timestamp, to_timestamp
)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc


VALID_STATUSES = ["NEW", "PENDING", "COMPLETED", "CANCELLED"]


def standardize(df: DataFrame) -> DataFrame:
    return df \
        .withColumn("id", col("id").cast("int")) \
        .withColumn("amount", col("amount").cast("double")) \
        .withColumn("customer_name", trim(col("customer_name"))) \
        .withColumn("status", trim(col("status")).cast("string")) \
        .withColumn("updated_at", to_timestamp(col("updated_at")))


def add_reject_reason(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "reject_reason",
        when(col("id").isNull(), "Missing id")
        .when(col("customer_name").isNull(), "Missing customer_name")
        .when(col("amount").isNull(), "Missing amount")
        .when(col("amount") <= 0, "Invalid amount — must be > 0")
        .when(col("status").isNull(), "Missing status")
        .when(
            ~col("status").isin(VALID_STATUSES),
            "Invalid status value"
        )
        .otherwise(None)
    )


def deduplicate(df: DataFrame) -> DataFrame:
    window = Window.partitionBy("id").orderBy(desc("updated_at"))
    return df \
        .withColumn("_rn", row_number().over(window)) \
        .filter(col("_rn") == 1) \
        .drop("_rn")


def split_valid_rejected(df: DataFrame):
    df_with_reason = add_reject_reason(df).withColumn(
        "ingested_at", current_timestamp()
    )

    valid_df = df_with_reason \
        .filter(col("reject_reason").isNull()) \
        .drop("reject_reason")

    rejected_df = df_with_reason \
        .filter(col("reject_reason").isNotNull())

    return valid_df, rejected_df