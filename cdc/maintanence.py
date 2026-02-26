import config
from spark_session import get_spark_session
from delta.tables import DeltaTable
from config import DELTA_PATH, SILVER_PATH, SILVER_REJECTED_PATH


def run_maintenance(spark, path: str, z_order_col: str, label: str):
    if not DeltaTable.isDeltaTable(spark, path):
        print(f"[{label}] Not a Delta table yet, skipping.")
        return

    delta_table = DeltaTable.forPath(spark, path)

    print(f"[{label}] Running OPTIMIZE + ZORDER by '{z_order_col}'...")
    delta_table.optimize().executeZOrderBy(z_order_col)
    print(f"[{label}] OPTIMIZE done.")

    print(f"[{label}] Running VACUUM (retain 168 hours)...")
    delta_table.vacuum(retentionHours=168)
    print(f"[{label}] VACUUM done.")

    print(f"[{label}] History:")
    delta_table.history(3).select("version", "timestamp", "operation").show(truncate=False)


def main():
    spark = get_spark_session(app_name="CDC-Maintenance")

    # Bronze — Z-order by id (merge key)
    run_maintenance(spark, DELTA_PATH, "id", "Bronze")

    # Silver valid — Z-order by id (merge key)
    run_maintenance(spark, SILVER_PATH, "id", "Silver-Valid")

    # Silver rejected — Z-order by ingested_at (mostly queried by time)
    run_maintenance(spark, SILVER_REJECTED_PATH, "ingested_at", "Silver-Rejected")

    spark.stop()
    print("\nMaintenance complete.")


if __name__ == "__main__":
    main()