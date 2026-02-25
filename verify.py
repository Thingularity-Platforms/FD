import config
from spark_session import get_spark_session
from config import DELTA_PATH, SILVER_PATH, SILVER_REJECTED_PATH

spark = get_spark_session(app_name="Verify-Silver")

print("\n========== BRONZE - RAW RECORDS ==========")
spark.read.format("delta").load(DELTA_PATH).show(truncate=False)

print("\n========== SILVER - VALID RECORDS ==========")
spark.read.format("delta").load(SILVER_PATH).show(truncate=False)

print("\n========== SILVER - REJECTED RECORDS ==========")
spark.read.format("delta").load(SILVER_REJECTED_PATH).show(truncate=False)

spark.stop()