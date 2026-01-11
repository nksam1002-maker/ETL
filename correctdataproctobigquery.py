from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType
)
import sys

# ----------------------------------------------------
# 1. Spark Session (Dataproc)
# ----------------------------------------------------
spark = SparkSession.builder \
    .appName("Sales_ETL_To_BigQuery") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ----------------------------------------------------
# 2. GCS Paths
# ----------------------------------------------------
SOURCE_PATH = "gs://sales-raw-bucket-delta/input/*.csv"
FAILED_PATH = "gs://sales-raw-bucket-delta/failed/"
BQ_TEMP_BUCKET = "sales-processed-bucket-delta"   # ✅ bucket ONLY

# ----------------------------------------------------
# 3. BigQuery Config
# ----------------------------------------------------
BQ_PROJECT = "delta-discovery-476909-p7"
BQ_DATASET = "sales_ds"
BQ_TABLE = "sales_orders"

# ----------------------------------------------------
# 4. CSV Schema
# ----------------------------------------------------
schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("amount", DoubleType(), True)
])

try:
    # ------------------------------------------------
    # 5. Read CSV from GCS
    # ------------------------------------------------
    df = spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv(SOURCE_PATH)

    # ------------------------------------------------
    # 6. Convert order_date to DATE
    # ------------------------------------------------
    df = df.withColumn(
        "order_date",
        to_date(col("order_date"), "yyyy-MM-dd")
    )

    # ------------------------------------------------
    # 7. Data Validation
    # ------------------------------------------------
    invalid_df = df.filter(
        col("order_id").isNull() |
        col("customer_id").isNull() |
        col("order_date").isNull() |
        col("amount").isNull()
    )

    valid_df = df.filter(
        col("order_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("order_date").isNotNull() &
        col("amount").isNotNull()
    )

    # ------------------------------------------------
    # 8. Deduplicate
    # ------------------------------------------------
    final_df = valid_df.dropDuplicates(
        ["order_id", "customer_id"]
    )

    # ------------------------------------------------
    # 9. Write to BigQuery
    # ------------------------------------------------
    final_df.write \
        .format("bigquery") \
        .option(
            "table",
            f"{BQ_PROJECT}:{BQ_DATASET}.{BQ_TABLE}"
        ) \
        .option("temporaryGcsBucket", BQ_TEMP_BUCKET) \
        .option("materializationDataset", BQ_DATASET) \
        .mode("append") \
        .save()

    # ------------------------------------------------
    # 10. Write Failed Records to GCS
    # ------------------------------------------------
    if invalid_df.count() > 0:
        invalid_df.write \
            .mode("append") \
            .option("header", "true") \
            .csv(FAILED_PATH)

    print("✅ ETL Job Completed Successfully")

except Exception as e:
    print("❌ ETL Job Failed")
    print(str(e))
    sys.exit(1)

finally:
    spark.stop()
