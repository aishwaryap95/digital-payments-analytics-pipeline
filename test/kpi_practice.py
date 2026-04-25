from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.utils.spark_session import *
from src.storage.read.payment_reader import *

spark = spark_session()

df = spark.read.parquet("s3a://de-aisha-project-2/data_marts/transaction_performance_mart/")

# KPI on transaction_performance_mart
# 1. daily transaction volume
df.groupBy(date_format(col("transaction_timestamp"), "yyyy-MM-dd").alias("Day")) \
    .agg(count("transaction_id").alias("daily_transaction_volume")) \
    .orderBy("Day") \
    .show()

# 2. success rate - successful transactions, percentage
df.agg(
    sum(
        when(lower(col("transaction_status")) == "success",1)
        .otherwise(0)
    ).alias("success_cnt"),
    count("*").alias("total_cnt")
).alias("transactions_success_rate"
).show()

df.agg((sum(
    when(col("transaction_status") == "SUCCESS",1)
    .otherwise(0)
) * 100.0 / count("*")).alias("success_rate")) \
.show()
