from pyspark.sql.types import *
from pyspark.sql.functions import *
from src.utils.logging_config import logger

# daily transaction volume
# success rate
# revenue by channel
# city-wise payment amount
# monthly success rate by channel

def run_transaction_kpis(df):

    logger.info("=============== KPI 1 : Daily Transaction Volume ===============")

    df.groupBy(date_format(col("transaction_timestamp"), "yyyy-MM-dd").alias("Day")) \
        .agg(count("transaction_id").alias("daily_transaction_volume")) \
        .orderBy("Day") \
        .show(truncate=False)


    logger.info("=============== KPI 2 : Success Rate ===============")

    df.agg((sum(
        when(col("transaction_status") == "SUCCESS", 1)
        .otherwise(0)
    ) * 100.0 / count("*")).alias("success_rate")) \
        .show()


    logger.info("=============== KPI 3 : Revenue by channel ===============")

    df.filter(col("transaction_status") == "SUCCESS") \
        .groupBy("channel_type") \
        .agg(round(sum("processing_fee"), 2).alias("channel_revenue")) \
        .orderBy(col("channel_revenue").desc()) \
        .show(truncate=False)


    logger.info("=============== KPI 4 : city-wise payment amount ===============")

    df.filter(col("transaction_status") == "SUCCESS") \
        .groupBy("city").agg(
        count("transaction_id").alias("txn_count"),
        round(sum(col("transaction_amount")), 2).alias("payment_amount"),
        round(avg(col("transaction_amount")), 2).alias("avg_ticket_size")
    ) \
        .orderBy(col("payment_amount").desc()) \
        .show(truncate=False)

    # Ticket size is the average value of each transaction. It helps compare payment behavior across cities, merchants, or channels.


    logger.info("=============== KPI 5 : monthly success rate by channel ===============")
    df.groupBy(
        date_format(col("transaction_timestamp"), "yyyy-MM").alias("month"),
        col("channel_type")
    ) \
        .agg(
        round(
            (
                sum(
                    when(col("transaction_status") == "SUCCESS", 1)
                    .otherwise(0)
                ) * 100.0 / count("*")
            ),
            2
        ).alias("success_rate")
    ).orderBy("month", "channel_type") \
        .show(truncate=False)