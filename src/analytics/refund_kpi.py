from pyspark.sql.types import *
from pyspark.sql.functions import *
from src.utils.logging_config import logger

# refund rate
# top refund merchants
# refund reason trend
# customer repeat refund frequency

# .select(col("r.refund_id"),col("r.transaction_id"),col("r.customer_id"),col("r.refund_timestamp"),col("r.refund_amount")
#                      ,col("r.refund_reason"),col("r.refund_status"),col("m.merchant_id"),col("m.merchant_name"),col("m.merchant_category")
#                      ,col("c.age_group"),col("c.city"),col("c.state"),col("t.transaction_amount"),col("t.transaction_status")
#                      ,col("t.transaction_timestamp"),col("t.channel_id"),
#                      year(col("refund_timestamp")).alias("refund_year"),
#                      month(col("refund_timestamp")).alias("refund_month")
def run_refund_kpis(refund_df, tran_df):
    logger.info("=============== KPI 1 : Refund Rate ===============")

    refund_txn = refund_df.select("transaction_id").distinct().count()
    tot_success_txn = tran_df.filter(col("transaction_status") == "SUCCESS").count()

    print("refund txn: ", refund_txn)
    print("tot_success_txn: ", tot_success_txn)

    # python round() is used here, so its expecting column, not float
    refund_rate = (refund_txn / tot_success_txn) * 100
    print(f"Refund Rate : {refund_rate:.2f}%")


    logger.info("=============== KPI 2 : Top Refund Merchants ===============")

    refund_df.groupBy(col("merchant_id"),col("merchant_name")) \
        .agg(count(col("refund_id")).alias("refund_cnt")) \
        .orderBy(col("refund_cnt").desc()) \
        .limit(5).show()


    logger.info("=============== KPI 3 : Refund Reason Trend ===============")

    refund_df.groupBy(col("refund_reason")) \
        .agg(count("refund_id").alias("refund_count")) \
        .orderBy(col("refund_count").desc()) \
        .show(truncate=False)

    logger.info("=============== KPI 4 : Customer Repeat Refund Frequency ===============")

    refund_df.groupBy(col("customer_id")) \
        .agg(count(col("refund_id")).alias("refund_cnt")) \
        .filter(col("refund_cnt") > 1) \
        .orderBy(col("refund_cnt").desc()) \
        .show(truncate=False)