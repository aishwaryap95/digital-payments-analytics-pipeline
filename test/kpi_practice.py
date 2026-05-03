from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.utils.spark_session import *
from src.storage.read.payment_reader import *

spark = spark_session()

df = spark.read.parquet("s3a://de-aisha-project-2/data_marts/transaction_performance_mart/transaction_year=2026/")
refund_df = spark.read.parquet("s3a://de-aisha-project-2/data_marts/refund_insights_mart/refund_year=2026/")
settlement_df = spark.read.parquet("s3a://de-aisha-project-2/data_marts/merchant_settlement_mart/settlement_year=2026/")

logger.info("=============== Refund Insights Mart KPI ===============")

# total settled amount by month
# pending settlement %
# top merchants by net payout
# fee revenue by merchant category

#select(
     # col("m.merchant_id"), col("m.merchant_name"), col("m.merchant_category"), col("m.city"), col("m.state"),
     # col("m.onboard_date"), col("m.risk_tier"), col("m.settlement_cycle"), col("s.settlement_id"),
     # col("s.settlement_date"),
     # year(col("s.settlement_date")).alias("settlement_year"),
     # month(col("s.settlement_date")).alias("settlement_month"),
     # col("s.transaction_id"), col("s.gross_amount"), col("s.fee_amount"),
     # col("s.net_amount"), col("s.settlement_status"), col("t.transaction_timestamp"), col("t.transaction_status")

settlement_df.show()

# __________ 1. total settled amount by month
# grpBy(settlement_month), sum(net_amount)
# settlement_df.groupBy(date_format("settlement_date", "yyyy-MM").alias("month")) \
#     .agg(round(sum(col("net_amount")).alias("total_settled_amount"),2)) \
#     .orderBy("month") \
#     .show()

# __________ 2. pending settlement %
# PENDING / SETTLED = count of pending settlements / total settlements * 100
# settlement_df.agg(round(
#     sum( when(col("settlement_status") == "PENDING",1).otherwise(0)) * 100 / count("*")
#     ,2
# ).alias("pending_settlement_%")) \
# .show()
# +--------------------+
# |pending_settlement_%|
# +--------------------+
# |                 8.0|
# +--------------------+

# __________ 3. top merchants by net payout
# net payout = top merchants with sum(net_amount) = amount paid to merchant after fees
# settlement_df.groupBy(col("merchant_id"), col("merchant_name")) \
#     .agg(round(sum(col("net_amount")),2).alias("merchant_net_payout")) \
#     .orderBy(col("merchant_net_payout").desc()) \
#     .show()
# +-----------+-------------+-------------------+
# |merchant_id|merchant_name|merchant_net_payout|
# +-----------+-------------+-------------------+
# |         17|  Merchant_17|           60728.92|
# |         23|  Merchant_23|           59136.56|
# |         32|  Merchant_32|           56261.15|
# |         27|  Merchant_27|           53749.93|
# |         14|  Merchant_14|           50711.78|

# __________ 4. fee revenue by merchant category
# settlement_df.groupBy("merchant_category") \
#     .agg(round(sum(col("fee_amount")),2).alias("fee_revenue")) \
#     .orderBy(col("fee_revenue").desc()) \
#     .show()
# +-----------------+-----------+
# |merchant_category|fee_revenue|
# +-----------------+-----------+
# |       Healthcare|     5131.4|
# |             Fuel|    4778.24|
# |             Food|     3195.6|
# |           Travel|     3136.7|
# |      Electronics|    3129.84|
# |          Grocery|    1816.24|
# |          Fashion|    1467.38|
# +-----------------+-----------+

# __________ 1. refund rate
# IMP: join is un-necessary here.
# no of refunded trans / tot trans * 100
# refund_df.show()

# count() does not take any argument.
# refund_id = refund records,  transaction_id = refunded transactions

# refund_txn = refund_df.select("transaction_id").distinct().count()
# tot_success_txn = df.filter(col("transaction_status") == "SUCCESS").count()
# print("refund txn: ", refund_txn)
# print("tot_success_txn: ", tot_success_txn)

# print("Refund Rate : ", __builtins__.round((refund_txn / tot_success_txn) * 100,2))   # python round() is used here, so its expecting column, not float

# __________ 2. top 5 refund merchants
# IMP: Merchant with high transaction volume naturally may have more refunds.
# group by merchant id, merchant_name, count refunds
# refund_df.groupBy(col("merchant_id"),col("merchant_name")) \
#     .agg(count(col("refund_id")).alias("refund_cnt")) \
#     .orderBy(col("refund_cnt").desc()) \
#     .limit(5).show()

# __________ 3. refund reason trend
# group by refund reason , top reasons
# refund_df.groupBy(col("refund_reason")) \
#     .agg(count("refund_id").alias("refund_count")) \
#     .orderBy(col("refund_count").desc()) \
#     .show(truncate=False)


# __________ 4. Customer Repeat Refund Frequency
#  customers who requested refunds multiple times.
# refund_df.groupBy(col("customer_id")) \
#     .agg(count(col("refund_id")).alias("refund_cnt")) \
#     .filter(col("refund_cnt") > 1) \
#     .orderBy(col("refund_cnt").desc()) \
#     .show(truncate=False)


# logger.info("=============== Transaction Performance Mart KPI ===============")
# __________ 1. daily transaction volume
# df.groupBy(date_format("transaction_timestamp", "yyyy-MM-dd")).agg(count("*").alias("Daily_transactions_volume")).show()

# __________ 2. revenue by channel
# df.filter(col("transaction_status") == "SUCCESS").groupBy(col("channel_type")).agg(round(sum(col("processing_fee")),2).alias("revenue_by_channel")).show()

# __________ 3. city-wise payment amount
# df.filter(col("transaction_status") == "SUCCESS") \
#     .groupBy(col("city")).agg(round(sum(col("transaction_amount")),2).alias("amount_spend_per_city")) \
#     .orderBy(col("amount_spend_per_city").desc()).show()

# __________ 3.1 city-wise payment amount, add txn_cnt as well
# df.filter(col("transaction_status") == "SUCCESS") \
#     .groupBy(col("city")).agg(
#         count(col("*")).alias("txn_cnt"),
#         round(sum(col("transaction_amount")),2).alias("amount_spend_per_city")
#     ) \
#     .orderBy(col("amount_spend_per_city").desc()).show()

# __________ 4. success rate
# for successful transactions, no of success tran / total tran * 100
#df.agg((sum( when(col("transaction_status")=="SUCCESS",1).otherwise(0)) * 100 / count("*")).alias("success_rate")).show()

# __________ 5. monthly success rate by channel
# df.groupBy(date_format(col("transaction_timestamp"), "yyyy-MM"), col("channel_type")) \
#     .agg(
#         round(sum( when(col("transaction_status") == "SUCCESS",1).otherwise(0))).alias("success_rate") * 100 / count("*")
#     )  \
#     .show()