from pyspark.sql.types import *
from pyspark.sql.functions import *
from src.utils.logging_config import logger

# total settled amount by month
# pending settlement %
# top merchants by net payout
# fee revenue by merchant category

def run_settlement_kpis(settlement_df):
    logger.info("=============== KPI 1 : Total Settled Amount By Month ===============")

    settlement_df.groupBy(date_format("settlement_date", "yyyy-MM").alias("month")) \
        .agg(round(sum(col("net_amount")).alias("total_settled_amount"),2)) \
        .orderBy("month") \
        .show()


    logger.info("=============== KPI 2 : Pending Settlement % ===============")

    # PENDING / SETTLED = count of pending settlements / total settlements * 100
    settlement_df.agg(round(
        sum( when(col("settlement_status") == "PENDING",1).otherwise(0)) * 100 / count("*")
        ,2
    ).alias("pending_settlement_%")) \
    .show()

    # +--------------------+
    # |pending_settlement_%|
    # +--------------------+
    # |                 8.0|
    # +--------------------+


    logger.info("=============== KPI 3 : Top Merchants by Net Payout ===============")

    # net payout = top merchants with sum(net_amount) = amount paid to merchant after fees
    settlement_df.groupBy(col("merchant_id"), col("merchant_name")) \
        .agg(round(sum(col("net_amount")),2).alias("merchant_net_payout")) \
        .orderBy(col("merchant_net_payout").desc()) \
        .show()

    # +-----------+-------------+-------------------+
    # |merchant_id|merchant_name|merchant_net_payout|
    # +-----------+-------------+-------------------+
    # |         17|  Merchant_17|           60728.92|
    # |         23|  Merchant_23|           59136.56|
    # |         32|  Merchant_32|           56261.15|
    # |         27|  Merchant_27|           53749.93|
    # |         14|  Merchant_14|           50711.78|

    logger.info("=============== KPI 4 : Fee Revenue by Merchant Category ===============")

    settlement_df.groupBy("merchant_category") \
        .agg(round(sum(col("fee_amount")),2).alias("fee_revenue")) \
        .orderBy(col("fee_revenue").desc()) \
        .show()

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