from pyspark.sql.types import *

customer_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("customer_name", StringType(), True),
    StructField("age_group", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("kyc_status",StringType(), True),
    StructField("preferred_channel_id", StringType(), True)
])

merchant_schema = StructType([
    StructField("merchant_id", IntegerType(), False),
    StructField("merchant_name", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("onboard_date", DateType(), True),
    StructField("risk_tier", StringType(), True),
    StructField("settlement_cycle", StringType(), True)
])

channel_schema = StructType([
    StructField("channel_id", IntegerType(), False),
    StructField("channel_type", StringType(), True),
    StructField("provider_name", StringType(), True)
])

transaction_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("merchant_id", IntegerType(), True),
    StructField("channel_id", IntegerType(), True),
    StructField("transaction_timestamp", TimestampType(), True),
    StructField("transaction_amount", DoubleType(), True),
    StructField("transaction_status", StringType(), True),
    StructField("failure_reason", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("bank_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("processing_fee", DoubleType(), True)
])

refund_schema = StructType([
    StructField("refund_id", StringType(), False),
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("refund_timestamp", TimestampType(), True),
    StructField("refund_amount", DoubleType(), True),
    StructField("refund_reason", StringType(), True),
    StructField("refund_status", StringType(), True)
])

settlement_schema = StructType([
    StructField("settlement_id", StringType(), False),
    StructField("merchant_id", IntegerType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("settlement_date", DateType(), True),
    StructField("gross_amount", DoubleType(), True),
    StructField("fee_amount", DoubleType(), True),
    StructField("net_amount", DoubleType(), True),
    StructField("settlement_status", StringType(), True)
])