from src.storage.read.payment_schemas import *
from src.utils.logging_config import logger

def read_csv(spark, path, schema):
    return(
        spark.read.
        format("csv")
        .option("header", "true")
        .option("mode","PERMISSIVE")
        .schema(schema)
        .load(path)
    )

def load_payment_dfs(spark, processing_files):
    df_map = {}
    for file in processing_files:
        file_name = file.split("/")[-1].lower()
        logger.info("Reading file: %s", file_name)

        if "dim_customer" in file_name:
            df_map["customer"] = read_csv(spark, file, customer_schema)
        elif "dim_merchant" in file_name:
            df_map["merchant"] = read_csv(spark, file, merchant_schema)
        elif "dim_payment_channel" in file_name:
            df_map["channel"] = read_csv(spark, file, channel_schema)
        elif "fact_refunds" in file_name:
            df_map["refunds"] = read_csv(spark, file, refund_schema)
        elif "fact_settlements" in file_name:
            df_map["settlements"] = read_csv(spark, file, settlement_schema)
        elif "fact_transactions" in file_name:
            df_map["transactions"] = read_csv(spark, file, transaction_schema)

    return df_map