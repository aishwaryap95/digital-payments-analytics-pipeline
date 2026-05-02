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

        df = None

        if "dim_customer" in file_name:
            df = read_csv(spark, file, customer_schema)
            key = "customer"

        elif "dim_merchant" in file_name:
            df = read_csv(spark, file, merchant_schema)
            key = "merchant"

        elif "dim_payment_channel" in file_name:
            df = read_csv(spark, file, channel_schema)
            key = "channel"

        elif "fact_refunds" in file_name:
            df = read_csv(spark, file, refund_schema)
            key = "refunds"

        elif "fact_settlements" in file_name:
            df = read_csv(spark, file, settlement_schema)
            key = "settlements"

        elif "fact_transactions" in file_name:
            df = read_csv(spark, file, transaction_schema)
            key = "transactions"

        else:
            continue

        # 🔥 UNION LOGIC
        if key in df_map:
            df_map[key] = df_map[key].unionByName(df)
        else:
            df_map[key] = df

    return df_map