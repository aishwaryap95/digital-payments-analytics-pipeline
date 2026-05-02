from src.utils.logging_config import logger
from resources.dev import config
from src.utils.s3_client_object import S3ClientProvider
from src.utils.encrypt_decrypt import *
from src.utils.my_sql_session import *
from src.storage.file_status_manager import *
from src.storage.read.s3_read import *
from src.storage.move.move_files import *
from src.utils.spark_session import *
from src.validation.schema_validator import *
from src.utils.helper import *
from src.storage.read.payment_reader import *
from src.storage.write.data_writer import *
from src.analytics.transaction_kpi import *
from src.analytics.refund_kpi import *
from src.analytics.settlement_kpi import *

LANDING = config.s3_landing_directory
PROCESSING = config.s3_processing_directory
PROCESSED = config.s3_processed_directory
FAILED = config.s3_failed_directory

aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key

def main():

    logger.info("*************** Job Started ***************")

    connection = get_mysql_connection()
    cursor = connection.cursor()

    # Create S3 Client
    logger.info("*************** Creating S3 client ***************")
    s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
    s3_client = s3_client_provider.get_client()

    logger.info("*************** Listing S3 buckets ***************")
    response = s3_client.list_buckets()
    logger.info("Successfully connected to S3")
    logger.info("List of Buckets: %s", response['Buckets'])

    try:
        # ------------------------------
        # List Landing Files
        # ------------------------------
        logger.info("*************** Listing Landing Files ***************")
        s3_reader = S3Reader()
        files = s3_reader.list_files(
            s3_client,
            config.bucket_name,
            folder_path=LANDING
        )

        # ------------------------------
        # Retry Failed Files
        # ------------------------------
        retry_rows = get_retryable_failed_files(cursor)

        if not files and not retry_rows:
            logger.info("No files found in landing and no retryable failed files")
            return

        logger.info("Landing files count: %s", len(files))
        logger.info("Retryable failed files count: %s", len(retry_rows))

        if retry_rows:
            logger.info("Retryable files: %s", [row[0] for row in retry_rows])

        # ------------------------------
        # FILE Level Validation (landing only)
        # ------------------------------
        csv_files = []
        error_files = []
        if files:
            csv_files, error_files = validate_input_files(files)

        # ------------------------------
        # Add Retry Files AFTER validation
        # ------------------------------
        for row in retry_rows:
            file_name = row[0]
            file_path = row[1]

            try:
                logger.info("Reprocessing failed file: %s", file_name)

                # Move from failed → landing
                move_s3_file(s3_client, config.bucket_name, file_path, LANDING)

                landing_path = build_s3a_path(config.bucket_name, LANDING, file_name)

                # Validate retry file
                retry_csv, retry_error = validate_input_files([landing_path])

                csv_files.extend(retry_csv)
                error_files.extend(retry_error)

            except Exception as e:
                logger.error("Retry failed for %s : %s", file_name, e)

        logger.info("Valid CSV file count: %s", len(csv_files))
        logger.info("Invalid file count: %s", len(error_files))

        # ------------------------------
        # Move Invalid Files to failed/
        # ------------------------------
        for file in error_files:
            file_name = file.split("/")[-1]
            try:
                logger.warning("Invalid file detected: %s",file_name)

                # mark failed in staging table
                failed_path = build_s3a_path(config.bucket_name, FAILED, file_name)

                upsert_file_failed(cursor, connection, file_name, failed_path,  "File failed extension validation","INVALID_FILE",)
                move_s3_file(s3_client, config.bucket_name, file, FAILED)

                logger.info("Moved invalid file to failed : %s", file_name)

            except Exception as e:
                logger.error("Failed handling invalid file %s : %s",file_name,e)


        logger.info("*************** Creating Spark Session ***************")
        spark = spark_session()
        logger.info("*************** Spark Session created. ***************")

        processing_files = []
        for file in csv_files:
                file_name = file.split('/')[-1]
                logger.info(f"Processing file: {file_name}")

                try:
                    # Move file to /processing
                    logger.info("Moving file to /processing: {file_name}")
                    move_s3_file(s3_client, config.bucket_name, file, PROCESSING)

                    # Mark file as Active
                    # Check file exists in staging? If not exists -> INSERT row with status = A, If exists -> UPDATE status = A
                    processing_path = build_s3a_path(config.bucket_name, PROCESSING, file_name)
                    upsert_file_active(cursor, connection, file_name, processing_path)

                    processing_files.append(processing_path)

                except Exception as e:
                    logger.error("Failed before spark load %s : %s", file_name, e)

        # Processing
        logger.info("processing_files = %s", processing_files)

        logger.info("*************** Loading DataFrames ***************")
        df_map = load_payment_dfs(spark, processing_files)

        datasets = {
            "transactions": "transaction_id",
            "refunds": "refund_id",
            "settlements": "settlement_id",
            "customer": "customer_id",
            "merchant": "merchant_id",
            "channel": "channel_id"
        }

        df_clean_map = {}

        for key, pk in datasets.items():
            df = df_map.get(key)
            if df is not None:
                df_clean_map[key] = df.dropDuplicates([pk])

        df_transactions = df_clean_map.get("transactions")
        df_refunds = df_clean_map.get("refunds")
        df_settlements = df_clean_map.get("settlements")
        df_customer = df_clean_map.get("customer")
        df_merchant = df_clean_map.get("merchant")
        df_channel = df_clean_map.get("channel")

        logger.info("*************** Source DataFrames loaded successfully ***************")

        datasets = {
            "Customer": df_customer,
            "Merchant": df_merchant,
            "Channel": df_channel,
            "Transaction": df_transactions,
            "Refund": df_refunds,
            "Settlement": df_settlements
        }

        for name, df in datasets.items():
            if df is not None:
                logger.info("%s records count : %s", name, df.count())
            else:
                logger.warning("%s dataframe not loaded", name)

        logger.info("*************** Building Transaction Performance Mart ***************")

        if df_transactions is not None and df_merchant is not None and df_channel is not None:

            transaction_performance_mart_df = (
                df_transactions.alias("t") \
                .join(
                    df_merchant.alias("m"),
                    df_transactions["merchant_id"] == df_merchant["merchant_id"],
                    "left"
                ) \
                .join(
                    df_channel.alias("c"),
                    df_transactions["channel_id"] == df_channel["channel_id"],
                    "left"
                ).select(
                    col("t.transaction_id"), col("t.customer_id"), col("t.merchant_id"), col("t.channel_id"),
                    col("t.transaction_timestamp"), col("t.transaction_amount"), col("t.transaction_status"), col("t.city"),
                    col("t.processing_fee"), col("c.channel_type"), col("c.provider_name"), col("m.merchant_name"),
                    col("m.merchant_category"), col("m.state"), col("m.risk_tier"), col("m.settlement_cycle"),
                    year(col("t.transaction_timestamp")).alias("transaction_year"),
                    month(col("t.transaction_timestamp")).alias("transaction_month")
                )
            )

            logger.info("Transaction Performance Mart created successfully")
            transaction_performance_mart_df.show()

            s3_transaction_performance_mart_path = f"s3a://{bucket_name}/{config.s3_transaction_performance_mart}"
            logger.info("Writing Transaction Performance Mart to : %s", s3_transaction_performance_mart_path)
            data_writer = DataWriter("append","parquet")
            data_writer.dataframe_writer( transaction_performance_mart_df, s3_transaction_performance_mart_path,
                                          ["transaction_year", "transaction_month"])
            logger.info("Transaction Performance Mart written successfully")

        else:
            logger.warning("Skipping Transaction Performance Mart due to missing inputs")

        if all([df_settlements, df_transactions, df_merchant]):

            logger.info("*************** Building Merchant Settlement Mart ***************")
            # merchant.join(transactions_df) on merchant_id = creates explosion risk. Bcoz 1 transaction not eql to 1 settlement.
            merchant_settlement_mart_df = (
                df_settlements.alias("s") \
                    .join(
                    df_transactions.alias("t"),
                    col("s.transaction_id") == col("t.transaction_id"),
                    "left"
                ) \
                    .join(
                    df_merchant.alias("m"),
                    col("s.merchant_id") == col("m.merchant_id"),
                    "left"
                ).select(
                    col("m.merchant_id"), col("m.merchant_name"), col("m.merchant_category"), col("m.city"), col("m.state"),
                    col("m.onboard_date"), col("m.risk_tier"), col("m.settlement_cycle"), col("s.settlement_id"),
                    col("s.settlement_date"),
                    year(col("s.settlement_date")).alias("settlement_year"),
                    month(col("s.settlement_date")).alias("settlement_month"),
                    col("s.transaction_id"), col("s.gross_amount"), col("s.fee_amount"),
                    col("s.net_amount"), col("s.settlement_status"), col("t.transaction_timestamp"), col("t.transaction_status")
                )
            )

            logger.info("Merchant Settlement Mart created successfully")
            merchant_settlement_mart_df.show()

            s3_merchant_settlement_mart_path = f"s3a://{bucket_name}/{config.s3_merchant_settlement_mart}/"
            logger.info("Writing Merchant Settlement Mart to : %s", s3_merchant_settlement_mart_path)
            data_writer = DataWriter("append", "parquet")
            data_writer.dataframe_writer(merchant_settlement_mart_df, s3_merchant_settlement_mart_path,
                                         ["settlement_year","settlement_month"])
            logger.info("Merchant Settlement Mart written successfully")

        else:
            logger.warning("Skipping Settlement Mart")

        if all([df_refunds, df_transactions, df_merchant, df_customer]):

            logger.info("*************** Building Refund Insights Mart ***************")
            refund_insights_mart_df = (
                df_refunds.alias("r") \
                    .join(
                    df_transactions.alias("t"),
                    col("r.transaction_id") == col("t.transaction_id"),
                    "left"
                ) \
                    .join(
                    df_merchant.alias("m"),
                    col("m.merchant_id") == col("t.merchant_id"),
                    "left"
                ) \
                    .join(
                    df_customer.alias("c"),
                    col("c.customer_id") == col("t.customer_id"),
                    "left"
                ).select(col("r.refund_id"),col("r.transaction_id"),col("r.customer_id"),col("r.refund_timestamp"),col("r.refund_amount")
                         ,col("r.refund_reason"),col("r.refund_status"),col("m.merchant_id"),col("m.merchant_name"),col("m.merchant_category")
                         ,col("c.age_group"),col("c.city"),col("c.state"),col("t.transaction_amount"),col("t.transaction_status")
                         ,col("t.transaction_timestamp"),col("t.channel_id"),
                         year(col("refund_timestamp")).alias("refund_year"),
                         month(col("refund_timestamp")).alias("refund_month")
                )
            )
            logger.info("Refund Insights Mart created successfully")
            refund_insights_mart_df.show()

            s3_refund_insights_mart_path = f"s3a://{bucket_name}/{config.s3_refund_insights_mart}"
            logger.info("Writing Refund Insights Mart to : %s", s3_refund_insights_mart_path)
            data_writer = DataWriter("append", "parquet")
            data_writer.dataframe_writer(refund_insights_mart_df, s3_refund_insights_mart_path,
                                         ["refund_year","refund_month"])
            logger.info("Refund Insights Mart written successfully")

        else:
            logger.warning("Skipping Refund Insights Mart")

        logger.info("*************** All Data Marts Created Successfully ***************")

        logger.info("*************** Running KPI Layer ***************")

        if all([df_refunds, df_transactions, df_merchant, df_customer, df_transactions, df_settlements]):
            run_transaction_kpis(transaction_performance_mart_df)
            run_settlement_kpis(merchant_settlement_mart_df)
            run_refund_kpis(refund_insights_mart_df, transaction_performance_mart_df)

        logger.info("*************** KPI Layer Completed ***************")

        # ==========================================
        # SUCCESS FLOW
        # ==========================================
        logger.info("*************** Marking Files Completed ***************")

        for file_path in processing_files:
            file_name = file_path.split("/")[-1]

            try:
                # Update DB status = Completed
                mark_file_completed(cursor, connection, file_name)

                # Move processing -> processed
                logger.info("Moving file to processed : %s", file_name)

                move_s3_file(s3_client, config.bucket_name, file_path, PROCESSED)

            except Exception as e:
                logger.error("Failed to complete file %s : %s", file_name, e)

    except Exception as e:
        logger.error(f"Pipeline Failed : {e}")

        for file_path in processing_files:
            file_name = file_path.split("/")[-1]

            try:
                move_s3_file(s3_client, config.bucket_name, file_path, FAILED)
                failed_path = build_s3a_path( config.bucket_name, FAILED, file_name )
                upsert_file_failed(cursor, connection, file_name, failed_path, str(e), "SYSTEM_FAILURE")


            except Exception as inner_error:
                logger.error( "Failed to move/update failed file %s : %s", file_name, inner_error )
        raise

    finally:
        cursor.close()
        connection.close()

    logger.info("*************** Job Completed ***************")

if __name__ == "__main__":
    main()