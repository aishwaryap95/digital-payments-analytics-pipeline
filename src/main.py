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
        # List Landing Files
        logger.info("*************** Listing Landing Files ***************")
        s3_reader = S3Reader()
        files = s3_reader.list_files(
            s3_client,
            config.bucket_name,
            folder_path=LANDING
        )

        if not files:
            logger.info("No files found in landing")
            return

        logger.info(f"Total files found: {len(files)}")

        # FILE Level Validation
        csv_files, error_files = validate_input_files(files)

        logger.info("Valid CSV file count: %s",len(csv_files))
        logger.info("Invalid file count: %s",len(error_files))

        logger.info("*************** Creating Spark Session ***************")
        spark = spark_session()
        logger.info("*************** Spark Session created. ***************")


        # logger.info("*************** Moving error data to error directory if any ***************")
        # if error_files:
        #     for file_path in error_files:
        #         file_name = file_path.rstrip('/').split('/')[-1]
        #         logger.info("Extracted file name: %s", file_name)
        #
        #         elif missing_columns:
        #             error_files.append((file_path, missing_columns, "SCHEMA_FAILURE"))
        #         else:
        #             error_type = None
        #
        #         logger.info( "Moving file %s to failed folder due to %s", file_name, error_type )

        #         try:
        #             # Move file in S3
        #             move_s3_file( s3_client, config.bucket_name, file_path, FAILED )
        #
        #             # Update DB
        #             mark_file_failed( cursor, connection, file_name, error_message, error_type )
        #         except Exception as e:
        #             logger.error( "Failed to move/update file %s: %s", file_name,  str(e))
        # else:
        #     logger.info("No error files found. All files are valid.")

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
        logger.info("*************** Loading DataFrames ***************")
        df_map = load_payment_dfs(spark, processing_files)

        df_customer = df_map.get("customer")
        df_merchant = df_map.get("merchant")
        df_channel = df_map.get("channel")
        df_transactions = df_map.get("transactions")
        df_refunds = df_map.get("refunds")
        df_settlements = df_map.get("settlements")

        logger.info("Customer Count: %s", df_customer.count())
        logger.info("Merchant Count: %s", df_merchant.count())
        logger.info("Transaction Count: %s", df_transactions.count())

        df_customer.show()
        df_merchant.show()
        df_channel.show()
        df_refunds.show()
        df_settlements.show()
        df_transactions.show(5, False)
        logger.info("*************** DataFrames Loaded Successfully ***************")

        logger.info("*************** Writing data to Data Mart ***************")

        transaction_performance_mart_df = df_transactions.alias("t").join(df_merchant.alias("m"), df_transactions["merchant_id"] == df_merchant["merchant_id"]) \
                                        .join(df_channel.alias("c"), df_channel["channel_id"] == df_transactions["channel_id"]) \
                                        .select(col("t.transaction_id"), col("t.customer_id"), col("t.merchant_id"), col("t.channel_id"),
                                            col("t.transaction_timestamp"), col("t.transaction_amount"), col("t.transaction_status"), col("t.city"),
                                            col("t.processing_fee"), col("c.channel_type"), col("c.provider_name"), col("m.merchant_name"),
                                            col("m.merchant_category"), col("m.state"), col("m.risk_tier"), col("m.settlement_cycle"))
        transaction_performance_mart_df.show()

        s3_transaction_performance_mart_path = f"s3a://{bucket_name}/{config.s3_transaction_performance_mart}/"
        data_writer = DataWriter("overwrite","parquet")
        data_writer.dataframe_writer(transaction_performance_mart_df, s3_transaction_performance_mart_path)

        # Success
                    # mark_file_completed(cursor, connection, file_name)

                    # Move to processed
                    # logger.info(f"Moving file to processed {file_name}")
                    # move_s3_file(s3_client,config.bucket_name,f"{PROCESSING}/{file_name}",PROCESSED)

                # # Mark Failed
                # mark_file_failed(cursor,connection,file_name,str(e),"SYSTEM_FAILURE")
                # # Move to failedmove_s3_file( s3_client, config.bucket_name, f"{PROCESSING}/{file_name}", FAILED )

    except Exception as e:
        logger.error(f"Pipeline Failed : {e}")
        raise e

    finally:
        cursor.close()
        connection.close()

    logger.info("*************** Job Completed ***************")

if __name__ == "__main__":
    main()