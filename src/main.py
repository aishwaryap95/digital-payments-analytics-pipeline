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

    # Step 1 - Mark old active files
    mark_old_active_as_failed(cursor, connection)

    # Step 2 - Create S3 Client
    logger.info("*************** Creating S3 client ***************")
    s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
    s3_client = s3_client_provider.get_client()

    logger.info("*************** Listing S3 buckets ***************")
    response = s3_client.list_buckets()
    logger.info("Successfully connected to S3")
    logger.info("List of Buckets: %s", response['Buckets'])

    try:
        # Step 3 - List Landing Files
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

        # Step 4 - Reprocess Failed Files
        failed_files = reprocess_failed_files(cursor)
        logger.info(f"Failed files eligible for reprocess: {failed_files}")

        landing_files = files
        landing_file_names = set([f.split("/")[-1] for f in landing_files])
        failed_file_paths = []

        for f in failed_files:
            file_name = f[0]

            if file_name not in landing_file_names:
                failed_file_paths.append(
                    f"s3://{config.bucket_name}/{LANDING}/{file_name}"
                )

        # Merge landing + failed files
        all_files = landing_files + failed_file_paths
        logger.info("Files to process: %s", all_files)

        # Filter files with .csv in their name and create absolute paths
        if all_files:
            csv_files = []
            error_files = []
            for file in all_files:
                if file.endswith(".csv"):
                    csv_files.append(file)
                else:
                    error_files.append(os.path.abspath(file))

            if not csv_files:
                logger.error("No CSV data available to process the request.")
                raise Exception("No csv data available to process the request")
        else:
            logger.error("There is no data to process")
            raise Exception("There is no data to process")

        logger.info("*************** Listing the File ***************")
        logger.info("List of CSV files that needs to be processed: %s", csv_files)

        logger.info("*************** Creating Spark Session ***************")
        spark = spark_session()
        logger.info("*************** Spark Session created. ***************")

        # schema validation
        correct_files = validate_csv_schema(
            spark,
            csv_files,
            s3_client,
            cursor,
            connection
        )

        logger.info("Files ready for processing: %s", correct_files)

        logger.info("*************** Moving error data to error directory if any ***************")

        # if error_files:
        #     for file_path in error_files:
        #         file_name = file_path.rstrip('/').split('/')[-1]
        #         logger.info("Extracted file name: %s", file_name)
        #
        #         if not file_name.lower().endswith(".csv"):
        #             error_files.append((file_path, None, "FORMAT_FAILURE"))
        #
        #         elif missing_columns:
        #             error_files.append((file_path, missing_columns, "SCHEMA_FAILURE"))
        #
        #         else:
        #             error_type = None
        #
        #         logger.info(
        #             "Moving file %s to failed folder due to %s",
        #             file_name,
        #             error_type
        #         )
        #
        #         try:
        #             # Move file in S3
        #             move_s3_file(
        #                 s3_client,
        #                 config.bucket_name,
        #                 file_path,
        #                 FAILED
        #             )
        #
        #             # Update DB
        #             mark_file_failed(
        #                 cursor,
        #                 connection,
        #                 file_name,
        #                 error_message,
        #                 error_type
        #             )
        #
        #         except Exception as e:
        #             logger.error(
        #                 "Failed to move/update file %s: %s",
        #                 file_name,
        #                 str(e)
        #             )
        # else:
        #     logger.info("No error files found. All files are valid.")
        #
        # for file in csv_files:
        #     file_name = file.split("/")[-1]
        #
        #     logger.info(f"Processing file: {file_name}")
        #     try:
        #         # Move to processing
        #         logger.info(f"Moving file to processing: {file_name}")
        #
        #         move_s3_file(
        #             s3_client,
        #             config.bucket_name,
        #             file,
        #             PROCESSING
        #         )
        #
        #         # Mark Active
        #         mark_file_active(cursor, connection, file_name)
        #
        #         #
        #
        #         # Processing
        #         logger.info(f"Validating file {file_name}")
        #
        #         # validation logic
        #         logger.info(f"Transforming file {file_name}")
        #
        #         # transformation logic
        #         logger.info(f"Loading file {file_name}")
        #
        #         # load logic
        #
        #         # # Success
        #         # mark_file_completed(cursor, connection, file_name)
        #         #
        #         # # Step 8 - Move to processed
        #         # logger.info(f"Moving file to processed {file_name}")
        #         #
        #         # move_s3_file(
        #         #     s3_client,
        #         #     config.bucket_name,
        #         #     f"{PROCESSING}/{file_name}",
        #         #     PROCESSED
        #         # )
        #
        #     except Exception as e:
        #
        #         logger.error(f"File processing failed {file_name} : {e}")

                # # Mark Failed
                # mark_file_failed(
                #     cursor,
                #     connection,
                #     file_name,
                #     str(e),
                #     "SYSTEM_FAILURE"
                # )
                #
                # # Move to failed
                # move_s3_file(
                #     s3_client,
                #     config.bucket_name,
                #     f"{PROCESSING}/{file_name}",
                #     FAILED
                # )

    except Exception as e:
        logger.error(f"Pipeline Failed : {e}")
        raise e

    finally:
        cursor.close()
        connection.close()

    logger.info("*************** Job Completed ***************")

if __name__ == "__main__":
    main()