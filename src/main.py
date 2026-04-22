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

        for file in csv_files:
            if __name__ == '__main__':
                file_name = file.split('/')[-1]
                logger.info(f"Processing file: {file_name}")

                try:
                    # Move file to /processing
                    logger.info("Moving file to /processing: {file_name}")
                    move_s3_file(s3_client, config.bucket_name, file, PROCESSING)

                    # Mark file as Active
                    # Check file exists in staging?
                    # If not exists -> INSERT row with status = A
                    # If exists -> UPDATE status = A
                    processing_path = build_s3_path(config.bucket_name, PROCESSING, file_name)
                    upsert_file_active(cursor, connection, file_name, processing_path)

                    # Processing

                    # Success
                    # mark_file_completed(cursor, connection, file_name)

                    # Move to processed
                    # logger.info(f"Moving file to processed {file_name}")
                    # move_s3_file(s3_client,config.bucket_name,f"{PROCESSING}/{file_name}",PROCESSED)

                except Exception as e:
                    logger.error(f"File processing failed {file_name} : {e}")

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