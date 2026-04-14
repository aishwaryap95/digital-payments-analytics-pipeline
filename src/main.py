from src.utils.logging_config import logger
from resources.dev import config
from src.utils.s3_client_object import S3ClientProvider
from src.utils.encrypt_decrypt import *
from src.utils.my_sql_session import *
from src.storage.file_status_manager import *
from src.storage.read.s3_read import *

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

    # Step 3 - List Landing Files
    logger.info("*************** Listing Landing Files ***************")
    s3_reader = S3Reader()
    files = s3_reader.list_files(
        s3_client,
        config.bucket_name,
        folder_path=LANDING
    )

    # try:
    #
    #     logger.info("*************** Listing Landing Files ***************")
    #
    #     s3_reader = S3Reader()
    #
    #     files = s3_reader.list_files(
    #         config.s3_client,
    #         config.bucket_name,
    #         folder_path=LANDING
    #     )
    #
    #     if not files:
    #         logger.info("No files found in landing")
    #         return
    #
    #     logger.info(f"Total files found: {len(files)}")
    #
    #     # Step 9 - Reprocess Failed Files
    #     failed_files = reprocess_failed_files(cursor)
    #
    #     logger.info(f"Failed files eligible for reprocess: {failed_files}")
    #
    #     # Merge landing + failed files
    #     all_files = files + [f[0] for f in failed_files]
    #
    #     for file in all_files:
    #
    #         file_name = file.split("/")[-1]
    #
    #         logger.info(f"Processing file: {file_name}")
    #
    #         try:
    #
    #             # Step 4 - Move to processing
    #             logger.info(f"Moving file to processing: {file_name}")
    #
    #             move_s3_file(
    #                 config.s3_client,
    #                 config.bucket_name,
    #                 file,
    #                 PROCESSING
    #             )
    #
    #             # Step 5 - Mark Active
    #             mark_file_active(cursor, connection, file_name)
    #
    #             # Step 6 - Processing
    #             logger.info(f"Validating file {file_name}")
    #
    #             # validation logic
    #
    #             logger.info(f"Transforming file {file_name}")
    #
    #             # transformation logic
    #
    #             logger.info(f"Loading file {file_name}")
    #
    #             # load logic
    #
    #             # Step 7 - Success
    #             mark_file_completed(cursor, connection, file_name)
    #
    #             # Step 8 - Move to processed
    #             logger.info(f"Moving file to processed {file_name}")
    #
    #             move_s3_file(
    #                 config.s3_client,
    #                 config.bucket_name,
    #                 f"{PROCESSING}/{file_name}",
    #                 PROCESSED
    #             )
    #
    #         except Exception as e:
    #
    #             logger.error(f"File processing failed {file_name} : {e}")
    #
    #             # Step 8 - Mark Failed
    #             mark_file_failed(
    #                 cursor,
    #                 connection,
    #                 file_name,
    #                 str(e),
    #                 "SYSTEM_FAILURE"
    #             )
    #
    #             # Move to failed
    #             move_s3_file(
    #                 config.s3_client,
    #                 config.bucket_name,
    #                 f"{PROCESSING}/{file_name}",
    #                 FAILED
    #             )
    #
    # except Exception as e:
    #     logger.error(f"Pipeline Failed : {e}")
    #     raise e
    #
    # finally:
    #     cursor.close()
    #     connection.close()
    #
    # logger.info("*************** Job Completed ***************")

if __name__ == "__main__":
    main()

# Mark Old Active Files as Failed
# Move File to Processing + Mark Active
# try and catch = block
# Move Failed Files to Landing (Reprocess)
# 1. Job Start
# 2. Mark old Active → Failed
# 3. List landing files
# 4. Move to processing
# 5. Mark Active
# 6. Try process
# 7. Success → Completed
# 8. Fail → Failed + error_type
# 9. Reprocess allowed failures