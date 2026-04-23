from src.utils.logging_config import logger
from resources.dev import config
from src.storage.move.move_files import move_s3_file
from src.storage.file_status_manager import mark_file_failed

def validate_csv_schema(
        spark,
        csv_files,
        s3_client,
        cursor,
        connection):

    logger.info("*************** Schema Validation Started ***************")

    correct_files = []
    error_files = []

    for file_path in csv_files:

        # keep original for boto3 move
        original_path = file_path

        # spark path
        spark_path = file_path.replace("s3://", "s3a://")

        file_name = original_path.rstrip('/').split('/')[-1]

        try:
            logger.info("Reading file: %s", spark_path)

            data_schema = spark.read.format("csv") \
                .option("header", "true") \
                .load(spark_path).columns

            logger.info("Schema for %s : %s", file_name, data_schema)

            missing_columns = set(config.mandatory_columns) - set(data_schema)

            if missing_columns:
                logger.warning(
                    "Schema validation failed for %s. Missing columns: %s",
                    file_name,
                    missing_columns
                )
                error_message = f"Missing columns: {missing_columns}"

                # Move to failed
                move_s3_file(
                    s3_client,
                    config.bucket_name,
                    original_path,
                    config.s3_failed_directory
                )

                # Update DB
                mark_file_failed(
                    cursor,
                    connection,
                    file_name,
                    error_message,
                    "SCHEMA_FAILURE"
                )

                error_files.append(original_path)

            else:
                logger.info("Schema validation passed for %s", file_name)
                correct_files.append(original_path)

        except Exception as e:

            logger.error(
                "Schema validation crashed for %s : %s",
                file_name,
                str(e)
            )

            move_s3_file(
                s3_client,
                config.bucket_name,
                original_path,
                config.s3_failed_directory
            )

            mark_file_failed(
                cursor,
                connection,
                file_name,
                str(e),
                "SCHEMA_FAILURE"
            )

            error_files.append(original_path)

    logger.info("Valid files count: %s", len(correct_files))
    logger.info("Invalid files count: %s", len(error_files))

    return correct_files