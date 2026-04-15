from src.utils.logging_config import logger
import traceback

def move_s3_file(s3_client, bucket_name, source_path, destination_prefix):
    try:
        logger.info("Moving file from %s to %s", source_path, destination_prefix)

        # Remove s3://bucket-name/
        prefix = f"s3://{bucket_name}/"
        source_key = source_path.replace(prefix, "")

        file_name = source_key.split("/")[-1]
        destination_key = f"{destination_prefix}/{file_name}"

        logger.info("Source path received: %s", source_path)
        logger.info("Prefix generated: %s", prefix)
        logger.info("Source key extracted: %s", source_key)
        logger.info("File name extracted: %s", file_name)
        logger.info("Destination key generated: %s", destination_key)

        # Copy file
        s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={
                'Bucket': bucket_name,
                'Key': source_key
            },
            Key=destination_key
        )

        # Delete source
        s3_client.delete_object(
            Bucket=bucket_name,
            Key=source_key
        )

        logger.info(
            "File moved successfully from %s to %s",
            source_key,
            destination_key
        )

    except Exception as e:
        logger.error("Error moving file: %s", str(e))
        logger.error(traceback.format_exc())
        raise