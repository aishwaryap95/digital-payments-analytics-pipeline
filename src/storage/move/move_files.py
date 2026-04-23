from src.utils.logging_config import logger
import traceback


def move_s3_file(s3_client, bucket_name, source_path, destination_prefix):
    try:
        logger.info("Source path received: %s", source_path)

        # Normalize path safely
        source_path = source_path.strip()

        if source_path.startswith("s3://"):
            source_path = "s3://" + source_path[len("s3://"):].replace("//", "/")
            prefix = f"s3://{bucket_name}/"

        elif source_path.startswith("s3a://"):
            source_path = "s3a://" + source_path[len("s3a://"):].replace("//", "/")
            prefix = f"s3a://{bucket_name}/"

        else:
            raise ValueError("Invalid S3 path. Path must start with s3:// or s3a://")

        logger.info("Normalized source path: %s", source_path)
        logger.info("Prefix generated: %s", prefix)

        source_key = source_path.replace(prefix, "", 1)
        logger.info("Source key extracted: %s", source_key)

        file_name = source_key.rstrip("/").split("/")[-1]
        logger.info("File name extracted: %s", file_name)

        destination_key = f"{destination_prefix.rstrip('/')}/{file_name}"
        logger.info("Destination key generated: %s", destination_key)

        s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={
                "Bucket": bucket_name,
                "Key": source_key
            },
            Key=destination_key
        )

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