from src.utils.logging_config import logger
from resources.dev import config
from src.utils.s3_client_object import S3ClientProvider
from src.utils.encrypt_decrypt import *

aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key

logger.info("*************** Creating S3 client ***************")
s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()

logger.info("*************** Listing S3 buckets ***************")
response = s3_client.list_buckets()
logger.info("Successfully connected to S3")
logger.info("List of Buckets: %s", response['Buckets'])