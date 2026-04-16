from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.utils.logging_config import logger
from src.utils.encrypt_decrypt import *

# We use s3a protocol with Spark and configure AWS credentials in SparkSession to enable direct S3 reads
def spark_session():
    spark = (SparkSession.builder
             .master("local[*]")
             .appName("retail_sales_project")

             .config("spark.jars.packages",
                     "org.apache.hadoop:hadoop-aws:3.3.4" )
             .config("spark.driver.extraClassPath",
                    "C:\\Program Files (x86)\\MySQL\\Connector J 8.0\\mysql-connector-java-8.0.22.jar")

             .config("spark.hadoop.fs.s3a.access.key", decrypt(config.aws_access_key))
             .config("spark.hadoop.fs.s3a.secret.key", decrypt(config.aws_secret_key))
             .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

             .getOrCreate()
             )
    logger.info("spark session %s",spark)
    return spark