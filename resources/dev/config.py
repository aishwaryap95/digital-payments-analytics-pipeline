import os

from dotenv import load_dotenv
load_dotenv()

key = "retail_project_2026"
iv = "retail_iv_123456"
salt = "retail_salt_2026"

# AWS Access And Secret key
aws_access_key = os.getenv("AWS_ACCESS_KEY")
aws_secret_key = os.getenv("AWS_SECRET_KEY")
bucket_name = "de-aisha-project-2"

# Base folders
s3_landing_directory = "landing/"
s3_processing_directory = "processing/"
s3_processed_directory = "processed/"
s3_failed_directory = "failed/"

# Data mart folders
s3_data_mart_directory = "data_mart/"
s3_customer_datamart_directory = "data_mart/customer_data_mart/"
s3_sales_datamart_directory = "data_mart/sales_team_data_mart/"

# Partitioned data mart
s3_partitioned_datamart_directory = "partitioned_data_mart/"

# Database credential
DB_HOST = os.getenv("MYSQL_HOST")
DB_USER = os.getenv("MYSQL_USER")
DB_PASSWORD = os.getenv("MYSQL_PASSWORD")
DB_NAME = os.getenv("MYSQL_DB")

STAGING_TABLE = "product_staging_table"

url = f"jdbc:mysql://{DB_HOST}:3306/{DB_NAME}"
properties = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Required columns
mandatory_columns = ["customer_id","store_id","product_name","category","sales_date","sales_person_id","price","quantity","total_cost"]
