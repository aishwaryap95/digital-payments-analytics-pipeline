#boto3 uses AWS SDK directly, so: s3://
def build_s3_path(bucket_name, folder_name, file_name):
    return f"s3://{bucket_name}/{folder_name.rstrip('/')}/{file_name}"

#Spark uses Hadoop filesystem layer. so:  s3a://
def build_s3a_path(bucket_name, folder_name, file_name):
    return f"s3a://{bucket_name}/{folder_name.rstrip('/')}/{file_name}"