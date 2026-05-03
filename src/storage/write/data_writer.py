import traceback
from src.utils.logging_config import logger

class DataWriter:
    def __init__(self,mode,data_format):
        self.mode = mode
        self.data_format = data_format

    def dataframe_writer(self,df, file_path, partition_cols = None):
        try:
            writer = df.write.format(self.data_format) \
                .option("header", "true") \
                .mode(self.mode)

            if partition_cols:
                writer = writer.partitionBy(*partition_cols)

            writer.save(file_path)

        except Exception as e:
            logger.error(f"Error writing the data : {str(e)}")
            traceback_message = traceback.format_exc()
            print(traceback_message)
            raise e