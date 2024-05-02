from pyspark.sql.functions import upper, col, lit, regexp_replace, concat_ws, \
    count, when, isnan, mean, size, split, countDistinct, sum, rank, dense_rank, row_number
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, DoubleType
from pyspark.sql.window import Window
import os
from datetime import datetime
import logging.config

logging.config.fileConfig('Properties/configuration/logging.conf')
loggers = logging.getLogger('extraction')


def extract_files(df, format, filepath, num_partition, headerReq, compressionType):
    try:
        loggers.info('extraction file started method ..... ')
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_folder = os.path.join(filepath, f"output_{timestamp}")
        df.coalesce(num_partition).write.mode('overwrite').format(format).option('compression', compressionType) \
            .option('header', headerReq).save(output_folder)

    except Exception as e:
        loggers.error(f"we have an error with a exception : {str(e)}")
        raise
    else:
        loggers.info("we have finish to extract , Good Job,... ")