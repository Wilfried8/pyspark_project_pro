import logging

from pyspark.sql.functions import current_date, current_timestamp
import logging.config

logging.config.fileConfig('Properties/configuration/logging.conf')
loggers = logging.getLogger('validate')


def get_current_date(spark):
    try:
        loggers.warning('started the get_current_date method ...')
        # output = spark.sql(
        #     """select current_date"""
        # )
        df = spark.range(1)
        output = df.select(current_timestamp())
        loggers.warning(f"we validate spark objet with the current date : {output.collect()[0][0]}")

    except Exception as e:
        loggers.error(f"we have an error with a exception : {str(e)}")

        raise

    else:
        loggers.warning('validation done, Good Job, go forward ...')
