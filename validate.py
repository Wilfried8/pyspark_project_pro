import logging
from pyspark.sql.functions import upper, col, lit, regexp_replace, concat_ws, count, when, isnan, mean
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


def print_schema(df, dfName):
    try:
        loggers.info('we started print de schema of the dataframe .... {}'.format(dfName))
        df_schema = df.printSchema()
        sc = df.schema.fields

        for field in sc:
            loggers.info(field.name + " , " + str(field.dataType))
    except Exception as e:
        loggers.error(f"we have an error with a exception : {str(e)}")
        raise
    else:
        loggers.warning('print schema done , Good Job, ...')


def check_null_values(df, dfName):
    try:
        loggers.info('check null values method for dataframe ..... {}'.format(dfName))
        df_null_values = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c)
                                    for c in df.columns])
    except Exception as e:
        loggers.error(f"we have an error with a exception : {str(e)}")
        raise
    else:
        loggers.warning('check for null values executed successfully , Good Job, ...')

    return df_null_values


def check_and_drop_duplicate_row(df, dfName):
    try:
        loggers.info("check duplicate rows in the dataframe ...... {}".format(dfName))

        if df.distinct().count() == df.count():
            loggers.warning("we don't have duplicate rows in the dataFrame ..... {}".format(dfName))
        else:
            df.dropDuplicates()
    except Exception as e:
        loggers.info('this is a problem with distinct value')
        loggers.error(f"we have an error with a exception : {str(e)}")
        raise
    else:
        loggers.warning("we don't have duplicate rows , Good Job, ...")
