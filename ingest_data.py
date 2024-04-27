import logging.config

logging.config.fileConfig('Properties/configuration/logging.conf')
loggers = logging.getLogger('ingest_data')


def load_files(spark, file_format, file_direction, header, inferSchema):
    loggers.info("load files method started ")

    try:
        if file_format == 'parquet':
            df = spark.read.format(file_format).load(file_direction)
        elif file_format == 'csv':
            # df = spark.read.format(file_format).option(header=header).option(inferSchema=inferSchema) \
            #   .load(file_direction)
            df = spark.read.option("header", header).csv(file_direction)

    except Exception as e:
        loggers.error(f"we have an error with a exception : {str(e)}")
        raise
    else:
        loggers.info("dataframe created successfully , Good Job, with format {} ".format(file_format))
    return df


def display_data(df, dfName):
    try:
        loggers.info('Here we display the top 8 in the {}'.format(dfName))
        df_show = df.show(8)

    except Exception as e:
        loggers.error(f"we have an error with a exception : {str(e)}")
        raise
    else:
        loggers.info("dataframe displayed successful , Good Job, with name {} ".format(dfName))
    return df_show


def df_count(df, dfName):
    try:
        loggers.info('Here to count the records in the {}'.format(dfName))
        df_c = df.count()

    except Exception as e:
        loggers.error(f"we have an error with a exception : {str(e)}")
        raise

    else:
        loggers.info("Number of records present in the {} are {} ".format(dfName, df_c))
    return df_c


