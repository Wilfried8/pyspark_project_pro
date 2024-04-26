from pyspark.sql import SparkSession
import logging.config

logging.config.fileConfig('Properties/configuration/logging.conf')
loggers = logging.getLogger('create_spark')


def get_spark_objet(envn, appName):
    try:
        loggers.info('started the get_spark_object method ...')
        if envn == 'DEV':
            master = 'local'
        else:
            master = 'yarn'

        loggers.info('master is {}'.format(master))

        spark = SparkSession.builder \
            .master(master) \
            .appName(appName) \
            .getOrCreate()

    except Exception as e:
        loggers.error(f"we have an error with a exception : {str(e)}")

        raise
    else:
        loggers.info("get spark object created, Good Job, go forward ...")
    return spark


