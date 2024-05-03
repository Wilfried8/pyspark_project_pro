
import logging.config

logging.config.fileConfig('Properties/configuration/logging.conf')
loggers = logging.getLogger('persist_data')


def persist_data_city(spark, df, dfName, partitionBy, mode):
    try:
        loggers.warning('persisting data into hive Table for {}'.format(dfName))

        loggers.info('creation of the database ..... ')

        spark.sql(
            """ create database if not exists cities """
        )
        spark.sql(
            """ use cities """
        )

        logging.info('Now writing {} into hive table by {} ..... '.format(df, partitionBy))

        df.write.mode(mode).saveAsTable(dfName, partitionBy=partitionBy)

    except Exception as e:
        loggers.error(f"we have an error with a exception : {str(e)}")
        raise
    else:
        loggers.info("we have finished to persist data city , Good Job,... ")


def persist_data_prescriber(spark, df, dfName, partitionBy, mode):
    try:
        loggers.warning('persisting data into hive Table for {}'.format(dfName))

        loggers.info('creation of the database ..... ')

        spark.sql(
            """ create database if not exists prescribers """
        )
        spark.sql(
            """ use prescribers """
        )

        logging.info('Now writing {} into hive table by {} ..... '.format(df, partitionBy))

        df.write.mode(mode).saveAsTable(dfName, partitionBy=partitionBy)

    except Exception as e:
        loggers.error(f"we have an error with a exception : {str(e)}")
        raise
    else:
        loggers.info("we have finished to persist data prescriber , Good Job,... ")


def persist_to_postgres(df, dfName, mode):
    try:
        loggers.warning('starting save data to postgres ......')

        url = "jdbc:postgresql://localhost:5432/city_presc"

        properties = {
            "user": "postgres",
            "password": "postgres",
            "driver": "org.postgresql.Driver"
        }

        logging.info('Now writing {} into postgres table by ..... '.format(df))
        df.write.jdbc(url, dfName, mode, properties)

    except Exception as e:
        loggers.error(f"we have an error with a exception : {str(e)}")
        raise
    else:
        loggers.info("we have finished to persist data to postgres , Good Job,... ")