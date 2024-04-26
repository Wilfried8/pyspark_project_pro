import os
import sys

import get_env_variables as gev
from create_spark import get_spark_objet
from validate import get_current_date
from ingest_data import load_files, display_data, df_count
import logging
import logging.config

logging.config.fileConfig('Properties/configuration/logging.conf')


def main():
    try:
        logging.info(" i am beginning with the main app .......")

        logging.info('creation of th spark object .....')
        spark = get_spark_objet(gev.envn, gev.appName)

        logging.info(f" this is the spark object created : {spark}")

        logging.info('validation spark object ....')
        get_current_date(spark)

        for file in os.listdir(gev.source_olap):
            logging.info('files are ' + file)
            file_direction = gev.source_olap + '/' + file
            logging.info(file_direction)

            if file.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'

            elif file.endswith('.csv'):
                file_format = 'csv'
                header = gev.header
                inferSchema = gev.inferSchema

        logging.info('we are reading a file with format {}'.format(file_format))

        df_city = load_files(spark=spark, file_format=file_format, file_direction=file_direction, header=header,
                             inferSchema=inferSchema)
        logging.info('displaying the dataframe {}'.format(df_city))
        display_data(df=df_city, dfname='df_cty')

        logging.info('validating the dataframe ..... ')
        df_count(df=df_city, dfname='df_city')

        for file2 in os.listdir(gev.source_oltp):
            logging.info('files are ' + file2)
            file_direction = gev.source_oltp + '/' + file2
            logging.info(file_direction)

            if file2.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'

            elif file2.endswith('.csv'):
                file_format = 'csv'
                header = gev.header
                inferSchema = gev.inferSchema

        logging.info('we are reading a file with format {}'.format(file_format))

        df_medicare = load_files(spark=spark, file_format=file_format, file_direction=file_direction, header=header,
                             inferSchema=inferSchema)
        logging.info('displaying the dataframe {}'.format(df_medicare))
        display_data(df=df_city, dfname='df_medicare')

        logging.info('validating the dataframe ..... ')
        df_count(df=df_medicare, dfname='df_medicare')

    except Exception as e:
        logging.info(f"we have an error with a exception : {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
    logging.info('application done, Good Job .....')
