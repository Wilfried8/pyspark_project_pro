import os
import sys

from time import perf_counter
import get_env_variables as gev
from create_spark import get_spark_objet
from validate import get_current_date, print_schema, check_null_values, check_and_drop_duplicate_row
from ingest_data import load_files, display_data, df_count
from processing_data import data_processing
from data_transformation import data_report1, data_report2
from extraction import extract_files
from persist_data import persist_data_city, persist_data_prescriber, persist_to_postgres
import logging
import logging.config

logging.config.fileConfig('Properties/configuration/logging.conf')

start_time = perf_counter()


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
        logging.info('displaying the dataframe ..... df_cty ')
        display_data(df=df_city, dfName='df_cty')

        logging.info('validating the dataframe ..... df_city ')
        df_count(df=df_city, dfName='df_city')

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
        logging.info('displaying the dataframe ..... df_medicare ')
        display_data(df=df_medicare, dfName='df_medicare')

        logging.info('validating the dataframe ..... ')
        df_count(df=df_medicare, dfName='df_medicare')

        logging.info('implementing processing data method .....')
        df_city_sel, df_medicare_sel = data_processing(df_city, df_medicare)

        logging.info('displaying the dataframe df_city_sel')
        display_data(df_city_sel, dfName='df_city_sel')
        logging.info('displaying the dataframe df_medicare_sel')
        display_data(df_medicare_sel, dfName='df_medicare_sel')

        logging.info('show schema of dataframe ..... df_city_sel')
        print_schema(df_city_sel, dfName='df_city_sel')

        logging.info('show schema of dataframe ..... df_medicare_sel')
        print_schema(df_medicare_sel, dfName='df_medicare_sel')

        logging.info('validating the dataframe ..... df_medicare_sel')
        df_count(df=df_medicare_sel, dfName='df_medicare_sel')

        logging.info('checking null values in dataframe ..... after processing')

        df_null_med = check_null_values(df_medicare_sel, dfName='df_medicare_sel')

        display_data(df_null_med, dfName='df_medicare_sel')

        logging.info('checking duplicate row in dataframe ..... after processing')
        check_and_drop_duplicate_row(df_medicare_sel, dfName='df_medicare_sel')
        check_and_drop_duplicate_row(df_city_sel, dfName='df_city_sel')

        logging.info('data transformation started ....')

        logging.info('transformation for reporting 1 ....')
        data_report_city = data_report1(df_city_sel, df_medicare_sel)

        logging.info('display data report 1 ......')
        display_data(data_report_city, dfName='data_report_1')

        logging.info('transformation for reporting 1 ....')
        data_report_prescriber = data_report2(df_medicare_sel)

        logging.info('display data report 1 ......')
        display_data(data_report_prescriber, dfName='data_report_1')

        logging.info('extract file to output')
        extract_files(data_report_city, 'orc', gev.output_city, 1, 'false', 'snappy')

        extract_files(data_report_prescriber, 'parquet', gev.output_prescriber, 2, 'false', 'snappy')
        logging.info('extract file terminated')

        logging.info('save data in hive table .....')
        persist_data_city(spark=spark, df=data_report_city, dfName='df_city', partitionBy='state_name', mode='append')
        persist_data_prescriber(spark=spark, df=data_report_prescriber, dfName='df_prescriber',
                                partitionBy='presc_state', mode='append')

        logging.info('successfully written into hive, good ......')

        logging.info('save data in postgres table .....')
        persist_to_postgres(df=data_report_city, dfName='df_city', mode='append')
        persist_to_postgres(df=data_report_prescriber, dfName='df_presc', mode='append')

        logging.info('successfully written into postgres, good ......')

    except Exception as e:
        logging.info(f"we have an error with a exception : {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
    end_time = perf_counter()
    logging.info(f'total amount of time taken {start_time - end_time:.2f} seconds')
    logging.info('application done, Good Job .....')
