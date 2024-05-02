from pyspark.sql.functions import upper, col, lit, regexp_replace, concat_ws, \
    count, when, isnan, mean, size, split, countDistinct, sum
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, DoubleType

import logging.config

logging.config.fileConfig('Properties/configuration/logging.conf')
loggers = logging.getLogger('data_transformation')


def data_report1(df_city, df_medicare):
    try:
        loggers.warning('processing data_report 1 method ..... ')
        loggers.info("calculate the number of zips by city")
        df_city_split = df_city.withColumn('num_zips', size(split('zips', ' ')))

        loggers.info("calculate the number of prescriber and total tx_count by city and state")
        df_medicare_agg = (
            df_medicare.groupBy(['presc_state', 'presc_city'])
            .agg(countDistinct('presc_id').alias('num_prescriber'),
                 sum('tx_count').alias('total_count'))
            .orderBy('presc_state')
        )

        loggers.info("Don't report a city if no prescriber is assigned to it ......  ")
        df_join = df_city_split.join(df_medicare_agg, (df_city.state_id == df_medicare_agg.presc_state) &
                                     (df_city.city == df_medicare_agg.presc_city), 'inner')

        df_final = df_join.select('city', 'state_name', 'country_name', 'population', 'num_zips', 'num_prescriber')

    except Exception as e:
        loggers.error(f"we have an error with a exception : {str(e)}")
        raise
    else:
        loggers.info("we have finish to transform data for reporting 1 , Good Job,... ")

    return df_final
