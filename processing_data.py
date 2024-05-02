from pyspark.sql.functions import upper, col, lit, regexp_replace, concat_ws, count, when, isnan, mean
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, DoubleType

import logging.config

logging.config.fileConfig('Properties/configuration/logging.conf')
loggers = logging.getLogger('processing_data')


def data_processing(df1, df2):
    try:
        loggers.info('we started data processing method .....')
        loggers.info('selecting required columns and converting some of upper .....')

        df_city_sel = df1.select(upper(df1.city).alias('city'), df1.state_id, upper(df1.state_name).alias('state_name'),
                                 upper(df1.county_name).alias('country_name'),
                                 df1.population, df1.zips)

        loggers.warning('in  OLTP, select some columns and rename .....')
        df_medicare_sel = df2.select(df2.npi.alias('presc_id'),
                                     df2.nppes_provider_last_org_name.alias('presc_last_name'),
                                     df2.nppes_provider_first_name.alias('presc_first_name'),
                                     df2.nppes_provider_city.alias('presc_city'),
                                     df2.nppes_provider_state.alias('presc_state'),
                                     df2.specialty_description.alias('presc_description'), df2.drug_name,
                                     df2.total_claim_count.cast(IntegerType()).alias('tx_count'),
                                     df2.total_day_supply.cast(IntegerType()), df2.total_drug_cost.cast(DoubleType()),
                                     df2.years_of_exp)

        loggers.info('add column country in df_medicare_sel')
        df_medicare_sel = df_medicare_sel.withColumn('country_name', lit('USA'))

        loggers.info('in column year_of_exp replace = by space  ')
        df_medicare_sel = df_medicare_sel.withColumn('years_of_exp', regexp_replace('years_of_exp', r"^=", " "))
        df_medicare_sel = df_medicare_sel.withColumn('years_of_exp', col('years_of_exp').cast(IntegerType()))

        loggers.info('concat first name and last name to have full name')
        df_medicare_sel = df_medicare_sel.withColumn('presc_full_name', concat_ws(' ', df_medicare_sel.presc_first_name,
                                                                                  df_medicare_sel.presc_last_name))

        loggers.info('dropping presc_last_name and presc_first_name')
        df_medicare_sel = df_medicare_sel.drop('presc_first_name', 'presc_last_name')

        loggers.info('check null value in all column in df_medicare_sel')
        df_medicare_sel_null = df_medicare_sel.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c)
                                                       for c in df_medicare_sel.columns])
        df_medicare_sel_null.select('*').show()

        loggers.info('drop the null value in some columns in df_medicare_sel')
        df_medicare_sel = df_medicare_sel.dropna(subset='presc_id')
        df_medicare_sel = df_medicare_sel.dropna(subset='drug_name')

        loggers.info('fill the null values in tx_count with the avg values')
        avg_tx_count = df_medicare_sel.select(mean(col('tx_count'))).collect()[0][0]
        df_medicare_sel = df_medicare_sel.fillna(avg_tx_count, 'tx_count')

        loggers.warning('successfully dropped null values in df_medicare_sel .....')

        loggers.info('check null value in all column in df_city_sel')
        df_city_sel_null = df_city_sel.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c)
                                              for c in df_city_sel.columns])
        df_city_sel_null.select('*').show()

    except Exception as e:
        loggers.error(f"we have an error with a exception : {str(e)}")
        raise
    else:
        loggers.info("we have finish to process data , Good Job,... ")

    return df_city_sel, df_medicare_sel
