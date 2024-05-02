from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.functions import upper, col, lit, regexp_replace, concat_ws, \
    count, when, isnan, mean, split, size, udf, sum, countDistinct
import os
from utils import count_num_zip


import get_env_variables as gev

spark2 = SparkSession.builder \
    .master('local') \
    .appName('test_functions') \
    .getOrCreate()

# df2 = spark2.read.option("header", gev.header).option("inferSchema", gev.inferSchema)\
#     .csv(gev.source_oltp + '/' + 'USA_Presc_Medicare_Data_12021.csv')

current_location = os.getcwd()

df_medicare = spark2.read.option("header", gev.header).option("inferSchema", gev.inferSchema) \
     .csv(current_location + '/' + 'test_source/USAMedicare_Data_sel')

df_city = spark2.read.parquet(current_location + '/' + 'test_source/us_cities.parquet')

# df1 = df1.select(upper(df1.city).alias('city'), df1.state_id, upper(df1.state_name).alias('state_name'),
#                  upper(df1.county_name).alias('country_name'),
#                  df1.population, df1.zips)
#
# df1.write.mode('overwrite').parquet(gev.source_olap + '/' + 'us_cities.parquet')

# df_medicare_sel = df2.select(df2.npi.alias('presc_id'),
#                              df2.nppes_provider_last_org_name.alias('presc_last_name'),
#                              df2.nppes_provider_first_name.alias('presc_first_name'),
#                              df2.nppes_provider_city.alias('presc_city'),
#                              df2.nppes_provider_state.alias('presc_state'),
#                              df2.specialty_description.alias('presc_description'), df2.drug_name,
#                              df2.total_claim_count.cast(IntegerType()).alias('tx_count'),
#                              df2.total_day_supply.cast(IntegerType()), df2.total_drug_cost.cast(DoubleType()),
#                              df2.years_of_exp)
#
# df_medicare_sel = df_medicare_sel.withColumn('country_name', lit('USA'))
#
# df_medicare_sel = df_medicare_sel.withColumn('years_of_exp', regexp_replace('years_of_exp', r"^=", " "))
# df_medicare_sel = df_medicare_sel.withColumn('years_of_exp', col('years_of_exp').cast(IntegerType()))
#
# df_medicare_sel = df_medicare_sel.withColumn('presc_full_name', concat_ws(' ', df_medicare_sel.presc_first_name,
#                                                                           df_medicare_sel.presc_last_name))
#
#
# df_medicare_sel = df_medicare_sel.dropna(subset='presc_id')
# df_medicare_sel = df_medicare_sel.dropna(subset='drug_name')
# df_medicare_sel.write.options(header='True').format("csv").mode('overwrite')\
#     .save(gev.source_oltp + '/' + 'USAMedicare_Data_sel')

# df_city.show(5, False)
# df_medicare.show(5, False)

# df_medicare_sel_null = df_medicare.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c)
#                                            for c in df_medicare.columns])
# df_medicare_sel_null.select('*').show()

# df_presciber_nb = df_medicare.select(mean(col('tx_count'))).collect()[0][0]
# print(df_presciber_nb)


# root
#  |-- presc_id: integer (nullable = true)
#  |-- presc_last_name: string (nullable = true)
#  |-- presc_first_name: string (nullable = true)
#  |-- presc_city: string (nullable = true)
#  |-- presc_state: string (nullable = true)
#  |-- presc_description: string (nullable = true)
#  |-- drug_name: string (nullable = true)
#  |-- tx_count: integer (nullable = true)
#  |-- total_day_supply: integer (nullable = true)
#  |-- total_drug_cost: double (nullable = true)
#  |-- years_of_exp: integer (nullable = true)
#  |-- country_name: string (nullable = true)
#  |-- presc_full_name: string (nullable = true)

df_city = df_city.withColumn('num_zips', size(split('zips', ' ')))
df_city = df_city.withColumn('num_zips', count_num_zip(df_city.zips))

df_medicare_agg = (
    df_medicare.groupBy(['presc_state', 'presc_city'])
    .agg(countDistinct('presc_id').alias('num_prescriber'),
         sum('tx_count').alias('total_count'))
    .orderBy('presc_state')
)


df_join = df_city.join(df_medicare_agg, (df_city.state_id == df_medicare_agg.presc_state) & (df_city.city == df_medicare_agg.presc_city), 'inner')

df_final = df_join.select('city', 'state_name', 'country_name', 'population', 'num_zips', 'num_prescriber')

df_final.show(20, False)
