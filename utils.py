from pyspark.sql.functions import size, udf, split
from pyspark.sql.types import IntegerType


@udf(returnType=IntegerType())
def count_num_zip(df_column):
    return len(df_column.split(' '))
