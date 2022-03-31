"""Split by -"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as f


def pipeline(df: DataFrame) -> DataFrame:
    return df.select(f.col('col1'),
                     f.col('col2'),
                     f.split(f.col('col2'),
                             '-').getItem(0).alias('_col3').cast('bigint'),
                     f.split(f.col('col2'), '-').getItem(1).alias('_col4'))
