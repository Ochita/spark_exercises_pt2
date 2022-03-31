"""Explode"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as f


def pipeline(df: DataFrame) -> DataFrame:
    return df.select(f.col('Name'), f.explode(f.col('FavSub')).alias('Marks'))
