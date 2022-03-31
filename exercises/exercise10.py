"""Split by , and explode"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as f

RGXP = r'\,\s?'


def pipeline(df: DataFrame) -> DataFrame:
    return df.withColumn('ev', f.explode_outer(f.split(f.col('ev'), RGXP))) \
        .withColumn('ev', f.col('ev').cast('bigint'))
