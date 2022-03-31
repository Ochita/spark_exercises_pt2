"""Replace non-digit chars and cast type"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as f

PATTERN = r'(\d+)'


def pipeline(df: DataFrame) -> DataFrame:
    return df.select(f.regexp_extract(f.col('_1'), PATTERN, 1).cast('integer')
                     .alias('result'))
