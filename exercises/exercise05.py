"""Format initials without dots"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as f

PATTERN = r'(\.\s(?!\w\w))|(\.)'


def pipeline(df: DataFrame) -> DataFrame:
    return df.select(f.regexp_replace(f.col('name'), PATTERN, '')
                     .alias('name_clean'))
