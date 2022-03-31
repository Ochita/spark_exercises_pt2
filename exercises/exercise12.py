"""Get year from string field"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as f

PATTERN = r'(.*)(\d{4})'


def pipeline(df: DataFrame) -> DataFrame:
    return df.withColumn('year',
                         f.regexp_extract(f.col('data'),
                                          PATTERN, 2).cast('integer'))
