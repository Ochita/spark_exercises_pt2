"""Filter by regex"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as f

PATTERN = r'site-desire-\d{6}'


def pipeline(df: DataFrame) -> DataFrame:
    return df.filter(f.col('id').rlike(PATTERN))
