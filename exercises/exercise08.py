"""Pivot"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as f


def pipeline(df: DataFrame) -> DataFrame:
    return df.groupBy().pivot('type').agg(f.first(f.col('value'))) \
        .withColumn('customer_id', f.lit(1).cast('bigint'))
