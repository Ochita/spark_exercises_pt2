"""Anti-join"""

from pyspark.sql import DataFrame


def pipeline(df1: DataFrame, df2: DataFrame) -> DataFrame:
    return df1.join(df2, 'a', 'left_anti')
