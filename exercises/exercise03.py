"""Search all name column values in text column
and aggregate to list it's original names"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.window import Window as w


# Cross-join pipeline
def _pipeline(df: DataFrame) -> DataFrame:
    renamed = df.select(f.col('_1').alias('text'),
                        f.col('_2').alias('name'),
                        f.col('_3').alias('original_name'))

    joined = renamed.crossJoin(renamed.select(f.col('name')
                                              .alias('name2'),
                                              f.col('original_name')
                                              .alias('new_column')))
    return joined.filter(f.col('text').contains(f.col('name2'))) \
        .groupBy('text').agg(f.first('name').alias('name'),
                             f.first('original_name').alias('original_name'),
                             f.collect_list('new_column').alias('new_column'))


# Window function pipeline
def pipeline(df: DataFrame) -> DataFrame:
    wdw = w.partitionBy()
    named = df.select(f.col('_1').alias('text'),
                      f.col('_2').alias('name'),
                      f.col('_3').alias('original_name'))

    return \
        named.withColumn('new_column',
                         f.transform(
                             f.filter(
                                 f.collect_list(f.struct(f.col('name'),
                                                         f.col(
                                                             'original_name'))
                                                ).over(wdw),
                                 lambda x: f.col('text').contains(x['name'])),
                             lambda x: x['original_name']))
