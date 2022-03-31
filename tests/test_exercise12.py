from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark_test import assert_pyspark_df_equal

from exercises.exercise12 import pipeline


def test_exercise12(spark_session: SparkSession) -> None:
    input_data = [
        {'data': 'July 4, 2020'},
        {'data': '10.05.2009'},
        {'data': '4 Dec 2017'},
        {'data': '5th June 2018'},
    ]

    input_df = spark_session.sparkContext.parallelize(input_data).toDF()

    expected_data = [
        {'data': 'July 4, 2020', 'year': 2020},
        {'data': '10.05.2009', 'year': 2009},
        {'data': '4 Dec 2017', 'year': 2017},
        {'data': '5th June 2018', 'year': 2018},
    ]

    expected_df = spark_session.sparkContext.parallelize(expected_data) \
        .toDF().withColumn('year', f.col('year').cast('integer'))

    result = pipeline(input_df)

    assert_pyspark_df_equal(expected_df, result)
