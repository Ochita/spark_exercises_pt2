from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark_test import assert_pyspark_df_equal

from exercises.exercise04 import pipeline


def test_exercise04(spark_session: SparkSession) -> None:
    input_data = [
        ('asb456jkhkjh',)
    ]

    input_df = spark_session.sparkContext.parallelize(input_data).toDF()

    expected_data = [
        {'result': 456},
    ]

    expected_df = spark_session.sparkContext.parallelize(expected_data) \
        .toDF().withColumn('result', f.col('result').cast('integer'))

    result = pipeline(input_df)

    assert_pyspark_df_equal(expected_df, result)
