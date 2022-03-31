from pyspark.sql import SparkSession
from pyspark_test import assert_pyspark_df_equal

from exercises.exercise07 import pipeline


def test_exercise07(spark_session: SparkSession) -> None:
    input_data1 = [
        {'a': 1, 'b': 'abc', 'c': 'cdf'},
        {'a': 2, 'b': 'bgf', 'c': 'nhf'},
        {'a': 3, 'b': 'hyu', 'c': 'jni'},
    ]

    input_data2 = [
        {'a': 1, 'Num': 'bc', 'seco': 'jhi'},
    ]

    input_df1 = spark_session.sparkContext.parallelize(input_data1).toDF()

    input_df2 = spark_session.sparkContext.parallelize(input_data2).toDF()

    expected_data = [
        {'a': 2, 'b': 'bgf', 'c': 'nhf'},
        {'a': 3, 'b': 'hyu', 'c': 'jni'},
    ]

    expected_df = spark_session.sparkContext.parallelize(expected_data).toDF()

    result = pipeline(input_df1, input_df2)

    assert_pyspark_df_equal(expected_df, result)
