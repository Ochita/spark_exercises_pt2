from pyspark.sql import SparkSession
from pyspark_test import assert_pyspark_df_equal

from exercises.exercise09 import pipeline


def test_exercise09(spark_session: SparkSession) -> None:
    input_data = [
        {'Name': 'A', 'FavSub': ['M', 'S', 'P']},
        {'Name': 'B', 'FavSub': ['C', 'M', 'P']},
        {'Name': 'C', 'FavSub': ['F']}
    ]

    input_df = spark_session.sparkContext.parallelize(input_data).toDF()

    expected_data = [
        {'Name': 'A', 'Marks': 'M'},
        {'Name': 'A', 'Marks': 'S'},
        {'Name': 'A', 'Marks': 'P'},
        {'Name': 'B', 'Marks': 'C'},
        {'Name': 'B', 'Marks': 'M'},
        {'Name': 'B', 'Marks': 'P'},
        {'Name': 'C', 'Marks': 'F'}
    ]

    expected_df = spark_session.sparkContext.parallelize(expected_data).toDF()

    result = pipeline(input_df)

    assert_pyspark_df_equal(expected_df, result)
