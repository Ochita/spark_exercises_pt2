from pyspark.sql import SparkSession
from pyspark_test import assert_pyspark_df_equal

from exercises.exercise11 import pipeline


def test_exercise11(spark_session: SparkSession) -> None:
    input_data = [
        {'col1': 108, 'col2': '123-yygrm'},
        {'col1': 201, 'col2': '777-psgdg'},
    ]

    input_df = spark_session.sparkContext.parallelize(input_data).toDF()

    expected_data = [
        {'col1': 108, 'col2': '123-yygrm', '_col3': 123, '_col4': 'yygrm'},
        {'col1': 201, 'col2': '777-psgdg', '_col3': 777, '_col4': 'psgdg'},
    ]

    expected_df = spark_session.sparkContext.parallelize(expected_data).toDF()

    result = pipeline(input_df)

    assert_pyspark_df_equal(expected_df, result)
