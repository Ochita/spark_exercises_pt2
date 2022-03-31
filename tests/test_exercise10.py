from pyspark.sql import SparkSession
from pyspark_test import assert_pyspark_df_equal

from exercises.exercise10 import pipeline


def test_exercise10(spark_session: SparkSession) -> None:
    input_data = [
        {'id': 1, 'ev': '200, 201, 202'},
        {'id': 1, 'ev': '23, 24, 25'},
        {'id': 1, 'ev': None},
        {'id': 2, 'ev': '32'},
        {'id': 2, 'ev': None},
    ]

    input_df = spark_session.sparkContext.parallelize(input_data).toDF()

    expected_data = [
        {'id': 1, 'ev': 200},
        {'id': 1, 'ev': 201},
        {'id': 1, 'ev': 202},
        {'id': 1, 'ev': 23},
        {'id': 1, 'ev': 24},
        {'id': 1, 'ev': 25},
        {'id': 1, 'ev': None},
        {'id': 2, 'ev': 32},
        {'id': 2, 'ev': None},
    ]

    expected_df = spark_session.sparkContext.parallelize(expected_data).toDF()

    result = pipeline(input_df)

    assert_pyspark_df_equal(expected_df, result)
