from pyspark.sql import SparkSession
from pyspark_test import assert_pyspark_df_equal

from exercises.exercise06 import pipeline


def test_exercise06(spark_session: SparkSession) -> None:
    input_data = [
        {'id': 'site-trinetx-67d97022f5c84c43a4377ce325b87'},
        {'id': 'site-informa-217913'},
        {'id': 'site-informa-244090'},
        {'id': 'site-desire-106672'},
        {'id': 'site-desire-106674'},
        {'id': 'site-desire-106673'},
        {'id': 'site-informa-106371'},
        {'id': 'site-desire-133102'},
        {'id': 'site-desire-123562'},
        {'id': 'site-desire-122038'},
        {'id': 'site-desire-117560'},
        {'id': 'site-desire-113532'},
        {'id': 'site-desire-123894'},
        {'id': 'site-trinetx-093113f07aa63b113aa84c61be3ad50'},
        {'id': 'site-trinetx-694ac47c9dd1f9e4a6b521323a895'},
        {'id': 'site-trinetx-6fd3b38c3b71d613165f178b50f'},
    ]

    input_df = spark_session.sparkContext.parallelize(input_data).toDF()

    expected_data = [
        {'id': 'site-desire-106672'},
        {'id': 'site-desire-106674'},
        {'id': 'site-desire-106673'},
        {'id': 'site-desire-133102'},
        {'id': 'site-desire-123562'},
        {'id': 'site-desire-122038'},
        {'id': 'site-desire-117560'},
        {'id': 'site-desire-113532'},
        {'id': 'site-desire-123894'},
    ]

    expected_df = spark_session.sparkContext.parallelize(expected_data).toDF()

    result = pipeline(input_df)

    assert_pyspark_df_equal(expected_df, result)
