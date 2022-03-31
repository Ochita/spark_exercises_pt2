from pyspark.sql import SparkSession
from pyspark_test import assert_pyspark_df_equal

from exercises.exercise05 import pipeline


def test_exercise05(spark_session: SparkSession) -> None:
    input_data = [
        {'name': 'J.J. Scott'},
        {'name': 'J. S. Joyce'},
        {'name': 'RV. Bradley Carter'},
    ]

    input_df = spark_session.sparkContext.parallelize(input_data).toDF()

    expected_data = [
        {'name_clean': 'JJ Scott'},
        {'name_clean': 'JS Joyce'},
        {'name_clean': 'RV Bradley Carter'},
    ]

    expected_df = spark_session.sparkContext.parallelize(expected_data).toDF()

    result = pipeline(input_df)

    assert_pyspark_df_equal(expected_df, result)
