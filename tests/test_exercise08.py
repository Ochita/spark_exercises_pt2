from pyspark.sql import SparkSession
from pyspark_test import assert_pyspark_df_equal

from exercises.exercise08 import pipeline


def test_exercise08(spark_session: SparkSession) -> None:
    input_data = [
        {'customer_id': 1, 'type': 'PRIMARY', 'value': 'lung_Cancer'},
        {'customer_id': 2, 'type': 'SECONDARY', 'value': 'dentist'},
        {'customer_id': 3, 'type': 'TERTIARY', 'value': 'childer care'},
    ]

    input_df = spark_session.sparkContext.parallelize(input_data).toDF()

    expected_data = [
        {'customer_id': 1, 'PRIMARY': 'lung_Cancer',
         'SECONDARY': 'dentist', 'TERTIARY': 'childer care'},
    ]

    expected_df = spark_session.sparkContext.parallelize(expected_data).toDF()

    result = pipeline(input_df)
    result.show()

    assert_pyspark_df_equal(expected_df, result)
