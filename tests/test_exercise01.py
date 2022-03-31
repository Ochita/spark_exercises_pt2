from pyspark.sql import SparkSession

from exercises.exercise01 import to_list_of_dicts


def test_exercise12(spark_session: SparkSession) -> None:
    input_data = [
        {'untitled_column_1': 44,
         'untitled_column_2': 8602,
         'untitled_column_3': 37.19},
        {'untitled_column_1': 35,
         'untitled_column_2': 5368,
         'untitled_column_3': 65.89},
    ]

    input_df = spark_session.sparkContext.parallelize(input_data).toDF()

    result = to_list_of_dicts(input_df)

    assert result, input_data
