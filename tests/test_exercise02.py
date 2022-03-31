from typing import Dict

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, StructField, StructType

from exercises.exercise02 import convert


def test_exercise02(spark_session: SparkSession) -> None:
    schema = StructType([
        StructField('untitled_column_1', IntegerType()),
        StructField('untitled_column_2', StringType()),
        StructField('untitled_column_3', StringType()),
        StructField('untitled_column_4', StringType()),
        StructField('untitled_column_5', IntegerType()),
        StructField('untitled_column_6', StringType()),
        StructField('untitled_column_7', IntegerType())])

    input_data = [
        {'untitled_column_1': 0,
         'untitled_column_2': None,
         'untitled_column_3': 'Will',
         'untitled_column_4': None,
         'untitled_column_5': 33,
         'untitled_column_6': None,
         'untitled_column_7': 385},
        {'untitled_column_1': 1,
         'untitled_column_2': None,
         'untitled_column_3': 'Jean-Luc',
         'untitled_column_4': None,
         'untitled_column_5': 26,
         'untitled_column_6': None,
         'untitled_column_7': 2},
        {'untitled_column_1': 2,
         'untitled_column_2': None,
         'untitled_column_3': 'Hugh',
         'untitled_column_4': None,
         'untitled_column_5': 55,
         'untitled_column_6': None,
         'untitled_column_7': 221},
    ]

    input_df = spark_session.sparkContext.parallelize(input_data) \
        .toDF(schema=schema)

    expected_data: Dict[str, int] = {}

    result = convert(input_df)

    assert result, expected_data
