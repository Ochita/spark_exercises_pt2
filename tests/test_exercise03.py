from pyspark.sql import SparkSession
from pyspark_test import assert_pyspark_df_equal

from exercises.exercise03 import pipeline


def test_exercise03(spark_session: SparkSession) -> None:
    input_data = [
        ('HELLOWORLD2019THISISGOOGLE', 'WORLD2019', 'WORLD_2019'),
        ('NATUREISVERYGOODFOROURHEALTH', None, None),
        ('THESUNCONTAINVITAMIND', 'VITAMIND', 'VITAMIN_D'),
        ('BECARETOOURHEALTHISVITAMIND', 'OURHEALTH', 'OUR_ / HEALTH'),
    ]

    input_df = spark_session.sparkContext.parallelize(input_data).toDF()

    expected_data = [
        {'text': 'HELLOWORLD2019THISISGOOGLE', 'name': 'WORLD2019',
         'original_name': 'WORLD_2019', 'new_column': ['WORLD_2019']},
        {'text': 'NATUREISVERYGOODFOROURHEALTH', 'name': None,
         'original_name': None, 'new_column': ['OUR_ / HEALTH']},
        {'text': 'THESUNCONTAINVITAMIND', 'name': 'VITAMIND',
         'original_name': 'VITAMIN_D', 'new_column': ['VITAMIN_D']},
        {'text': 'BECARETOOURHEALTHISVITAMIND', 'name': 'OURHEALTH',
         'original_name': 'OUR_ / HEALTH',
         'new_column': ['VITAMIN_D', 'OUR_ / HEALTH']},
    ]

    expected_df = spark_session.sparkContext.parallelize(expected_data).toDF()

    result = pipeline(input_df)

    assert_pyspark_df_equal(expected_df, result)
