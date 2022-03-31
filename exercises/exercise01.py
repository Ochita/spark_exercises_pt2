"""Convert df to dict"""

from typing import Dict, List

from pyspark.sql import DataFrame


def to_list_of_dicts(df: DataFrame) -> List[Dict[str, int]]:
    return [x.asDict() for x in df.collect()]
