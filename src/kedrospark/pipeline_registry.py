"""Project pipelines."""
from typing import Dict

from kedro.pipeline import Pipeline

from kedrospark.pipelines.amazonFoodV2 import pipeline as dp

from kedrospark.pipelines.amazonFoodSpark import pipeline as dpspark


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.

    """

    data_processing_pipeline = dp.create_pipeline()
    data_processing_pipeline_spark = dpspark.create_pipeline()
    return {
        "__default__": data_processing_pipeline + data_processing_pipeline_spark,
        "dp": data_processing_pipeline,
        "dpspark": data_processing_pipeline_spark

    }
