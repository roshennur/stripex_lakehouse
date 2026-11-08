"""
Shared utilities for the StripeX ingestion pipeline.
"""

from pyspark.sql import SparkSession

def get_catalog() -> str:
    """
    Returns the target catalog for  this pipeline run.

    Reads the 'stripex.catalog' key from the pipeline's Configuration field
    (set per DABs target - dev, uat, prod). Defaults to 'dev'
    so the pipeline still works during interactive development, before
    any configuration value has been set. 
    """

    spark = SparkSession.getActiveSession()
    return spark.conf.get("stripex.catalog", "dev")

def landing_path(subfolder: str) -> str:
    """Builds a path into this project's landing volume for a given source subfolder."""
    return f"/Volumes/{get_catalog()}/stripex/landing/{subfolder}"