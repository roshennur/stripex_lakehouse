from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, sum as _sum, max as _max, when


def compute_funnel_analysis(clickstream_df: DataFrame) -> DataFrame:
    session_outcomes = (
        clickstream_df
        .groupBy("session_id", "referrer")
        .agg(
            _max(when(col("event_type") == "purchase", 1).otherwise(0)).alias("converted")
        )
    )

    return (
        session_outcomes
        .groupBy("referrer")
        .agg(
            count("session_id").alias("total_sessions"),
            _sum("converted").alias("converted_sessions"),
        )
        .withColumn("conversion_rate", col("converted_sessions") / col("total_sessions"))
    )