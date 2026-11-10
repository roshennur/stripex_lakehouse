"""
Gold - gold_funnel_analysis: conversion rate by channel.

Answer Growth's question: which channel actually conversts to buying customers.

Attribution happens at the SESSION level, not the customer level. Roughly half
our clickstream events have a null customer_id — an anonymous visitor. If we
attributed conversion only by customer_id, every anonymous session would be
silently excluded from the funnel entirely, undercounting every channel that
brings in browsing traffic before someone logs in or checks out as a guest.
session_id exists on every event, known customer or not, so it's the correct
grain for this table.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, sum as _sum, max as _max, when

@dp.materialized_view(
    name="gold_funnel_analysis",
    comment="Conversion rate bu channel, including anonymous session."
)
def gold_funnel_analysis():
    events = spark.read.table("silver_clickstream")

    session_outcomes = (
        events
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
            _sum("converted").alias("converted_sessions")
        )
        .withColumn("conversion_rate", col("converted_sessions") / col("total_sessions"))
    )