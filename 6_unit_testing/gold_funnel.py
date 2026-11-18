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
from gold_funnel_logic import compute_funnel_analysis

@dp.materialized_view(
    name="gold_funnel_analysis",
    comment="Conversion rate bu channel, including anonymous session."
)
def gold_funnel_analysis():
    return compute_funnel_analysis(spark.read.table("silver_clickstream"))