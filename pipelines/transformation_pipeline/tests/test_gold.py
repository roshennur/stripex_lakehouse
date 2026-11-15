"""
Unit tests — Gold: gold_daily_revenue, gold_product_performance,
gold_customer_360.

Every fixture here is small and hand-crafted, written directly in the test
that uses it — no external file, no generator. Assertions use
pyspark.testing.utils.assertDataFrameEqual, PySpark's own standard-library
tool for exactly this (Spark 3.5+, no extra install).
"""

from datetime import date, datetime

import pytest
from pyspark.testing.utils import assertDataFrameEqual

from gold_revenue_logic import compute_daily_revenue
from gold_product_logic import compute_product_performance
from gold_customer_logic import compute_customer_360
from gold_funnel_logic import compute_funnel_analysis
from gold_fulfillment_logic import compute_fulfillment_health

# ---------------------------------------------------------------------------
# gold_daily_revenue
# ---------------------------------------------------------------------------

def test_daily_revenue_allocates_discount_proportionally_and_filters_scd2(local_spark):

    categories_df = local_spark.createDataFrame(
        [("CAT1", "Formal"), ("CAT2", "Sports")],
        schema="category_id string, category_name string"
    )
    products_df = local_spark.createDataFrame(
        [("P1", "CAT1"), ("P2", "CAT2")],
        schema="product_id string, category_id string"
    )

    orders_df = local_spark.createDataFrame(
        [
            # Current version of O1 — the one that should actually be used.
            ("O1", "2026-07-10", "CA", 10.0, None),
            # A SUPERSEDED version of the same order, with a deliberately wrong
            # discount, to prove the SCD2 filter excludes it. If this leaked
            # through, the test's expected totals below would be wrong.
            ("O1", "2026-07-10", "CA", 999.0, "2026-07-09T00:00:00"),
            # O2 has no discount at all — proves coalesce(discount_amount, 0).
            ("O2", "2026-07-10", "NY", None, None)
        ],
        schema="order_id string, order_date string, shipping_state string, discount_amount double, __END_AT string"
    )

    order_items_df = local_spark.createDataFrame(
        [
            ("I1", "O1", "P1", 60.0),
            ("I2", "O1", "P2", 40.0),
            ("I3", "O2", "P1", 50.0)
        ],
        schema="order_item_id string, order_id string, product_id string, line_total double"
    )

    result = compute_daily_revenue(order_items_df, orders_df, products_df, categories_df )

    expected = local_spark.createDataFrame(
        [
            (date(2026, 7, 10), "Formal", "CA", 60.0, 6.0, 54.0),
            (date(2026, 7, 10), "Sports", "CA", 40.0, 4.0, 36.0),
            (date(2026, 7, 10), "Formal", "NY", 50.0, 0.0, 50.0),
        ],
        schema="revenue_date date, category_name string, region string, "
        "gross_revenue double, total_discount double, net_revenue double",
    )

    assertDataFrameEqual(result, expected, checkRowOrder=False, rtol=1e-4)

























