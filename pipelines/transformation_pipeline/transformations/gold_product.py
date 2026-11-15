"""
Gold - gold_product_performance: top sellers, stockout risk.

Answer Merchandising's question: reorder based on real sales velocity
and current stock, not gut feel off a stale monthly report.

silver_inventory is a daily time series, not a current-state table — every
product has one row per warehouse per day it's ever been snapshotted. Summing
it directly would add up stock across every day in history, not just today.
We take the latest snapshot_date per product/warehouse first, using a window
function rather than a driver-side collect(), to stay fully incremental-refresh
friendly.

silver_order_items and silver_products are SCD Type 1 - no __END_AT filter 
needed.

Null-safety: a brand-new product with no inventory snapshot yet produces
current_stock = NULL after the left join. NULL < threshold evaluates to NULL,
not true — so an at-risk new product would silently show as neither flagged
nor safe. coalesce(current_stock, 0) forces it to read as at-risk, correctly.
"""

from pyspark import pipelines as dp
from gold_product_logic import compute_product_performance

@dp.materialized_view(
    name="gold_product_performance",
    comment="Top sellers and current stockout risk."
)
def gold_product_performance():
    return compute_product_performance(
        order_items_df=spark.read.table("silver_order_items"),
        products_df=spark.read.table("silver_products"),
        inventory_df=spark.read.table("silver_inventory"),
    )




































