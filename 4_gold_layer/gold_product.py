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
from pyspark.sql import Window
from pyspark.sql.functions import col, sum as _sum, row_number, desc, coalesce, lit

@dp.materialized_view(
    name="gold_product_performance",
    comment="Top sellers and current stockout risk."
)
def gold_product_performance():
    order_items = spark.read.table("silver_order_items") # SCD1
    products = spark.read.table("silver_products") # SCD1
    inventory = spark.read.table("silver_inventory") # daily time series, not current-state

    sales = (
        order_items
        .groupBy("product_id")
        .agg(
            _sum("quantity").alias("units_sold"),
            _sum("line_total").alias("revenue")
        )
    )

    latest_warehouse = (
        inventory
        .withColumn(
            "rn",
            row_number().over(Window.partitionBy("product_id", "warehouse_id").orderBy(desc("snapshot_date")))
        )
        .filter(col("rn") == 1)
    )

    current_stock = (
        latest_warehouse
        .groupBy("product_id")
        .agg(
            _sum("quantity_available").alias("current_stock")
        )
    )

    STOCKOUT_THRESHOLD = 20
    
    return (
        products
        .join(sales, "product_id", "left")
        .join(current_stock, "product_id", "left")
        .withColumn("current_stock", coalesce(col("current_stock"), lit(0)))
        .withColumn("stockout_risk", col("current_stock") < STOCKOUT_THRESHOLD)
        .select("product_id", "product_name", "brand", "units_sold", "revenue", "current_stock", "stockout_risk")
    )




































