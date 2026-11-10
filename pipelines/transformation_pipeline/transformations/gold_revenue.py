"""
Gold — gold_daily_revenue: revenue by day, category, and region.

Answers the VP of Finance's question: trustworthy, current revenue
numbers, not a day-late spreadsheet stitched together by hand.

Revenue definition: gross_revenue, total_discount, and net_revenue are
reported as three separate, clearly labeled columns — not one ambiguous
"total_revenue" that silently picks gross or net. discount_amount lives at the
ORDER grain, not the line-item grain, so net revenue per category requires
proportionally allocating each order's discount across its line items, by each
item's share of that order's subtotal — not just subtracting a flat amount.

silver_orders is SCD Type 2 — filtered to the current row (__END_AT IS NULL)
before anything else, same rule as every Gold read of an SCD Type 2 table.
silver_order_items, silver_products, silver_categories are SCD Type 1 —
already current, no __END_AT column to filter, and silver_order_items itself
is already the quarantine-verified table.

coalesce(discount_amount, 0): a null discount on a discounted order shouldn't
null out the entire net-revenue calculation for that order's line items.

to_date(col("order_date")): order_date arrives from Silver as a real
TIMESTAMP. This call truncates it down to a date, for "revenue by DAY"
grouping — a normal, permanent part of Gold's job, not a workaround for
anything upstream.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, sum as _sum, to_date, coalesce, lit, when

@dp.materialized_view(
    name="gold_daily_revenue",
    comment="Revenue by day, category, and region — gross, discount, and net reported."
)
def gold_daily_revenue():
    order_items = spark.read.table("silver_order_items") # SCD Type 1
    orders_current = spark.read.table("silver_orders").filter(col("__END_AT").isNull())
    products = spark.read.table("silver_products") # SCD Type 1
    categories = spark.read.table("silver_categories") # SCD Type 1

    order_subtotals = (
        order_items.groupBy("order_id")
          .agg(_sum("line_total").alias("order_subtotal"))
    )

    items_with_discount = (
        order_items
            .join(orders_current, "order_id")
            .join(order_subtotals, "order_id")
            .withColumn("order_discount", coalesce(col("discount_amount"), lit(0.0)))
            .withColumn(
                "allocated_discount",
                when(col("order_subtotal") > 0, col("line_total") / col("order_subtotal") * col("order_discount"))
                    .otherwise(lit(0.0))
            )
    )

    return (
        items_with_discount
        .join(products, "product_id")
        .join(categories, "category_id")
        .groupBy(
            to_date(col("order_date")).alias("revenue_date"),
            col("category_name"),
            col("shipping_state").alias("region")
        )
        .agg(
            _sum("line_total").alias("gross_revenue"),
            _sum("allocated_discount").alias("total_discount"),
            (_sum("line_total") - _sum("allocated_discount")).alias("net_revenue")
        )
    )






































