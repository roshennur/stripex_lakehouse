from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, sum as _sum, row_number, desc, coalesce, lit


def compute_product_performance(
    order_items_df: DataFrame,
    products_df: DataFrame,
    inventory_df: DataFrame,
    stockout_threshold: int = 20,
) -> DataFrame:
    sales = (
        order_items_df
        .groupBy("product_id")
        .agg(
            _sum("quantity").alias("units_sold"),
            _sum("line_total").alias("revenue"),
        )
    )

    latest_per_warehouse = (
        inventory_df
        .withColumn(
            "rn",
            row_number().over(Window.partitionBy("product_id", "warehouse_id").orderBy(desc("snapshot_date"))),
        )
        .filter(col("rn") == 1)
    )
    current_stock = (
        latest_per_warehouse
        .groupBy("product_id")
        .agg(_sum("quantity_available").alias("current_stock"))
    )

    return (
        products_df
        .join(sales, "product_id", "left")
        .join(current_stock, "product_id", "left")
        .withColumn("current_stock", coalesce(col("current_stock"), lit(0)))
        .withColumn("stockout_risk", col("current_stock") < stockout_threshold)
        .select("product_id", "product_name", "brand", "units_sold", "revenue", "current_stock", "stockout_risk")
    )