from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, sum as _sum, when, datediff, to_date


def compute_fulfillment_health(orders_df: DataFrame, sla_days: int = 5) -> DataFrame:
    orders_current = orders_df.filter(col("__END_AT").isNull())

    delivered = (
        orders_current
        .filter(col("order_status") == "delivered")
        .withColumn(
            "days_to_deliver",
            datediff(to_date(col("updated_at")), to_date(col("order_date"))),
        )
        .withColumn("within_sla", col("days_to_deliver") <= sla_days)
    )

    return (
        delivered
        .groupBy(
            to_date(col("order_date")).alias("order_date"),
            col("shipping_state").alias("region"),
        )
        .agg(
            count("order_id").alias("delivered_orders"),
            _sum(when(col("within_sla"), 1).otherwise(0)).alias("within_sla_orders"),
        )
        .withColumn("sla_compliance_rate", col("within_sla_orders") / col("delivered_orders"))
    )