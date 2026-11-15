"""
Gold - gold_fulfillment_health: fulfillment SLA compliance.

Answer Operation team's question: are promised delivery window target being accomplished.

Honest scope note, decided before building this: our schema has no
warehouse_id on orders, so there is no clean join from an order to "the
specific inventory that fulfilled it." Rather than fake a precise join that
the schema doesn't actually support, this table is scoped as an order-timing
SLA metric — order date to delivered date, against a defined promise window.
Inventory is deliberately not joined here; correlating regional stockouts with
delayed orders is a separate, aggregate-level analysis, not a per-order join.

updated_at is used as a proxy for the delivery timestamp — per the schema
document, it's "last status change," so for an order currently in delivered
status, updated_at approximates when delivery happened. This is a documented
simplification, not a precise delivery-event timestamp.
"""
from pyspark import pipelines as dp
from gold_fulfillment_logic import compute_fulfillment_health


@dp.materialized_view(
    name="gold_fulfillment_health",
    comment="Order fulfillment SLA compliance by day and region"
)
def gold_fulfillment_health():
    return compute_fulfillment_health(spark.read.table("silver_orders"))



























