"""
Gold — gold_customer_360: lifetime value, order frequency, churn signal.
Answer Marketing questions: who's valuable, who's about to churn.

Same SCD Type 2 rule as gold_revenue_logic.py — silver_customers and
silver_orders both carry history, so both get filtered to their current row
before anything else happens.

"Churn signal" is kept descriptive, not predictive — days_since_last_order is
a number a stakeholder can read and act on themselves, not a model output
someone has to trust blindly. This project isn't building a churn model.
"""

from pyspark import pipelines as dp
from datetime import timezone, datetime
from gold_customer_logic import compute_customer_360

@dp.materialized_view(
    name="gold_customer_360",
    comment="Lifetime value, order frequency, and a descriptive churn signal."
)
def gold_customer_360():
    return compute_customer_360(
        customers_df=spark.read.table("silver_customers"),
        orders_df=spark.read.table("silver_orders"),
        as_of_date=datetime.now(timezone.utc).date(),
    )