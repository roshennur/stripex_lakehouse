"""
StripeX — Post-Run Summary Report

Runs as a Python script task, the final step in stripex-orchestration-job,
after transformation completes. Prints a lightweight run summary — row counts
across bronze, silver, and gold, plus every quarantine pair's current count.

Scope, deliberately limited: this is a plain per-run summary, visible in the job
run's logs. It is NOT the DQ dashboard, trend lines, or alerting — that's real
infrastructure, built properly in later, querying the pipeline event
log. This script exists so a run's health is visible immediately, in the job
log itself, without waiting for that infrastructure to exist.

Runs via a spark_python_task — `spark` is available as a global in this
execution context, the same as in a notebook. Parameters arrive as command-line
arguments, not dbutils.widgets, since this is a script task, not a notebook task.
"""

import sys
from datetime import datetime, timezone

def get_arg(name: str, default: str) -> str:
    prefix = f"--{name}="
    for arg in sys.argv[1:]:
        if arg.startswith(prefix):
            return arg[len(prefix):]
    return default

catalog = get_arg("catalog", "dev")
run_date = get_arg("run_date", datetime.now(timezone.utc).date().isoformat())


BRONZE_TABLES = [
    "bronze_orders", "bronze_order_items", "bronze_customers",
    "bronze_products", "bronze_categories", "bronze_clickstream", "bronze_inventory",
]
SILVER_TABLES = [
    "silver_orders", "silver_customers", "silver_order_items",
    "silver_products", "silver_categories", "silver_clickstream", "silver_inventory",
]
GOLD_TABLES = [
    "gold_daily_revenue", "gold_customer_360", "gold_product_performance",
    "gold_funnel_analysis", "gold_fulfillment_health",
]


# Bronze only. _ingested_at is genuine processing time — stamped when Bronze
# actually wrote the row. Silver has no equivalent: every date column it has
# (order_date, event_timestamp, snapshot_date, ts_ms...) describes a business
# event, not when Silver processed it. So Silver reports total only, same as it should

DATE_COLUMNS = {
    "bronze_orders": ("_ingested_at", "timestamp"),
    "bronze_order_items": ("_ingested_at", "timestamp"),
    "bronze_customers": ("_ingested_at", "timestamp"),
    "bronze_products": ("_ingested_at", "timestamp"),
    "bronze_categories": ("_ingested_at", "timestamp"),
    "bronze_clickstream": ("_ingested_at", "timestamp"),
    "bronze_inventory": ("_ingested_at", "timestamp"),
    "silver_orders": ("ts_ms", "epoch_ms"),
    "silver_customers": ("ts_ms", "epoch_ms"),
    "silver_order_items": ("ts_ms", "epoch_ms")
}

QUARANTINE_PAIRS = [
    ("bronze_orders_valid", "bronze_orders_quarantined", "_ingested_at", "timestamp"),
    ("bronze_order_items_valid", "bronze_order_items_quarantined", "_ingested_at", "timestamp"),
    ("bronze_customers_valid", "bronze_customers_quarantined", "_ingested_at", "timestamp"),
    ("bronze_products_valid", "bronze_products_quarantined", "_ingested_at", "timestamp"),
    ("bronze_categories_valid", "bronze_categories_quarantined", "_ingested_at", "timestamp"),
    ("bronze_clickstream_valid", "bronze_clickstream_quarantined", "_ingested_at", "timestamp"),
    ("bronze_inventory_valid", "bronze_inventory_quarantined", "_ingested_at", "timestamp"),
    ("silver_order_items", "silver_order_items_quarantined", "ts_ms", "epoch_ms")
]

def date_filter(date_column: str, date_column_type: str) -> str:
    if date_column_type == "epoch_ms":
        return f"date(from_unixtime({date_column} / 1000)) = '{run_date}'"
    return f"date({date_column}) = '{run_date}'" 


def count_table(name: str, today_only: bool = False) -> int:
    try:
        df = spark.table(f"{catalog}.stripex.{name}")
        if today_only and name in DATE_COLUMNS:
            col, _type = DATE_COLUMNS[name]
            df = df.filter(date_filter(col, _type))
        return df.count()
    except Exception:
        return -1 # table doesn't exist or unreadable

def fmt(count: int) -> str:
    return str(count) if count >= 0 else "ERROR"


def print_section(title: str, tables: list[str], show_today: bool) -> None:
    print(f"\n{title}:")
    for t in tables:
        total = count_table(t)
        if show_today and t in DATE_COLUMNS:
            today = count_table(t, today_only=True)
            print(f"    {t:<32} total={fmt(total):<8} today={fmt(today)}")
        else:
            print(f"    {t:<32} total={fmt(total)}")


print(f"=== StripeX run Summary — catalog={catalog}, run_date={run_date} ===")

print_section("Bronze", BRONZE_TABLES, show_today=True)
print_section("Silver", SILVER_TABLES, show_today=False)
print_section("Gold", GOLD_TABLES, show_today=False)  # materialized views — "today" isn't meaningful here


print("\nQuarantine (scoped to run_date):")
for valid_table, quarantined_table, date_column, date_type in QUARANTINE_PAIRS:
    filter_expr = date_filter(date_column, date_type)
    try:
        valid_today = spark.table(f"{catalog}.stripex.{valid_table}").filter(filter_expr).count()
        quarantined_today = spark.table(f"{catalog}.stripex.{quarantined_table}").filter(filter_expr).count()
        total_today = valid_today + quarantined_today
        rate = (quarantined_today / total_today) if total_today > 0 else 0.0
        print(f"  {quarantined_table:<32} {quarantined_today} quarantined of {total_today} total ({rate:.1%})")
    except Exception:
        print(f"  {quarantined_table:<32} ERROR")










































