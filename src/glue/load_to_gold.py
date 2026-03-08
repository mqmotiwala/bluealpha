"""Glue job — load silver Parquet into gold fact tables.

Reads validated Parquet from the silver layer, builds three fact tables
(fct_ad_performance, fct_crm_orders, fct_campaign_revenue_reconciliation),
and writes them as partitioned Parquet to the gold layer. A Glue Crawler
catalogs the gold layer for Athena queries.

Idempotency: for each fact table, read any existing gold Parquet for the
target partition, concat with new data, deduplicate on natural keys (keeping
the latest row), and overwrite the partition. This read-concat-dedup-overwrite
pattern is the S3/Parquet equivalent of a SQL MERGE.

Gold partitioning:
  - fct_ad_performance:  partitioned by platform
  - fct_crm_orders:      partitioned by year/month
  - fct_campaign_revenue_reconciliation: unpartitioned (small table)

In production this runs as an AWS Glue ETL job (PySpark or Python shell).
The core logic below uses PyArrow for local testability.
"""

import logging
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

from src.helpers.constants import BUCKET, GOLD_PREFIX, SILVER_PREFIX

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def read_silver_parquet(source):
    """Read all Parquet files from a silver source prefix into a PyArrow Table."""
    raise NotImplementedError("Requires AWS environment or local silver files")

    # stubbed for local development, in production:
    import pyarrow.dataset as ds
    dataset = ds.dataset(
        f"s3://{BUCKET}/{SILVER_PREFIX}/{source}/",
        format="parquet",
    )
    return dataset.to_table()


def read_existing_gold(table_name, partition_filter=None):
    """Read existing gold Parquet for a table, optionally filtered by partition.

    Returns an empty table with the correct schema if no data exists yet."""

    raise NotImplementedError("Requires AWS environment or local gold files")

    # stubbed for local development, in production:
    import pyarrow.dataset as ds
    path = f"s3://{BUCKET}/{GOLD_PREFIX}/{table_name}/"
    if not s3_prefix_exists(path):
        return None
    dataset = ds.dataset(path, format="parquet", partitioning="hive")
    if partition_filter:
        dataset = dataset.filter(partition_filter)
    return dataset.to_table()

def write_gold_parquet(table_name, table, partition_cols=None):
    """Write a PyArrow Table to the gold layer, optionally partitioned."""

    raise NotImplementedError("Requires AWS environment or local gold path")

    # stubbed for local development, in production:
    pq.write_to_dataset(
        table,
        root_path=f"s3://{BUCKET}/{GOLD_PREFIX}/{table_name}/",
        partition_cols=partition_cols,
        existing_data_behavior="overwrite_or_ignore",
    )


def merge_on_natural_keys(existing, incoming, key_columns):
    """
    This function handles idempotency for the S3 gold layer.

    Merge incoming data into existing data, deduplicating on natural keys.

    Keeps the incoming (latest) row when duplicates are found. This is the
    S3/Parquet equivalent of a SQL MERGE — read, concat, dedup, overwrite.
    """
    if existing is None or existing.num_rows == 0:
        return incoming

    combined = pa.concat_tables([existing, incoming])

    # Deduplication logic here has been referenced as: 
    # convert to pandas, drop duplicates keeping last (incoming), convert back. 

    # For production scale, I would use DynamicFrame or Spark dedup for better performance.
    df = combined.to_pandas()
    df = df.drop_duplicates(subset=key_columns, keep="last")
    return pa.Table.from_pandas(df, schema=incoming.schema, preserve_index=False)


def build_fct_ad_performance():
    """Build fct_ad_performance from silver Google Ads and Facebook Ads data.

    Unifies both platforms into a single campaign/day grain table.
    Schema: platform, campaign_id, campaign_name, date, impressions, clicks,
            spend, conversions, conversion_value
    """
    schema = pa.schema([
        ("platform", pa.string()),
        ("campaign_id", pa.string()),
        ("campaign_name", pa.string()),
        ("date", pa.string()),
        ("impressions", pa.int64()),
        ("clicks", pa.int64()),
        ("spend", pa.float64()),
        ("conversions", pa.int64()),
        ("conversion_value", pa.float64()),
    ])

    # Read silver data for both platforms
    google_silver = read_silver_parquet("google_ads")
    facebook_silver = read_silver_parquet("facebook")

    # Select only the columns in the unified schema (Facebook has extra cols)
    unified_cols = [f.name for f in schema]
    google_subset = google_silver.select(unified_cols)
    facebook_subset = facebook_silver.select(unified_cols)

    incoming = pa.concat_tables([google_subset, facebook_subset])

    # Merge with existing gold data per platform partition
    for platform in ["google", "facebook"]:
        platform_rows = incoming.to_pandas()
        platform_rows = platform_rows[platform_rows["platform"] == platform]
        if platform_rows.empty:
            continue

        platform_table = pa.Table.from_pandas(
            platform_rows, schema=schema, preserve_index=False
        )
        existing = read_existing_gold("fct_ad_performance")
        merged = merge_on_natural_keys(
            existing, platform_table, ["platform", "campaign_id", "date"]
        )
        write_gold_parquet("fct_ad_performance", merged, partition_cols=["platform"])

    logger.info(f"Built fct_ad_performance: {incoming.num_rows} total rows")


def build_fct_crm_orders():
    """Build fct_crm_orders from silver CRM data.

    Order-level fact table partitioned by year/month.
    Schema: order_id, customer_id, order_date, revenue, channel_attributed,
            campaign_source, product_category, region
    """
    crm_silver = read_silver_parquet("crm")
    df = crm_silver.to_pandas()

    # Add partition columns derived from order_date
    df["year"] = df["order_date"].str[:4]
    df["month"] = df["order_date"].str[5:7]

    schema = pa.schema([
        ("order_id", pa.string()),
        ("customer_id", pa.string()),
        ("order_date", pa.string()),
        ("revenue", pa.float64()),
        ("channel_attributed", pa.string()),
        ("campaign_source", pa.string()),
        ("product_category", pa.string()),
        ("region", pa.string()),
        ("year", pa.string()),
        ("month", pa.string()),
    ])

    incoming = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

    existing = read_existing_gold("fct_crm_orders")
    merged = merge_on_natural_keys(existing, incoming, ["order_id"])
    write_gold_parquet("fct_crm_orders", merged, partition_cols=["year", "month"])

    logger.info(f"Built fct_crm_orders: {merged.num_rows} rows")


def build_fct_campaign_revenue_reconciliation():
    """Build fct_campaign_revenue_reconciliation from gold fact tables.

    Reconciles platform-claimed conversion value against CRM actual revenue
    at a campaign/period grain. CRM is the attribution source of truth;
    platform conversion_value is surfaced for visibility only.

    Period grain is deliberate: day-grain reconciliation is meaningless because
    platforms attribute conversions to the click date while CRM records the
    purchase date.

    Schema: campaign_id, period_start, period_end, platform_claimed_revenue,
            crm_actual_revenue, delta
    """
    # Read the gold fact tables we just built
    ad_perf = read_existing_gold("fct_ad_performance")
    crm_orders = read_existing_gold("fct_crm_orders")

    ad_df = ad_perf.to_pandas()
    crm_df = crm_orders.to_pandas()

    # Aggregate platform-claimed revenue by campaign over the full period
    platform_agg = (
        ad_df.groupby("campaign_id")
        .agg(
            period_start=("date", "min"),
            period_end=("date", "max"),
            platform_claimed_revenue=("conversion_value", "sum"),
        )
        .reset_index()
    )

    # Aggregate CRM actual revenue by campaign_source over the full period
    crm_agg = (
        crm_df[crm_df["campaign_source"] != ""]
        .groupby("campaign_source")
        .agg(crm_actual_revenue=("revenue", "sum"))
        .reset_index()
        .rename(columns={"campaign_source": "campaign_id"})
    )

    # Join platform claims with CRM actuals
    reconciled = platform_agg.merge(crm_agg, on="campaign_id", how="left")
    reconciled["crm_actual_revenue"] = reconciled["crm_actual_revenue"].fillna(0.0)
    reconciled["delta"] = (
        reconciled["platform_claimed_revenue"] - reconciled["crm_actual_revenue"]
    )

    schema = pa.schema([
        ("campaign_id", pa.string()),
        ("period_start", pa.string()),
        ("period_end", pa.string()),
        ("platform_claimed_revenue", pa.float64()),
        ("crm_actual_revenue", pa.float64()),
        ("delta", pa.float64()),
    ])

    table = pa.Table.from_pandas(reconciled, schema=schema, preserve_index=False)

    # Unpartitioned — small table, full overwrite each run
    existing = read_existing_gold("fct_campaign_revenue_reconciliation")
    merged = merge_on_natural_keys(existing, table, ["campaign_id"])
    write_gold_parquet("fct_campaign_revenue_reconciliation", merged)

    logger.info(f"Built fct_campaign_revenue_reconciliation: {merged.num_rows} campaigns")


def main():
    """Run all gold layer builds in dependency order."""
    logger.info(f"Starting gold layer build at {datetime.now(timezone.utc).isoformat()}")

    # fct_ad_performance and fct_crm_orders are independent
    build_fct_ad_performance()
    build_fct_crm_orders()

    # Reconciliation depends on the other two being complete
    build_fct_campaign_revenue_reconciliation()

    logger.info("Gold layer build complete")

if __name__ == "__main__":
    main()
