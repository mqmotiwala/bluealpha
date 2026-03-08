"""Glue Crawler configuration — pseudo-code.

Defines the Glue Crawler that catalogs gold layer Parquet files into the
Glue Data Catalog, making them queryable via Athena.

In production this would be defined via AWS CDK.
"""

GLUE_DATABASE = {
    "name": "bluealpha_warehouse",
    "description": "BlueAlpha analytics warehouse — gold layer fact tables.",
}

GLUE_CRAWLER = {
    "name": "bluealpha-gold-crawler",
    "role": "arn:aws:iam::ACCOUNT_ID:role/bluealpha-glue-service-role",
    "database_name": GLUE_DATABASE["name"],

    # Crawl each gold table prefix independently so the crawler infers
    # the correct partition structure per table.
    "targets": {
        "s3_targets": [
            {
                "path": "s3://bluealpha-data/gold/fct_ad_performance/",
                "exclusions": [],
            },
            {
                "path": "s3://bluealpha-data/gold/fct_crm_orders/",
                "exclusions": [],
            },
            {
                "path": "s3://bluealpha-data/gold/fct_campaign_revenue_reconciliation/",
                "exclusions": [],
            },
        ],
    },

    "schema_change_policy": {
        # Add new columns automatically; don't delete columns that disappear
        # (protects against partial file writes).
    },

    "configuration": {
        # Group Parquet files under each prefix into a single table,
        # treating Hive-style partition keys (platform=, year=, month=)
        # as partition columns.
    },
}

# Expected Glue Catalog tables after crawling:
#
# bluealpha_warehouse.fct_ad_performance
#   Columns: campaign_id, campaign_name, date, impressions, clicks, spend,
#            conversions, conversion_value
#   Partition keys: platform (string)
#
# bluealpha_warehouse.fct_crm_orders
#   Columns: order_id, customer_id, order_date, revenue, channel_attributed,
#            campaign_source, product_category, region
#   Partition keys: year (string), month (string)
#
# bluealpha_warehouse.fct_campaign_revenue_reconciliation
#   Columns: campaign_id, period_start, period_end, platform_claimed_revenue,
#            crm_actual_revenue, delta
#   Partition keys: (none)
