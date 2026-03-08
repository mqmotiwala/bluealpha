BUCKET = "bluealpha-data"

BRONZE_PREFIX = "bronze"
SILVER_PREFIX = "silver"
GOLD_PREFIX = "gold"
QUARANTINE_PREFIX = "quarantine"

SOURCES = {
    "google_ads": "google_ads",
    "facebook": "facebook",
    "crm": "crm",
}

GOLD_TABLES = {
    "ad_performance": "fct_ad_performance",
    "crm_orders": "fct_crm_orders",
    "reconciliation": "fct_campaign_revenue_reconciliation",
}
