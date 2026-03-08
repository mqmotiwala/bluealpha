"""Google Ads Ingestion Lambda — bluealpha-google-ingestion.

Cron-triggered (EventBridge schedule). Calls the Google Ads API, retrieves
campaign performance data, and writes the raw JSON response to the bronze
layer in S3. No transformation or validation happens here.

Trigger: EventBridge cron rule (e.g. daily at 06:00 UTC).
Output:  s3://bluealpha-data/bronze/google_ads/year=YYYY/month=MM/day=DD/google_ads_api.json

Facebook and CRM sources do not have ingestion Lambdas — vendors upload files
directly to the bronze prefix, which triggers transformation Lambdas. See
DESIGN.md for rationale.
"""

import json
import logging
from datetime import datetime, timezone

from src.helpers.constants import BUCKET, BRONZE_PREFIX, SOURCES
from src.helpers.s3 import build_partitioned_prefix, write_s3_object

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Pseudo-code: Google Ads API client configuration.
# In production this would use google-ads-python with OAuth credentials
# stored in AWS Secrets Manager.
#
# from google.ads.googleads.client import GoogleAdsClient
# GOOGLE_ADS_CLIENT_CONFIG = {
#     "developer_token": "{{secrets_manager:google-ads/developer-token}}",
#     "client_id": "{{secrets_manager:google-ads/client-id}}",
#     "client_secret": "{{secrets_manager:google-ads/client-secret}}",
#     "refresh_token": "{{secrets_manager:google-ads/refresh-token}}",
#     "login_customer_id": "123-456-7890",
# }


def fetch_google_ads_report(date):
    """Call the Google Ads API and return campaign performance data as a dict.

    In production this builds a GAQL query for CAMPAIGN_PERFORMANCE_REPORT
    filtered to the target date, executes it via the Google Ads API client,
    and returns the raw response as a Python dict.
    """
    raise NotImplementedError("Requires Google Ads API credentials")


def handler(event, context):
    """Lambda entry point. Triggered by EventBridge cron schedule."""
    now = datetime.now(timezone.utc)
    logger.info(f"Google Ads ingestion started at {now.isoformat()}")

    # Fetch raw data from the Google Ads API
    raw_data = fetch_google_ads_report(now.strftime("%Y-%m-%d"))

    # Write raw JSON to bronze layer, untouched
    prefix = build_partitioned_prefix(BRONZE_PREFIX, SOURCES["google_ads"], now)
    key = f"{prefix}google_ads_api.json"
    write_s3_object(BUCKET, key, json.dumps(raw_data).encode("utf-8"))

    logger.info(f"Wrote raw Google Ads data to s3://{BUCKET}/{key}")
    return {"statusCode": 200, "body": f"Ingested to {key}"}
