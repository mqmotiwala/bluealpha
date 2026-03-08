"""Google Ads Transformation Lambda — bluealpha-google-transformation.

Triggered by S3 PutObject events on the bronze/google_ads/ prefix.
Reads the raw Google Ads API JSON, flattens the nested campaign/daily_metrics
structure, validates each record with Pydantic, converts cost_micros to spend,
and writes validated records as Parquet to the silver layer.

Input:  s3://bluealpha-data/bronze/google_ads/year=YYYY/month=MM/day=DD/google_ads_api.json
Output: s3://bluealpha-data/silver/google_ads/year=YYYY/month=MM/day=DD/stg_google_ads.parquet
"""

import json
import logging

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import ValidationError

from src.helpers.constants import BUCKET, BRONZE_PREFIX, SILVER_PREFIX, SOURCES
from src.helpers.models import GoogleAdMetric
from src.helpers.s3 import (
    build_partitioned_prefix,
    read_s3_object,
    write_parquet_to_s3,
    write_quarantine_record,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

COST_MICROS_DIVISOR = 1_000_000


def flatten_google_ads_json(raw_json):
    """Flatten nested Google Ads API response into a list of flat row dicts.

    The API response nests daily_metrics inside each campaign object. This
    function denormalizes that into one dict per campaign-day, carrying the
    campaign-level fields into every row.
    """
    data = json.loads(raw_json) if isinstance(raw_json, (str, bytes)) else raw_json
    rows = []
    for campaign in data.get("campaigns", []):
        for metric in campaign.get("daily_metrics", []):
            rows.append({
                "campaign_id": campaign["campaign_id"],
                "campaign_name": campaign["campaign_name"],
                **metric,
            })
    return rows


def validate_and_transform(rows):
    """Validate each row with Pydantic and convert cost_micros to spend.

    Returns a tuple of (valid_records, quarantined_records).
    Valid records are dicts in the stg_google_ads schema.
    """
    valid = []
    quarantined = []

    for row in rows:
        try:
            record = GoogleAdMetric(**row)
        except ValidationError as e:
            logger.warning(f"Validation failed for row {row}: {e}")
            quarantined.append({
                "reason": "validation_error",
                "record": row,
                "errors": str(e),
            })
            continue

        valid.append({
            "platform": "google",
            "campaign_id": record.campaign_id,
            "campaign_name": record.campaign_name,
            "date": record.date,
            "impressions": record.impressions,
            "clicks": record.clicks,
            "spend": record.cost_micros / COST_MICROS_DIVISOR,
            "conversions": record.conversions,
            "conversion_value": record.conversion_value,
        })

    return valid, quarantined


def records_to_parquet_table(records):
    """Convert a list of stg_google_ads dicts to a PyArrow Table."""
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
    columns = {field.name: [r[field.name] for r in records] for field in schema}
    return pa.table(columns, schema=schema)


def handler(event, context):
    """Lambda entry point. Triggered by S3 PutObject on bronze/google_ads/."""
    s3_event = event["Records"][0]["s3"]
    bucket = s3_event["bucket"]["name"]
    key = s3_event["object"]["key"]
    logger.info(f"Processing bronze Google Ads file: s3://{bucket}/{key}")

    # Read raw JSON from bronze
    raw_body = read_s3_object(bucket, key)
    rows = flatten_google_ads_json(raw_body)
    logger.info(f"Flattened {len(rows)} rows from Google Ads JSON")

    # Validate and transform
    valid, quarantined = validate_and_transform(rows)
    logger.info(f"Valid: {len(valid)}, Quarantined: {len(quarantined)}")

    # Write quarantined records
    for q in quarantined:
        write_quarantine_record(SOURCES["google_ads"], q["reason"], q["record"])

    # Write valid records as Parquet to silver
    if valid:
        table = records_to_parquet_table(valid)
        # Derive silver path from the bronze key's partition structure
        partition_path = key.replace(BRONZE_PREFIX, SILVER_PREFIX).rsplit("/", 1)[0]
        silver_key = f"{partition_path}/stg_google_ads.parquet"
        write_parquet_to_s3(bucket, silver_key, table)
        logger.info(f"Wrote {len(valid)} records to s3://{bucket}/{silver_key}")

    return {"statusCode": 200, "records_processed": len(valid)}
