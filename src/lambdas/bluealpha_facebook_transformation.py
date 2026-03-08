"""Facebook Ads Transformation Lambda — bluealpha-facebook-transformation.

Triggered by S3 PutObject events on the bronze/facebook/ prefix.
Reads the raw Facebook CSV export, normalizes dates, fills missing purchases
with 0, validates each row with Pydantic, and writes validated records as
Parquet to the silver layer.

Input:  s3://bluealpha-data/bronze/facebook/year=YYYY/month=MM/day=DD/facebook_export.csv
Output: s3://bluealpha-data/silver/facebook/year=YYYY/month=MM/day=DD/stg_facebook_ads.parquet
"""

import logging

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import ValidationError

from src.helpers.constants import BUCKET, BRONZE_PREFIX, SILVER_PREFIX, SOURCES
from src.helpers.csv_parser import parse_csv
from src.helpers.models import FacebookAdRow
from src.helpers.s3 import (
    build_partitioned_prefix,
    read_s3_object,
    write_parquet_to_s3,
    write_quarantine_record,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def validate_and_transform(rows):
    """Validate each row with Pydantic and map to stg_facebook_ads schema.

    Missing purchases are filled with 0 by the Pydantic model.
    Dates are normalized to YYYY-MM-DD by the model's field validator.

    Returns a tuple of (valid_records, quarantined_records).
    """
    valid = []
    quarantined = []

    for row in rows:
        try:
            record = FacebookAdRow(**row)
        except ValidationError as e:
            logger.warning(f"Validation failed for row {row}: {e}")
            quarantined.append({
                "reason": "validation_error",
                "record": row,
                "errors": str(e),
            })
            continue

        valid.append({
            "platform": "facebook",
            "campaign_id": record.campaign_id,
            "campaign_name": record.campaign_name,
            "date": record.date,
            "impressions": record.impressions,
            "clicks": record.clicks,
            "spend": record.spend,
            "conversions": record.purchases,
            "conversion_value": record.purchase_value,
            "reach": record.reach,
            "frequency": record.frequency,
        })

    return valid, quarantined


def records_to_parquet_table(records):
    """Convert a list of stg_facebook_ads dicts to a PyArrow Table."""
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
        ("reach", pa.int64()),
        ("frequency", pa.float64()),
    ])
    columns = {field.name: [r[field.name] for r in records] for field in schema}
    return pa.table(columns, schema=schema)


def handler(event, context):
    """Lambda entry point. Triggered by S3 PutObject on bronze/facebook/."""
    s3_event = event["Records"][0]["s3"]
    bucket = s3_event["bucket"]["name"]
    key = s3_event["object"]["key"]
    logger.info(f"Processing bronze Facebook file: s3://{bucket}/{key}")

    # Read raw CSV from bronze
    raw_body = read_s3_object(bucket, key)
    rows = parse_csv(raw_body)
    logger.info(f"Parsed {len(rows)} rows from Facebook CSV")

    # Validate and transform
    valid, quarantined = validate_and_transform(rows)
    logger.info(f"Valid: {len(valid)}, Quarantined: {len(quarantined)}")

    # Write quarantined records
    for q in quarantined:
        write_quarantine_record(SOURCES["facebook"], q["reason"], q["record"])

    # Write valid records as Parquet to silver
    if valid:
        table = records_to_parquet_table(valid)
        partition_path = key.replace(BRONZE_PREFIX, SILVER_PREFIX).rsplit("/", 1)[0]
        silver_key = f"{partition_path}/stg_facebook_ads.parquet"
        write_parquet_to_s3(bucket, silver_key, table)
        logger.info(f"Wrote {len(valid)} records to s3://{bucket}/{silver_key}")

    return {"statusCode": 200, "records_processed": len(valid)}
