"""CRM Transformation Lambda — bluealpha-crm-transformation.

Triggered by S3 PutObject events on the bronze/crm/ prefix.
Reads the raw CRM revenue CSV, validates each row with Pydantic, quarantines
rows with null revenue or null customer_id, deduplicates on order_id,
quarantines suspected revenue outliers, normalizes channels to lowercase,
and writes validated records as Parquet to the silver layer.

Input:  s3://bluealpha-data/bronze/crm/year=YYYY/month=MM/day=DD/crm_revenue.csv
Output: s3://bluealpha-data/silver/crm/year=YYYY/month=MM/day=DD/stg_crm_orders.parquet
"""

import logging
import statistics

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import ValidationError

from src.helpers.constants import BUCKET, BRONZE_PREFIX, SILVER_PREFIX, SOURCES
from src.helpers.csv_parser import parse_csv
from src.helpers.models import CrmOrderRow
from src.helpers.s3 import (
    build_partitioned_prefix,
    read_s3_object,
    write_parquet_to_s3,
    write_quarantine_record,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Outlier threshold: rows with revenue > mean + OUTLIER_STDDEV_THRESHOLD * stddev
# are quarantined as suspected data entry errors.
OUTLIER_STDDEV_THRESHOLD = 3


def deduplicate_rows(rows):
    """Remove duplicate rows by order_id, keeping the first occurrence.

    Returns a tuple of (unique_rows, dropped_count).
    """
    seen = set()
    unique = []
    dropped = 0
    for row in rows:
        order_id = row.get("order_id", "")
        if order_id in seen:
            logger.info(f"Dropping duplicate order_id: {order_id}")
            dropped += 1
            continue
        seen.add(order_id)
        unique.append(row)
    return unique, dropped


def compute_outlier_threshold(valid_records):
    """Compute the revenue outlier threshold as mean + 3 * stddev.

    Only considers records with non-None revenue. Returns None if fewer
    than 2 records are available (can't compute stddev).
    """
    revenues = [r.revenue for r in valid_records if r.revenue is not None]
    if len(revenues) < 2:
        return None
    mean = statistics.mean(revenues)
    stddev = statistics.stdev(revenues)
    return mean + OUTLIER_STDDEV_THRESHOLD * stddev


def validate_and_transform(rows):
    """Validate, quarantine, and transform CRM rows.

    Validation pipeline:
    1. Parse each row through Pydantic (normalizes dates, lowercases channel)
    2. Check business rules (null revenue, null customer_id) via get_quarantine_reason
    3. After all rows parsed, compute outlier threshold and quarantine suspected outliers

    Returns a tuple of (valid_records, quarantined_records).
    """
    parsed = []
    quarantined = []

    # Step 1 & 2: Pydantic validation + business rule checks
    for row in rows:
        try:
            record = CrmOrderRow(**row)
        except ValidationError as e:
            logger.warning(f"Validation failed for {row.get('order_id')}: {e}")
            quarantined.append({
                "reason": "validation_error",
                "record": row,
            })
            continue

        quarantine_reason = record.get_quarantine_reason()
        if quarantine_reason:
            logger.info(f"Quarantining {record.order_id}: {quarantine_reason}")
            quarantined.append({
                "reason": quarantine_reason,
                "record": record.model_dump(),
            })
            continue

        parsed.append(record)

    # Step 3: Outlier detection on revenue
    threshold = compute_outlier_threshold(parsed)
    valid = []
    if threshold is not None:
        for record in parsed:
            if record.revenue > threshold:
                logger.info(
                    f"Quarantining {record.order_id}: suspected_outlier (revenue={record.revenue:.2f}, threshold={threshold:.2f})"
                )
                quarantined.append({
                    "reason": "suspected_outlier",
                    "record": record.model_dump(),
                })
            else:
                valid.append(record)
    else:
        valid = parsed

    return valid, quarantined


def records_to_parquet_table(records):
    """Convert a list of validated CrmOrderRow instances to a PyArrow Table."""
    schema = pa.schema([
        ("order_id", pa.string()),
        ("customer_id", pa.string()),
        ("order_date", pa.string()),
        ("revenue", pa.float64()),
        ("channel_attributed", pa.string()),
        ("campaign_source", pa.string()),
        ("product_category", pa.string()),
        ("region", pa.string()),
    ])
    columns = {
        "order_id": [r.order_id for r in records],
        "customer_id": [r.customer_id for r in records],
        "order_date": [r.order_date for r in records],
        "revenue": [r.revenue for r in records],
        "channel_attributed": [r.channel_attributed for r in records],
        "campaign_source": [r.campaign_source for r in records],
        "product_category": [r.product_category for r in records],
        "region": [r.region for r in records],
    }
    return pa.table(columns, schema=schema)


def handler(event, context):
    """Lambda entry point. Triggered by S3 PutObject on bronze/crm/."""
    s3_event = event["Records"][0]["s3"]
    bucket = s3_event["bucket"]["name"]
    key = s3_event["object"]["key"]
    logger.info(f"Processing bronze CRM file: s3://{bucket}/{key}")

    # Read raw CSV from bronze
    raw_body = read_s3_object(bucket, key)
    rows = parse_csv(raw_body)
    logger.info(f"Parsed {len(rows)} rows from CRM CSV")

    # Deduplicate
    rows, dropped = deduplicate_rows(rows)
    logger.info(f"Deduplicated: kept {len(rows)}, dropped {dropped}")

    # Validate and transform
    valid, quarantined = validate_and_transform(rows)
    logger.info(f"Valid: {len(valid)}, Quarantined: {len(quarantined)}")

    # Write quarantined records
    for q in quarantined:
        write_quarantine_record(SOURCES["crm"], q["reason"], q["record"])

    # Write valid records as Parquet to silver
    if valid:
        table = records_to_parquet_table(valid)
        partition_path = key.replace(BRONZE_PREFIX, SILVER_PREFIX).rsplit("/", 1)[0]
        silver_key = f"{partition_path}/stg_crm_orders.parquet"
        write_parquet_to_s3(bucket, silver_key, table)
        logger.info(f"Wrote {len(valid)} records to s3://{bucket}/{silver_key}")

    return {"statusCode": 200, "records_processed": len(valid)}
