"""S3 utilities for BlueAlpha data pipeline - used by Lambdas & Glue script.

Note:
most of these are stubbed for local development; in production these run in AWS
Present, they serve as documented interfaces showing the expected S3 interactions.
"""

import json
from datetime import datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from src.helpers.constants import BUCKET, QUARANTINE_PREFIX


def get_s3_client():
    """Return a boto3 S3 client."""
    raise NotImplementedError("S3 client requires AWS environment")

    # Note: Stubbed for local development.
    return boto3.client("s3")


def build_partitioned_prefix(base_prefix, source, dt=None):
    """Build a Hive-style partitioned S3 prefix.

    Example: bronze/google_ads/year=2024/month=01/day=15/
    """
    if dt is None:
        dt = datetime.now(timezone.utc)
    return (
        f"{base_prefix}/{source}/"
        f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
    )


def read_s3_object(bucket, key):
    """Read an object from S3 and return its bytes."""

    raise NotImplementedError("Requires AWS environment")

    # stubbed for local development, in production:
    s3 = get_s3_client()
    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()

def write_s3_object(bucket, key, body):
    """Write bytes or string to an S3 object."""

    raise NotImplementedError("Requires AWS environment")

    # stubbed for local development, in production:
    s3 = get_s3_client()
    s3.put_object(Bucket=bucket, Key=key, Body=body)

def write_parquet_to_s3(bucket, key, table):
    """Write a PyArrow Table as Parquet to S3.

    Args:
        bucket: S3 bucket name.
        key: Full S3 key including .parquet extension.
        table: A pyarrow.Table to serialize.
    """
    raise NotImplementedError("Requires AWS environment")

    # stubbed for local development, in production:
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf)
    write_s3_object(bucket, key, buf.getvalue().to_pybytes())

def write_quarantine_record(source, reason, record, dt=None):
    """Write a failed validation record to the quarantine prefix.

    Writes a JSON file to:
        s3://bluealpha-data/quarantine/{source}/year=YYYY/month=MM/day=DD/
        reason={reason}/{timestamp}.json

    Args:
        source: Source name (google_ads, facebook, crm).
        reason: Validation failure reason (e.g. null_revenue, suspected_outlier).
        record: Dict of the failed row data.
        dt: Optional datetime for partitioning; defaults to utcnow.
    """
    
    if dt is None:
        dt = datetime.now(timezone.utc)
    prefix = build_partitioned_prefix(QUARANTINE_PREFIX, source, dt)
    key = f"{prefix}reason={reason}/{dt.strftime('%Y%m%d%H%M%S%f')}.json"
    body = json.dumps(record, default=str)

    raise NotImplementedError("Requires AWS environment")
    
    # stubbed for local development, in production:
    write_s3_object(BUCKET, key, body.encode("utf-8"))
