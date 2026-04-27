BlueAlpha Founding Data Engineer ; data infra system design

---

# BlueAlpha Data Pipeline

Advertising data pipeline that ingests data from Google Ads, Facebook, and a CRM system, validates and transforms it through a medallion architecture (bronze/silver/gold S3 keys), and serves it via Athena for analytics.

## Architecture

```
Vendors / API
      |
      v
[Ingestion]  Google Ads: cron Lambda calls API -> bronze
             Facebook/CRM: exported directly to bronze S3
      |
      v
[Bronze]     s3://bluealpha-data/bronze/{source}/year=/month=/day=/
             Raw files, untouched
      |
      v  (S3 event triggers)
[Transformation Lambdas]
             Pydantic validation, date normalization, deduplication,
             outlier detection. Failures -> quarantine with reason.
      |
      v
[Silver]     s3://bluealpha-data/silver/{source}/year=/month=/day=/
             Validated Parquet, source-specific staging schemas
      |
      v  (Glue ETL job)
[Gold]       s3://bluealpha-data/gold/fct_*/
             Unified fact tables as Parquet, cataloged by Glue Crawler
      |
      v
[Athena]     SQL queries over Glue Data Catalog
```

## Project Structure

```
src/
├── helpers/             Shared utilities (imported by all Lambdas + Glue)
│   ├── constants.py       S3 paths, bucket name, source names
│   ├── date_parser.py     Robust date parsing (dateutil)
│   ├── models.py          Pydantic validation models per source
│   └── s3.py              S3 read/write helpers (pseudo-code)
│
├── lambdas/             Lambda functions
│   ├── bluealpha_google_ingestion.py        Cron -> API -> bronze
│   ├── bluealpha_google_transformation.py   Bronze -> validate -> silver
│   ├── bluealpha_facebook_transformation.py Bronze -> validate -> silver
│   └── bluealpha_crm_transformation.py      Bronze -> validate -> silver
│
├── glue/                Glue ETL
│   └── load_to_gold.py    Silver -> gold fact tables (idempotent merge)
│
└── infra/               Infrastructure config (pseudo-code)
    ├── lambda_triggers.py   S3 events + EventBridge cron
    ├── cloudwatch_alarms.py Lambda failure + quarantine alarms
    ├── glue_crawler.py      Crawler config for gold layer
    └── iam_roles.py         Least-privilege IAM roles
```

## Setup

### Requirements

- Python 3.9+
- Dependencies: `pip install -r requirements.txt`

```
pydantic>=2.0,<3.0
python-dateutil>=2.8
boto3>=1.28
pyarrow>=14.0
```

### Running Locally

The core parsing, validation, and transformation logic is real, runnable Python. Infrastructure code (S3 interactions, Lambda handlers, Glue plumbing) is pseudo-code that documents the production behavior.

To inspect the transformation logic locally:

```python
import json
from src.lambdas.bluealpha_google_transformation import flatten_google_ads_json, validate_and_transform

with open("data/google_ads_api.json") as f:
    raw = json.load(f)

rows = flatten_google_ads_json(raw)
valid, quarantined = validate_and_transform(rows)
```

```python
import csv
from src.lambdas.bluealpha_facebook_transformation import validate_and_transform

with open("data/facebook_export.csv") as f:
    rows = list(csv.DictReader(f))

valid, quarantined = validate_and_transform(rows)
```

```python
import csv
from src.lambdas.bluealpha_crm_transformation import parse_crm_csv, deduplicate_rows, validate_and_transform

with open("data/crm_revenue.csv") as f:
    rows = list(csv.DictReader(f))

rows, dropped = deduplicate_rows(rows)
valid, quarantined = validate_and_transform(rows)
```

### Deployment

In production, each Lambda would be packaged with the `src/helpers/` directory as a Lambda layer.   

Infrastructure would be provisioned via CDK using the configs in `src/infra/` as guides.

See [SUBMISSION_DESIGN.md](SUBMISSION_DESIGN.md) for detailed data quality strategy and architecture decisions.
