# Design Document

## Schema Design

### Medallion Architecture

Data ingestion is handled via bronze/silver/gold layers in S3. 

##### Bronze layer
Stores raw source files exactly as received without any parsing or transformation work.  

This preserves an audit trail and allows reprocessing if transformation logic changes.

##### Silver layer
Holds validated, source-specific staging tables in Parquet format. Each source keeps its own schema at this layer.  
This is important because each source has its own unique ingestion quirks & possible errors to enforce during the validation step.  

##### Gold layer
Contains unified fact tables ready for analytics.  
I made partition decisions based on assumed access patterns for the BlueAlpha AI agent.

- **fct_ad_performance** (campaign/day grain): merges Google and Facebook ad data into one table. Partitioned by platform because the primary query pattern is assumed to be per-platform performance analysis.
- **fct_crm_orders** (order grain): validated CRM orders. Partitioned by year/month for time-range queries.
- **fct_campaign_revenue_reconciliation** (campaign/period grain): joins platform-claimed conversion value against CRM actual revenue. Unpartitioned — small table, fully overwritten each run.

### Reconciliation Table Design

The reconciliation table uses period grain deliberately. Day-grain reconciliation is meaningless because ad platforms attribute conversions to the click date while the CRM records the purchase date, for example: a purchase on January 10th may be attributed to a January 3rd click.  

CRM revenue is the attribution source of truth (we confirmed the BlueAlpha software is designed as such, via email); platform conversion_value is surfaced for visibility and comparison only if needed. 

### Idempotency

S3/Parquet has no native MERGE. Idempotency is implemented as read-concat-dedup-overwrite: read the existing gold partition, concatenate incoming rows, deduplicate on natural keys (keeping the latest), and overwrite. Natural keys:
- fct_ad_performance: (platform, campaign_id, date)
- fct_crm_orders: (order_id)
- fct_campaign_revenue_reconciliation: (campaign_id)

At modest data scales, this is OK.  
But I could be concerned about this as gold data scales. There are solutions (partition pruning, migrating to Iceberg -- we can discuss this in more detail when we chat.)

## Data Quality Strategy

### Validation Approach

Each source has a Pydantic model that enforces schema constraints and applies normalizations (date parsing, channel lowercasing, missing value defaults). 
Rows that fail Pydantic validation are routed to quarantine with a specific failure reason. 
Rows that pass schema validation but fail business rules (null revenue, null customer_id, suspected outliers) are quarantined separately.

Quarantine files land in `quarantine/{source}/year=.../reason={reason}/`.  
The reason= partition is a good design choice for triaging & engineering speed.

### Per-Source Handling

**Google Ads**: This source was pretty clean overall. Only transformation is dividing cost_micros by 1,000,000 to normalize spend to dollars.

**Facebook**: 
- Mixed date formats (YYYY-MM-DD, MM/DD/YYYY, DD-Mon-YYYY, etc.) normalized via dateutil parser.  
There is also an explicit catch for MMM dd, YYYY format which would typically break csv parsing.  
- Missing purchases on some rows is treated as 0.

**CRM**:
- Null revenue (ORD-10008): quarantined with reason=null_revenue. Dropping or allowing it to pass feels like the wrong judgement call. 
- Null customer_id (ORD-10030): quarantined with reason=null_customer_id. 
- Duplicate order_ids (ORD-10021, ORD-10056, ORD-10071): identical rows deduplicated by keeping the first occurrence and logging the drop.
- Channel case inconsistencies (Google, FACEBOOK, google): normalized to lowercase.
- Mixed date formats: same dateutil parser logic as Facebook.
- Revenue outlier (ORD-10081, \$9,999,999.99) quarantined with reason=suspected_outlier. Threshold is mean + 3 standard deviations. This is a judgement call, we can tune this for production. But I like the idea of having this check in place. 
- Negative revenue (ORD-10076, -$50.00): I allowed this to pass through. Negative revenue could be a refund ¯\\\_(ツ)_/¯.
- Missing campaign_source (ORD-10081): allowed through as empty string. It's valid that there was no attribution cookie for some orders.

## Architecture Decisions

### ADR 1: Athena/Lakehouse over Redshift

**Options considered**: (1) Redshift data warehouse, (2) S3 + Glue + Athena lakehouse.

**Chosen**: Athena lakehouse.

**Rationale**: The primary consumer is an AI agent, not a BI dashboard with concurrent human users, so Athena's serverless, pay-per-query model is a better fit. It results in not having to manage a provisioned cluster (i.e. no idle compute costs).

Also, I'm assuming  the agent's query patterns (periodic batch analysis) doesn't need Redshift's sub-second concurrency. 

**Trade-off**: If the product evolves toward human-facing dashboards with many concurrent users, Redshift (or Redshift Serverless) would provide better latency under load. 

Migration would be easy: the gold Parquet files can be loaded into Redshift via COPY.

### ADR 2: Facebook/CRM Ingestion

Facebook and CRM data was received as CSV files. As such, there was no mock ingestion pipeline to demonstrate, and we assumed the raw data will reach out S3 buckets directly.  

In any case, it's worth considering what that design pattern would look like for production.  

We may set up vendor-side software to produce the data exports, and push it to the bronze S3 layer directly, or to an intermediary point via SFTP where BlueAlpha side software can pick it up and move it to the bronze S3 layer.  

In any case, we rely on S3 PutObject event triggers to initiate the transformation Lambdas.
