# Data Pipeline Challenge

## Overview

Build a data pipeline that ingests advertising data from multiple "platforms," handles data quality issues, transforms the data into an analytics-ready warehouse schema, and includes orchestration.


**Expected time:** 4 hours.

**When to Ask vs. When to Assume:**

We're evaluating **both** your judgment about when to ask vs. when to assume **and** the quality of your assumptions. Technical implementation details can often be assumed. Business-critical decisions about data relationships and intended use cases may warrant clarification.

## Background

At BlueAlpha, we help marketers understand which channels drive results and how to allocate budgets. To do this, we need to:
1. Ingest data from many different sources (ad platforms, CRMs, spreadsheets)
2. Handle the inevitable data quality issues (missing values, duplicates, format inconsistencies)
3. Transform everything into a unified schema for analysis

This challenge mirrors that work at a smaller scale.

## Deliverables

### 1. Data Ingestion Layer (Python)

Ingest data from the 3 mock sources provided in the `data/` directory:

| File | Description |
|------|-------------|
| `google_ads_api.json` | Simulated API response |
| `facebook_export.csv` | CSV export file |
| `crm_revenue.csv` | CRM export |

### 2. Data Quality & Validation

- Implement validation checks for each source
- Handle: missing values, duplicates, format inconsistencies, invalid data

### 3. Transformation Layer (SQL/dbt-style)

Design a normalized warehouse schema and write transformations. You can use dbt, SQLAlchemy, raw SQL, or Python—whatever you're most comfortable with.

### 4. Orchestration

Define a DAG/workflow that:
- Handles dependencies between ingestion, validation, and transformation
- Includes retry logic for failures
- Is idempotent (safe to re-run)

**Note:** Working code is preferred, but well-documented pseudo-code/config is acceptable if local orchestrator setup is complex.

### 5. Documentation

Include in your submission:

**README.md** with:
- Setup instructions (how to run your pipeline)
- Dependencies and requirements

**DESIGN.md** (~1-2 pages) covering:
- Schema design decisions and rationale
- Data quality strategy
- Trade-offs made given time constraints
- What you'd do differently with more time
- **Architecture Decision** for major decisions:
  - For key decisions, document: the options considered, your chosen approach, why you chose it, and what trade-offs you accepted
  - Examples: CRM data handling, outlier treatment, orchestration tool choice


## Submission

Submit your solution as a GitHub repository (public or private with access granted). Include:
- All source code
- README with setup instructions
- DESIGN.md with your design document (including ADRs)
- Any additional documentation you think is helpful

## After Submission

We'll schedule a 30-45 minute live walkthrough where you'll:
1. Walk us through your design decisions
2. Explain how you handled specific data quality issues
3. Discuss what you'd change with more time
4. Answer questions about scaling and edge cases

This is a conversation, not a test. We're interested in your thinking process and how you approach problems.

