# Architecture Overview

## olist-de-pipeline

End-to-end data engineering pipeline for the [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

---

## Layers

```
Kaggle (Source)
      │
      ▼
[Bronze Layer]  s3://olist-raw-105906274703/bronze/olist/
      │         Raw CSVs ingested as-is via Lambda
      │
      ▼
[Silver Layer]  s3://olist-processed-105906274703/silver/
      │         Cleaned, typed, deduplicated (Databricks)
      │
      ▼
[Gold Layer]    s3://olist-processed-105906274703/gold/
                Aggregated, business-ready tables (Databricks)
```

---

## Components

| Component | Technology | Purpose |
|---|---|---|
| Bronze Ingestion | AWS Lambda + Kaggle API | Download raw CSVs → S3 |
| Orchestration | Amazon EventBridge | Weekday schedule at 18:00 UTC |
| Silver / Gold | Databricks (planned) | Transform and aggregate |
| Streaming | Amazon Kinesis (planned) | Real-time order events |
| Secrets | AWS Secrets Manager | Kaggle API credentials |
| IaC | AWS CloudFormation | All AWS resource provisioning |
| CI/CD | GitHub Actions + OIDC | Lint, test, deploy on push to main |

---

## Data Flow (Batch)

1. EventBridge triggers `olist-ingestor` Lambda on weekdays at 18:00 UTC
2. Lambda fetches Kaggle credentials from Secrets Manager (`olist/kaggle-credentials`)
3. Lambda downloads the Olist dataset via Kaggle API to `/tmp`
4. Each CSV is uploaded to `s3://olist-raw-105906274703/bronze/olist/<filename>`

---

*Streaming and Databricks sections to be documented as those layers are built.*
