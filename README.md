# olist-de-pipeline

End-to-end data engineering pipeline on AWS for the Olist Brazilian E-Commerce dataset — covering batch ingestion, real-time streaming, and Databricks transformations.

---

## Architecture Overview

```
Kaggle (Source)
      │
      ▼
[Bronze]  s3://olist-raw-105906274703/bronze/olist/       ← Raw CSVs (Lambda)
      │
      ▼
[Silver]  s3://olist-processed-105906274703/silver/       ← Cleaned (Databricks)
      │
      ▼
[Gold]    s3://olist-processed-105906274703/gold/         ← Aggregated (Databricks)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | AWS Lambda, Kaggle API |
| Storage | Amazon S3 |
| Orchestration | Amazon EventBridge |
| Streaming | Amazon Kinesis *(planned)* |
| Transformation | Databricks *(planned)* |
| Secrets | AWS Secrets Manager |
| IaC | AWS CloudFormation |
| CI/CD | GitHub Actions (OIDC) |

---

## Folder Structure

```
olist-de-pipeline/
├── .github/
│   └── workflows/
│       └── ci.yml              # CI/CD — lint, test, deploy
├── batch_pipeline/
│   └── lambda/
│       ├── olist_ingestor.py   # Bronze ingestion Lambda
│       └── requirements.txt    # Lambda runtime dependencies
├── streaming_pipeline/         # Kinesis streaming (coming soon)
├── databricks/                 # Silver/Gold notebooks (coming soon)
├── infra/
│   └── cloudformation/
│       └── template.yml        # All AWS resources
├── docs/
│   └── architecture.md         # Architecture deep-dive
├── .gitignore
└── README.md
```

---

## Setup

### Prerequisites
- AWS CLI configured with appropriate IAM permissions
- GitHub repository secrets set:
  - `AWS_ROLE_ARN` — IAM role ARN for OIDC authentication

### Deploy Infrastructure

```bash
aws cloudformation deploy \
  --stack-name olist-de-pipeline \
  --template-file infra/cloudformation/template.yml \
  --region ap-south-1 \
  --capabilities CAPABILITY_NAMED_IAM
```

### Update Kaggle Credentials

After deploying, update the dummy values in AWS Secrets Manager:

```bash
aws secretsmanager update-secret \
  --secret-id olist/kaggle-credentials \
  --secret-string '{"KAGGLE_USERNAME": "<your-username>", "KAGGLE_KEY": "<your-api-key>"}' \
  --region ap-south-1
```

### CI/CD

Push to `main` → GitHub Actions automatically:
1. Lints with `flake8`
2. Runs `pytest`
3. Packages and uploads Lambda artifact to S3
4. Deploys CloudFormation stack

---

## License

MIT
