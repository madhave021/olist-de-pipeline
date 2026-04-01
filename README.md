# Olist E-Commerce Data Pipeline

An end-to-end data engineering pipeline built on AWS for processing the [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). It covers batch ingestion, real-time streaming, infrastructure-as-code, and an AI-powered analytics agent.

## Architecture

```
Raw Data (S3)
    │
    ├── Batch Pipeline (PySpark)  ──► Processed Data (S3 Parquet)
    │
    └── Streaming Pipeline (Kinesis + Kafka)  ──► Real-time Events
                                                        │
                                              AI Agent (Claude / Anthropic)
                                                        │
                                              Analytics & Insights
```

## Project Structure

```
olist-de-pipeline/
├── batch_pipeline/        # PySpark batch ingestion & transformation jobs
├── streaming_pipeline/    # Kinesis/Kafka real-time event processing
├── agent/                 # AI analytics agent powered by Anthropic Claude
├── infra/                 # Terraform IaC for AWS resources
├── common/                # Shared utilities (logger, S3 helpers, constants)
├── .github/workflows/     # CI/CD pipelines (GitHub Actions)
├── requirements.txt
├── Makefile
└── .env.example
```

## Prerequisites

- Python 3.11+
- AWS CLI configured (`aws configure`)
- Terraform >= 1.5
- Java 11+ (for PySpark)

## Getting Started

```bash
# 1. Clone the repo
git clone https://github.com/madhave021/olist-de-pipeline.git
cd olist-de-pipeline

# 2. Set up environment
cp .env.example .env
# Edit .env with your values

# 3. Install dependencies
make install

# 4. Provision AWS infrastructure
cd infra
terraform init
terraform apply

# 5. Run tests
make test
```

## CI/CD

GitHub Actions runs on every push to `main` and on pull requests:
- Linting (flake8, black)
- Unit tests with coverage

AWS credentials are stored as GitHub repository secrets (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`).

## License

MIT
