# 🏦 Fintech AWS Data Pipeline

A complete data pipeline for the financial sector built on AWS, following the **Medallion Architecture** (Bronze → Silver → Gold) with **Star Schema** dimensional modeling.

> Hands-on AWS Data Engineering learning project.

---

## 📐 Architecture

```
Source (CSV)
    │
    ▼
┌─────────────────────────────────────────────────────┐
│                    AWS S3                           │
│  /raw → /bronze → /silver → /gold                  │
└─────────────────────────────────────────────────────┘
    │           │           │           │
    ▼           ▼           ▼           ▼
 Upload      Lambda      Glue ETL    Glue ETL
  CSV       CSV→Parquet  Cleaning   Dimensions
                                    + Fact
                                       │
                                       ▼
                                   Amazon Athena
                                  (SQL Queries)
                                       │
                                       ▼
                                   Power BI
                                  (Dashboard)
```

---

## 🗂️ Medallion Architecture

| Layer | S3 Path | Description |
|---|---|---|
| **Raw** | `/raw` | Original CSVs uploaded by Python script |
| **Bronze** | `/bronze` | Raw Parquet — data as received from source |
| **Silver** | `/silver` | Cleaned, typed and deduplicated data |
| **Gold** | `/gold` | Star Schema dimensional model for BI |

---

## ⭐ Dimensional Model (Star Schema)

```
                    dim_tempo
                       │
    dim_cliente ── fat_transacoes ── dim_cartao
                       │
              dim_comerciante
              dim_pagamento
              dim_status
              dim_localizacao
```

### Gold Layer Tables

| Table | Type | Records | Description |
|---|---|---|---|
| `fat_transacoes` | Fact | 3,386,122 | Credit card transactions |
| `dim_tempo` | Dimension | 455 | Calendar Jan/2025–Mar/2026 |
| `dim_cliente` | Dimension | 20,000 | Customer profiles |
| `dim_cartao` | Dimension | 20,000 | Card and credit limit data |
| `dim_comerciante` | Dimension | 488 | Merchants/Establishments |
| `dim_pagamento` | Dimension | 26 | Payment types |
| `dim_status` | Dimension | 14 | Transaction status and decline reasons |
| `dim_localizacao` | Dimension | 20 | Brasília/DF regions |

---

## 🛠️ AWS Services Used

| Service | Usage |
|---|---|
| **Amazon S3** | Data Lake — storage for all layers |
| **AWS Lambda** | Automatic ingestion CSV → Parquet |
| **AWS Glue Crawler** | Automatic schema cataloging |
| **AWS Glue ETL** | Bronze → Silver → Gold transformation |
| **Amazon Athena** | Serverless SQL queries |
| **AWS IAM** | Access control and permissions |
| **Amazon CloudWatch** | Monitoring and logs |
| **Power BI** | Dashboard and visualization |

---

## 📊 Simulated Data

Realistic financial data generated with Python + Faker:

- **20,000 customers** with real addresses from Brasília/DF
- **20,000 profiles** with Gold, Platinum and Black cards
- **3.3 million transactions** from Jan/2025 to Mar/2026
- Purchases at establishments worldwide
- ~0.3% of transactions flagged as fraud
- Multiple card flags: Visa, Mastercard, Elo, Amex

---

## 🚀 How to Run

### Prerequisites

```bash
pip install boto3 pandas pyarrow faker
aws configure  # configure with your IAM credentials
```

### 1. Generate data

```bash
cd 01_data_generation
python generate_bank_data.py
```

### 2. Configure Lambda

- Deploy code from `02_ingestion/lambda_function.py`
- Add Layer: `AWSSDKPandas-Python311`
- Configure S3 trigger on `/raw` folder
- Timeout: 15 min | Memory: 3008 MB | Storage: 2048 MB

### 3. Run Bronze → Silver ETL

```bash
aws glue start-job-run --job-name bronze-to-silver-customers
aws glue start-job-run --job-name bronze-to-silver-customer-profiles
aws glue start-job-run --job-name bronze-to-silver-transactions
```

### 4. Run Silver → Gold ETL

```bash
# Run scripts in 04_silver_to_gold/ in order:
# 1. dim_tempo.py
# 2. dim_cliente.py
# 3. dim_localizacao.py
# 4. dim_cartao.py
# 5. dim_comerciante.py
# 6. dim_pagamento.py
# 7. dim_status.py
# 8. fat_transacoes.py
```

### 5. Catalog in Athena

```bash
aws glue start-crawler --name fintech-crawler
```

---

## 🔒 Security

- ⚠️ **Never commit AWS credentials** — use environment variables
- All Access Keys must be from IAM user, not root account
- `.gitignore` is already configured to ignore sensitive files
- In production, use AWS Secrets Manager to manage credentials

---

## 📁 Repository Structure

```
fintech-aws-pipeline/
├── 01_data_generation/     # Simulated data generation
├── 02_ingestion/           # Lambda function
├── 03_bronze_to_silver/    # Cleaning ETL jobs
├── 04_silver_to_gold/      # Modeling ETL jobs
├── 05_queries/             # Validation and analytical SQL queries
└── docs/                   # Diagrams and documentation
```

---

## 🗺️ Roadmap

- [x] Simulated data generation
- [x] Ingestion pipeline with Lambda
- [x] Medallion Architecture (Bronze/Silver/Gold)
- [x] Star Schema dimensional model
- [x] SQL queries with Amazon Athena
- [x] Dashboard with Power BI
- [ ] End-to-end automation with Step Functions
- [ ] SFTP ingestion with AWS Transfer Family
- [ ] Infrastructure as Code with Terraform
- [ ] Data quality testing with Great Expectations
- [ ] CI/CD with GitHub Actions

---

## 👨‍💻 Author

Developed as a hands-on AWS Data Engineering learning project.

**Brasília/DF — 2026**

---

## 📄 License

MIT License — feel free to use and adapt.
