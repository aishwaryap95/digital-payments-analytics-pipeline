# Digital Payments Analytics Pipeline

End-to-end batch data engineering project built using **PySpark, AWS S3, and MySQL** to process digital payment transaction files, create curated data marts, and generate business KPIs for analytics reporting.

---

## Project Overview

This project simulates a real-world digital payments ecosystem where upstream systems drop CSV files into an S3 landing zone. The pipeline validates files, processes payment data using PySpark, builds curated marts, and generates insights for transaction monitoring, merchant settlements, and refund analytics.

---

## Tech Stack

- **Languages** :- Python, SQL 
- **Data Processing** :- PySpark
- **Cloud & Storage** :- AWS S3, Parquet, CSV
- **Database** :- MySQL
- **Tools** :- Git, GitHub, PyCharm

---

## Business Problem

Digital payment platforms process high transaction volumes across:

- UPI
- Cards
- Wallets
- Net Banking
- Merchant payouts
- Refund requests

Business teams need reliable curated datasets for:

- Transaction success monitoring
- Revenue reporting
- Merchant settlement tracking
- Refund trend analysis
- Operational dashboards

---

## Source Files

The pipeline processes multiple CSV source files:

- `dim_customer.csv`
- `dim_merchant.csv`
- `dim_channel.csv`
- `fact_transactions.csv`
- `fact_refunds.csv`
- `fact_settlements.csv`

---

# Architecture Diagram

![digital-payments-pipeline-architecture.drawio.png](digital-payments-pipeline-architecture.drawio.png)

../docs/digital-payments-pipeline-architecture.drawio.png

# Data Model / Schema Diagram

![digital-payments-star-schema.drawio.png](digital-payments-star-schema.drawio.png)

../docs/digital-payments-star-schema.drawio.png

---

## S3 Data Lake Architecture

```text
s3://bucket/

landing/       -> Incoming source files
processing/    -> Files under execution
processed/     -> Successfully processed files
failed/        -> Rejected / failed files

data_marts/
  transaction_performance_mart/
  merchant_settlement_mart/
  refund_insights_mart/
```
---

# Pipeline Flow

1. Read source files from landing zone
2. Move files to processing zone
3. Mark files Active in staging table
4. Validate schema / format
5. Load Spark DataFrames
6. Build curated marts
7. Write partitioned parquet marts to S3
8. Run KPI queries
9. Mark files Completed
10. Move files to processed zone
11. Failed files moved to failed zone

---

# Data Marts

#### 1. Transaction Performance Mart

Tracks payment throughput and operational performance.

KPIs
- Daily transaction volume
- Success rate %
- Revenue by channel
- City-wise payment amount
- Monthly success rate by channel
#### 2. Merchant Settlement Mart

Tracks merchant payouts and settlement operations.

KPIs
- Total settled amount
- Pending settlements %
- Top merchants by payout
- Fee revenue by merchant category
#### 3. Refund Insights Mart

Tracks refund behaviour and customer issues.

KPIs
- Refund rate %
- Top refund merchants
- Refund reason trend
- Customer repeat refunds

#### Partition Strategy

Time-based partitioning is used for efficient reads and scalable storage.

- transaction_year / transaction_month
- settlement_year / settlement_month 
- refund_year / refund_month

---

# Key Engineering Features

- Multi-file Batch Ingestion: Processes multiple dimension and fact files in a single run.

- File Lifecycle Management: Automates movement across landing, processing, processed, and failed zones.

- Staging Control Table: Tracks file status using Active / Completed / Failed states.

- Partitioned Parquet Marts: Optimized curated outputs for analytical workloads.

- Modular PySpark Architecture : Separate modules for ingestion, transformation, analytics, and utilities.

- KPI Analytics Layer: Business reporting queries built on top of curated marts.

- Logging & Error Handling: Centralized logging with failure tracking and operational visibility.

---

# Sample KPI Output
- Daily Transactions       : 1,000
- Success Rate            : 78.8%
- Top Revenue Channel     : UPI
- Highest Payment City    : Mumbai
- Refund Rate             : 6.4%
- Pending Settlements     : 9.2%
- Top Refund Reason       : Duplicate Charge

---

# How to Run

## Prerequisites

- Python installed
- Java installed
- Spark configured locally
- MySQL installed
- AWS IAM user with S3 read/write access
- Required S3 bucket created

---

## Steps

#### 1. Clone Repository

```bash
git clone <your-repo-url>
cd digital-payments-analytics-pipeline
```
#### 2. Open in PyCharm

Import project and verify folder structure.

#### 3. Create Virtual Environment
```bash
python -m venv .venv
```
Activate environment:
```bash
.venv\Scripts\activate
```
#### 4. Install Dependencies
```bash
pip install -r requirements.txt
```
#### 5. Configure Credentials

Update AWS access key / secret key and project configs inside utility/config files.

#### 6. Generate Sample Source Files

Run data generator:
```bash
python src/test/digital_payments_data_upload_to_s3.py
```
This creates sample CSV files and uploads to landing zone.

#### 7. Upload Files to S3 Landing Zone

Run upload script:
```bash
python src/test/digital_payments_data_upload_to_s3.py
```
This uploads generated CSV files to the S3 landing zone.

#### 8. Run Pipeline
```bash
python src/main.py
```
---

# What I Learned
- Building layered batch pipelines
- Spark joins and transformations
- Data mart modeling
- KPI design for business teams
- S3 file lifecycle orchestration
- Partition strategy for analytics workloads

--- 

# Future Enhancements
- Airflow orchestration
- Incremental loads
- Power BI / Tableau dashboards
- Data quality framework
- CI/CD deployment
- Alerting & monitoring
- Unit test coverage

---

# Author

#### Aishwarya Patankar
Data Engineer | PySpark | SQL | AWS | Batch Pipelines