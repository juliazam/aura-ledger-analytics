# Aura Ledger Analytics 🚀
### Enterprise-Grade Multi-Source Financial Ingestion & Processing Pipeline

A production-ready **ETL (Extract, Transform, Load)** data pipeline built on **Apache Airflow 3.x**. This orchestrator manages dual-source ingestion (SQL relational transactions and external RESTful currency APIs), performs dynamic currency normalization, analyzes records for fraud/anomalies, and commits outputs into a column-oriented **Hive-style partitioned Parquet Data Lake**.

---

## 🏗️ Architecture & Pipeline Flow

The pipeline executes dual-branch ingestion concurrently to maximize throughput and resources:

```text
                     [ create_vault_directory ]
                                 |
        +------------------------+------------------------+
        |                                                 |
[ check_api_availability ]                    [ create_transactions_table ]
        | (HttpSensor)                                    |
[ extract_api_data ] (rates)                  [ extract_transactions_from_db ]
        |                                                 |
        +------------------------+------------------------+
                                 |
                      [ transform_and_enrich ] (Pandas & Parquet)
```
1. **Pre-requisites Phase:** Prepares local block directories and sets up structural database schemas.

2. **Sensing Phase:** A highly efficient HttpSensor checks exchange endpoint status in reschedule mode, releasing worker slots between checks.

3. **Ingestion Phase:** Extracts daily exchange rates from the REST API and completed records from PostgreSQL.

4. **Transform Phase:** Vectorized cross-rate calculations, anomaly/fraud checking, and schema generation using Pandas.

5. **Storage Phase:** Writes Hive-structured partitions (year=YYYY/month=MM/day=DD) to a local vault directory in .parquet format.

## ⚡ Key Engineering Highlights
- **XCom Payload Optimization:** Standard API responses carry more than 160+ exchange rates. We filter and trim down to 4 target currencies before calling return. This keeps Airflow's internal metadata DB clean and fast.

- **Resilient Sensors:** Utilizing the reschedule mode of HttpSensor prevents worker starvation by putting tasks to sleep while polling, maintaining high concurrency.

- **Production Alerting:** A robust centralized callback (on_failure_callback) catches execution exceptions, generating structured operational alerts with detailed diagnostic payloads.

- **Zero-Setup Configuration:** Out-of-the-box support for .env integration, passing local Docker structures and parameter configurations directly into Airflow Variables.

## 📁 Repository Directory Structure
```text
aura-ledger-analytics/
├── .env.example                 # Environment variables blueprint
├── docker-compose.yaml          # Standardized multi-container Airflow 3.x setup
├── requirements.txt             # Project dependencies (pandas, pyarrow, etc.)
├── .gitignore                   # Safe configuration ignoring local data and secrets
├── README.md                    # Project documentation (this file)
└── dags/
    └── aura_fintech_pipeline.py # Production-grade DAG file with docs
```
## 🛠️ Local Deploy Guide
1. Provision the Infrastructure
Ensure Docker and Docker Compose are installed. Spin up the cluster:
```bash
docker compose up -d
```

2. Configure Local Connections

Configure Airflow Connections to allow target integrations to authorize securely:
- **Database Connection** (fintech_oltp_db): Points to your internal relational storage.

- **REST API Connection** (fintech_api_rates): Points to the target financial exchange rate API.

3. Run the DAG
Open the Airflow UI (http://localhost:8080), toggle the aura_multi_source_fintech_etl pipeline, and trigger an manual execution.

## 📊 Sample Output Format
Inside your defined vault_dir, processed transactions are automatically partitioned:
```text
/opt/airflow/data/aura_blockchain_vault/
├── year=2026/
│   └── month=07/
│       └── day=13/
│           └── part-0.parquet   # High-performance column-oriented dataset
```

## ⏱️ Deadline Monitoring

The pipeline uses Airflow's `DeadlineAlert` (replacement for the deprecated SLA feature in Airflow 3.0+) 
to detect abnormally slow DAG runs.

**Current threshold:** 15 minutes from `DAGRUN_LOGICAL_DATE`.

This value is intentionally conservative — real measured run duration is ~27 seconds under normal 
conditions (verified via manual trigger). The 15-minute threshold is not tuned to typical performance; 
it's set as a generous upper bound to catch true pipeline stalls (e.g. hung API calls, DB locks) rather 
than to flag routine variance. As more production run history accumulates, this should be tightened 
(e.g. to 2–3 minutes) based on observed p95/p99 run durations.