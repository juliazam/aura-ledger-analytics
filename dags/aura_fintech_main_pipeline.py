"""
Aura Multi-Source Fintech ETL Pipeline.

This DAG orchestrates a high-throughput financial data pipeline that:
1. Validates the availability of the external Currency Exchange Rate API.
2. Extracts and filters daily USD-based exchange rates to optimize XCom storage.
3. Extracts completed transaction records from an upstream PostgreSQL OLTP database.
4. Performs in-memory cross-rate calculations (converting transactions to SGD) using Pandas 3.x.
5. Monitors processed records for financial anomalies using dynamic thresholds.
6. Writes partitioned analytical datasets to a local Data Lake in high-performance Parquet format.

Author: juliazam / Aura Blockchain Team
Date: July 2026
"""
import os
from datetime import datetime, timezone
import pandas as pd
from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models import Variable

def report_task_failure(context):
    """
    Generate and print a structured critical failure alert to standard output.
    
    In a production-ready environment, this handler can be extended to dispatch
    real-time webhook notifications to platforms like Telegram, Slack, or PageDuty
    using Airflow Connections/Hooks.
    
    Args:
        context (dict): The Airflow context dictionary containing metadata about the failed task.
    """
    dag_id = context.get("task_instance").dag_id
    task_id = context.get("task_instance").task_id
    run_id = context.get("task_instance").run_id
    exception = context.get("exception")

    alert_message = f'''
    ❌ [AIRFLOW ALERT] Task Failed!
    --------------------------------------
    DAG: {dag_id}
    Task: {task_id}
    Run ID: {run_id}
    Error: {exception}
    Time: {datetime.now(timezone.utc).isoformat()}
    --------------------------------------
    '''
    print(alert_message)
    # Reserved for integration: HttpHook(http_conn_id='telegram_mesh').run(...)

@dag(
    dag_id="aura_multi_source_fintech_etl",
    start_date=datetime(2026, 7, 1),
    schedule="@hourly",
    catchup=False,
    default_args={
        "on_failure_callback": report_task_failure,
        "retries": 1,
        "retry_delay": 30,
    }
)
def multi_source_fintech_etl():
    """
    Orchestrate the multi-source financial ingestion and processing pipeline.
    
    Fetches raw OLTP records and exchange rate updates concurrently, executes
    enrichment and risk evaluations, and dumps the output into a localized Parquet Data Lake.
    """

    # Fetch configuration schema from Airflow Variables.
    # Uses fallback dictionary for local development to support Zero-Setup Deployments.
    conf = Variable.get(
        "aura_etl_config", 
        deserialize_json=True,
        default_var={
            "vault_dir": "/opt/airflow/data/aura_blockchain_vault",
            "target_currencies": ["USD", "EUR", "SGD", "NZD"],
            "anomaly_threshold_sgd": 10000.0
        }
    )

    # 1. External API Availability Check
    # Ensures the pipeline fails fast if the external exchange rate service is unreachable.
    check_api_availability = HttpSensor(
        task_id="check_api_availability",
        http_conn_id="fintech_api_rates",
        endpoint="v6/latest/USD",
        request_params={},
        response_check=lambda response: response.status_code == 200,
        poke_interval=20,       # Check endpoint status every 20 seconds
        timeout=600,            # Hard limit of 10 minutes for retry polling
        mode="reschedule"       # Release worker slots while waiting to maximize efficiency
    )

    # 2. Local File System Preparation
    prepare_storage = BashOperator(
        task_id="create_vault_directory",
        bash_command=f"mkdir -p {conf['vault_dir']}"
    )

    # 3. Exchange Rate Extraction
    @task
    def extract_api_data() -> dict[str, float]:
        """
        Pull the latest currency rates and apply XCom volume optimization.
        
        Saves computational overhead by immediately filtering out non-target currencies 
        before sending the payload to the Airflow metadata database (XCom).
        """
        task_conf = Variable.get("aura_etl_config", deserialize_json=True, default_var=conf)
        target_curs = task_conf.get("target_currencies")

        print("Downloading currency rates from MAS API...")
        hook = HttpHook(http_conn_id="fintech_api_rates", method="GET")
        response = hook.run(endpoint="v6/latest/USD")
        full_data = response.json()

        rates = full_data.get("rates", {})

        # In-memory filtering of API payload to minimize database footprint
        filtered_rates = {
            cur: float(rates[cur])
            for cur in target_curs
            if cur in rates
        }
        print(f"XCom Optimization: Saved only {len(filtered_rates)} rates instead of {len(rates)}.")
        return filtered_rates

    # 4. OLTP Schema Bootstrapping
    # Note: Used strictly for demonstration and local development to guarantee
    # a seamless "Zero-Setup" initialization. In production, this table is
    # populated upstream by an external microservice.
    init_transaction_table = SQLExecuteQueryOperator(
        task_id="create_transactions_table",
        conn_id="fintech_oltp_db",
        sql='''
        CREATE TABLE IF NOT EXISTS raw_transactions (
            transaction_id VARCHAR(50) PRIMARY KEY,
            account_id VARCHAR(50) NOT NULL,
            amount NUMERIC(18, 4) NOT NULL,
            currency VARCHAR(3) NOT NULL,
            status VARCHAR(20) NOT NULL,
            created_at TIMESTAMP NOT NULL
        );

        INSERT INTO raw_transactions (transaction_id, account_id, amount, currency, status, created_at) 
            VALUES 
                ('tx_889201', 'acc_x771', 1250.50, 'USD', 'COMPLETED', '2026-07-13 08:30:00'),
                ('tx_889202', 'acc_y102', 45000.00, 'SGD', 'COMPLETED', '2026-07-13 08:32:15'),
                ('tx_889203', 'acc_z990', 15.00, 'USD', 'FAILED', '2026-07-13 08:35:00'),
                ('tx_889204', 'acc_x771', 300.25, 'EUR', 'COMPLETED', '2026-07-13 08:40:10'),
                ('tx_889205', 'acc_k442', 890000.00, 'SGD', 'COMPLETED', '2026-07-13 08:42:00'),
                ('tx_889206', 'acc_m112', 5400.00, 'USD', 'PENDING', '2026-07-13 08:50:00'),
                ('tx_889207', 'acc_y102', 120.00, 'EUR', 'COMPLETED', '2026-07-13 08:55:22')
            ON CONFLICT (transaction_id) DO NOTHING;
        '''
    )

    # 5. Extract Completed Transactions
    extract_transactions = SQLExecuteQueryOperator(
        task_id="extract_transactions_from_db",
        conn_id="fintech_oltp_db",
        sql='''
        SELECT transaction_id, account_id, amount, currency, status, created_at
        FROM raw_transactions
        WHERE status = 'COMPLETED';
        '''
    )

    # 6. Transform, Enrich, and Partition
    @task
    def transform_and_enrich(transactions_records: list, exchange_rates: dict):
        """
        Perform analytical transformations, anomaly checking, and write output datasets.
        
        Processes transactions using Pandas 3.x, normalizes values to SGD using 
        cross-rates, flags large anomalies, and stores the results in a structured, 
        partitioned manner within the Data Lake directory.
        """
        if not transactions_records:
            print("No transactions found to process.")
            return "No data"

        task_conf = Variable.get("aura_etl_config", deserialize_json=True, default_var=conf)
        target_curs = task_conf.get("target_currencies")
        anomaly_limit = task_conf.get("anomaly_threshold_sgd")
        vault_dir = task_conf.get("vault_dir")

        # Create base DataFrame
        columns = ["transaction_id", "account_id", "amount", "currency", "status", "created_at"]
        df_tx = pd.DataFrame(transactions_records, columns=columns)
        print(f"Loaded {len(df_tx)} completed transactions from DB.")

        # Filter out transactions not in scope
        df_tx = df_tx[df_tx["currency"].isin(target_curs)].copy()
        print(f"Filtered to {len(df_tx)} transactions with currencies: {target_curs}")

        # Compute conversion multipliers dynamically based on USD cross-rates
        default_sgd_rate = 1.293678
        usd_to_sgd_rate = exchange_rates.get("SGD", default_sgd_rate)

        conversion_multipliers = {}
        for cur in target_curs:
            rate_to_usd = exchange_rates.get(cur, 1.0)
            conversion_multipliers[cur] = usd_to_sgd_rate / rate_to_usd
            print(f"Conversion multiplier {cur} -> SGD: {conversion_multipliers[cur]:.6f}")

        # Normalize amounts
        df_tx["amount"] = df_tx["amount"].astype(float)
        df_tx["conversion_multiplier"] = df_tx["currency"].map(conversion_multipliers)
        df_tx["amount_in_sgd"] = (df_tx["amount"] * df_tx["conversion_multiplier"]).round(2)

        # Audit and anomaly checks
        df_tx["is_anomaly"] = df_tx["amount_in_sgd"] > anomaly_limit
        if df_tx["is_anomaly"].any():
            raise ValueError("ALERT: Fraud/Anomaly detected! Pipeline stopped for audit.")

        # Extract partition keys from temporal metadata
        df_tx["created_at"] = pd.to_datetime(df_tx["created_at"])
        df_tx["year"] = df_tx["created_at"].dt.year
        df_tx["month"] = df_tx["created_at"].dt.strftime("%m")
        df_tx["day"] = df_tx["created_at"].dt.strftime("%d")

        print("\n--- Processed Data Sample ---")
        print(df_tx[["transaction_id", "currency", "amount", "amount_in_sgd", "is_anomaly"]])

        try:
            df_tx.to_parquet(
                vault_dir,
                partition_cols=["year", "month", "day"],
                index=False,
                engine="pyarrow"
            )
            print(f"\nSuccessfully saved partitioned Parquet to Data Lake: {vault_dir}")
        except ImportError:
            print("\n[WARNING]: pyarrow is not installed. " \
            "Falling back to simple partition simulation via directories...")
            for (year, month, day), group in df_tx.groupby(["year", "month", "day"]):
                partition_path = f"{vault_dir}/year={year}/month={month}/day={day}"
                os.makedirs(partition_path, exist_ok=True)
                group.to_csv(f"{partition_path}/data.csv", index=False)
            print(f"\nSaved simulated partitions (CSV) to: {vault_dir}")

        return "Success"

    # Define task instances and pass downstream dependencies
    currency_rates = extract_api_data()
    enriched_data = transform_and_enrich(transactions_records=extract_transactions.output,
    exchange_rates=currency_rates)

    # Establish dependency graph
    prepare_storage >> [
        check_api_availability >> currency_rates,
        init_transaction_table >> extract_transactions
    ] >> enriched_data

# Initialize the pipeline graph
multi_source_fintech_etl()
