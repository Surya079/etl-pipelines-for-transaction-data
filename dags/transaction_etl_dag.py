from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="transaction_etl_dag",
    default_args=default_args,
    description="ETL pipeline for synthetic transactions",
    schedule_interval="@hourly",
    catchup=False,
    tags=["etl", "transactions"],
) as dag:

    create_tables_task = BashOperator(
        task_id="create_tables",
        bash_command="python /opt/airflow/scripts/create_raw_table.py && python /opt/airflow/scripts/create_core_table.py",
    )

    ingest_task = BashOperator(
        task_id="ingest_transactions",
        bash_command="python /opt/airflow/scripts/ingest_transactions.py --start-page 1 --end-page 5 --limit 20",
    )

    transform_task = BashOperator(
        task_id="transform_to_core",
        bash_command="python /opt/airflow/datawarehouse/transform.py",
    )

    soda_scan_task = BashOperator(
        task_id="soda_quality_checks",
        bash_command="soda scan -c /opt/airflow/data_quality/configuration.yml -d postgres /opt/airflow/data_quality/checks/",
    )

    create_tables_task >> ingest_task >> transform_task >> soda_scan_task
