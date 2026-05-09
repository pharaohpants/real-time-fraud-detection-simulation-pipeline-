"""
Fraud Detection Pipeline - Daily DAG
Orchestrates data validation, dbt runs, and quality checks
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import logging

logger = logging.getLogger(__name__)

# Default arguments for DAG
default_args = {
    'owner': 'fraud-pipeline',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'fraud_pipeline_daily',
    default_args=default_args,
    description='Daily fraud detection pipeline with dbt and data quality',
    schedule_interval='@daily',
    catchup=False,
    tags=['fraud-detection', 'data-engineering'],
)

# Define task paths
DBT_PATH = '/opt/airflow/dbt'
DBT_BIN = '/home/airflow/.local/bin/dbt'
GE_PATH = '/opt/airflow/great_expectations'
POSTGRES_ENV = {
    'POSTGRES_HOST': os.getenv('POSTGRES_HOST', 'postgres'),
    'POSTGRES_PORT': os.getenv('POSTGRES_PORT', '5432'),
    'POSTGRES_DB': os.getenv('POSTGRES_DB', 'fraud_db'),
    'POSTGRES_USER': os.getenv('POSTGRES_USER', 'fraud_user'),
    'POSTGRES_PASSWORD': os.getenv('POSTGRES_PASSWORD', 'fraud_pass'),
}


def log_success():
    """Log task success"""
    logger.info("✓ Pipeline task completed successfully")


# Task 1: Validate raw data with Great Expectations
validate_raw_data = BashOperator(
    task_id='validate_raw_data',
    bash_command=f'cd {GE_PATH} && python data_quality_checks.py',
    env=POSTGRES_ENV,
    dag=dag,
)

# Task 2: Install dbt packages once per DAG run
install_dbt_deps = BashOperator(
    task_id='install_dbt_deps',
    bash_command=f'cd {DBT_PATH} && {DBT_BIN} deps',
    env=POSTGRES_ENV,
    dag=dag,
)

# Task 3: Run dbt Bronze models
run_dbt_bronze = BashOperator(
    task_id='run_dbt_bronze',
    bash_command=f'cd {DBT_PATH} && {DBT_BIN} run --select bronze',
    env=POSTGRES_ENV,
    dag=dag,
)

# Task 4: Run dbt Silver models
run_dbt_silver = BashOperator(
    task_id='run_dbt_silver',
    bash_command=f'cd {DBT_PATH} && {DBT_BIN} run --select silver',
    env=POSTGRES_ENV,
    dag=dag,
)

# Task 5: Run dbt Gold models
run_dbt_gold = BashOperator(
    task_id='run_dbt_gold',
    bash_command=f'cd {DBT_PATH} && {DBT_BIN} run --select gold',
    env=POSTGRES_ENV,
    dag=dag,
)

# Task 6: Run dbt tests
run_dbt_tests = BashOperator(
    task_id='run_dbt_tests',
    bash_command=f'cd {DBT_PATH} && {DBT_BIN} test',
    env=POSTGRES_ENV,
    dag=dag,
)

# Task 7: Validate gold data
validate_gold_data = BashOperator(
    task_id='validate_gold_data',
    bash_command=f'cd {GE_PATH} && python data_quality_checks.py',
    env=POSTGRES_ENV,
    dag=dag,
)

# Task 8: Notify success
notify_success = PythonOperator(
    task_id='notify_success',
    python_callable=log_success,
    dag=dag,
)

# Set task dependencies
validate_raw_data >> install_dbt_deps >> run_dbt_bronze
run_dbt_bronze >> run_dbt_silver
run_dbt_silver >> run_dbt_gold
run_dbt_gold >> run_dbt_tests
run_dbt_tests >> validate_gold_data
validate_gold_data >> notify_success
