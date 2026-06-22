"""
Fraud Detection Pipeline - Daily DAG
Menggunakan Astronomer Cosmos untuk dbt task-level visibility
dan Airflow Connections untuk credentials management.
"""
import logging
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
DBT_PATH           = '/opt/airflow/dbt'
DBT_BIN            = '/home/airflow/.local/bin/dbt'
GE_PATH            = '/opt/airflow/great_expectations'
POSTGRES_CONN_ID   = 'fraud_postgres'
SLACK_CONN_ID      = 'fraud_slack_webhook'


# ── PERBAIKAN: POSTGRES_ENV tidak bisa pakai Jinja {{ conn.x }} ──────────────
# BashOperator env= dievaluasi saat parse time, bukan render time.
# Jinja templating hanya bekerja di bash_command= string, bukan di dict env=.
# Solusi: pakai BaseHook.get_connection() di fungsi Python biasa.
def get_postgres_env() -> dict:
    """Ambil credentials dari Airflow Connection 'fraud_postgres'."""
    conn = BaseHook.get_connection(POSTGRES_CONN_ID)
    return {
        'POSTGRES_HOST':     conn.host,
        'POSTGRES_PORT':     str(conn.port or 5432),
        'POSTGRES_DB':       conn.schema,
        'POSTGRES_USER':     conn.login,
        'POSTGRES_PASSWORD': conn.password,
    }


# ── Slack Alert ───────────────────────────────────────────────────────────────
def send_failure_alert(context):
    """Kirim notifikasi Slack saat task GAGAL."""
    task_instance = context.get('task_instance')
    dag_run       = context.get('dag_run')
    exception     = context.get('exception', 'Unknown error')
    log_url       = task_instance.log_url if task_instance else 'N/A'

    message = (
        ':red_circle: *Fraud Pipeline GAGAL*\n'
        f'*DAG:* `{dag_run.dag_id if dag_run else "unknown"}`\n'
        f'*Task:* `{task_instance.task_id if task_instance else "unknown"}`\n'
        f'*Run ID:* `{dag_run.run_id if dag_run else "unknown"}`\n'
        f'*Error:* {str(exception)[:200]}\n'
        f'*Log:* {log_url}'
    )

    try:
        SlackWebhookHook(
            slack_webhook_conn_id=SLACK_CONN_ID
        ).send(text=message)
        logger.info('Slack failure alert sent successfully')
    except Exception:
        logger.exception('Failed to send Slack failure alert')


def send_success_alert(context):
    """Kirim notifikasi Slack saat seluruh DAG SUKSES."""
    dag_run = context.get('dag_run')
    message = (
        ':large_green_circle: *Fraud Pipeline SUKSES*\n'
        f'*DAG:* `{dag_run.dag_id if dag_run else "unknown"}`\n'
        f'*Run ID:* `{dag_run.run_id if dag_run else "unknown"}`\n'
        'Semua dbt models, tests, dan validasi berhasil.'
    )
    try:
        SlackWebhookHook(
            slack_webhook_conn_id=SLACK_CONN_ID
        ).send(text=message)
    except Exception:
        logger.exception('Failed to send Slack success alert')


# ── Cosmos Config ─────────────────────────────────────────────────────────────
profile_config = ProfileConfig(
    profile_name='fraud_pipeline',
    target_name='dev',
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id=POSTGRES_CONN_ID,
        profile_args={'schema': 'public'},
    ),
)

execution_config = ExecutionConfig(dbt_executable_path=DBT_BIN)

project_config = ProjectConfig(
    dbt_project_path=DBT_PATH,
    # NOTE: ProjectConfig in this Cosmos version doesn't accept `install_dbt_deps`.
    # We perform `dbt deps` in the `install_dbt_deps` task instead.
)


def build_dbt_group(group_id: str, select: str) -> DbtTaskGroup:
    """Factory untuk DbtTaskGroup per layer (bronze/silver/gold)."""
    return DbtTaskGroup(
        group_id=group_id,
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={
            'select': select,
        },
    )


# ── Default Args ──────────────────────────────────────────────────────────────
default_args = {
    'owner':              'fraud-pipeline',
    'depends_on_past':    False,
    'email_on_failure':   False,
    'email_on_retry':     False,
    'retries':            1,
    'retry_delay':        timedelta(minutes=5),
    # Setiap task yang gagal otomatis trigger Slack
    'on_failure_callback': send_failure_alert,
}

# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id='fraud_pipeline_daily',
    default_args=default_args,
    description='Daily fraud detection pipeline — Cosmos + Slack alerts',
    schedule='@daily',
    # PERBAIKAN: ganti days_ago(1) dengan pendulum — days_ago deprecated
    start_date=pendulum.datetime(2026, 1, 1, tz='UTC'),
    catchup=False,
    # Slack alert saat seluruh DAG sukses
    on_success_callback=send_success_alert,
    tags=['fraud-detection', 'data-engineering', 'dbt', 'cosmos'],
) as dag:

    # ── Task 1: Validasi data raw ─────────────────────────────────────────
    validate_raw_data = BashOperator(
        task_id='validate_raw_data',
        bash_command=f'cd {GE_PATH} && python data_quality_checks.py --layer raw',
        # PERBAIKAN: panggil fungsi, bukan dict dengan Jinja
        env=get_postgres_env(),
    )

    # ── Task 2: Install dbt deps ──────────────────────────────────────────
    install_dbt_deps = BashOperator(
        task_id='install_dbt_deps',
        bash_command=f'cd {DBT_PATH} && {DBT_BIN} deps',
        env=get_postgres_env(),
    )

    # ── Task 3–5: dbt Bronze → Silver → Gold (tiap model jadi task sendiri)
    bronze_models = build_dbt_group('bronze_models', 'bronze')
    silver_models = build_dbt_group('silver_models', 'silver')
    gold_models   = build_dbt_group('gold_models',   'gold')

    # ── Task 6: Validasi Gold marts ───────────────────────────────────────
    validate_gold_data = BashOperator(
        task_id='validate_gold_data',
        bash_command=f'cd {GE_PATH} && python data_quality_checks.py --layer gold',
        env=get_postgres_env(),
    )

    # ── Task 7: Notify success ────────────────────────────────────────────
    notify_success = PythonOperator(
        task_id='notify_success',
        python_callable=lambda: logger.info('✓ Pipeline selesai. Slack notified.'),
    )

    # ── Dependencies ──────────────────────────────────────────────────────
    (
        validate_raw_data
        >> install_dbt_deps
        >> bronze_models
        >> silver_models
        >> gold_models
        >> validate_gold_data
        >> notify_success
    )