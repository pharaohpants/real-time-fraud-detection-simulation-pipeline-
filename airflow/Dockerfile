FROM apache/airflow:2.7.0

USER airflow
RUN pip install --no-cache-dir --user \
    "astronomer-cosmos[dbt-postgres]==1.10.1" \
    "dbt-core==1.7.0" \
    "dbt-postgres==1.7.0" \
    "protobuf<5" \
    "apache-airflow-providers-slack==8.5.1" \
    "apache-airflow-providers-postgres==5.10.2"
    # openlineage dihapus — tidak dipakai dan menyebabkan conflict