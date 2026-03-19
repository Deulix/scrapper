from datetime import datetime as dt

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id="inote_scrapper_dag", start_date=dt(2026, 3, 1), schedule="@daily"
) as dag:

    def my_script():
        print("=== НАЧИНАЕМ СКРАППИНГ ===")

    task = PythonOperator(task_id="check_script", python_callable=my_script)

    extract_dag = BashOperator(
        task_id="run_scrapper",
        bash_command="cd /opt/airflow && uv run python -m src.scrapper.scrapper",
    )

    transform_dag = BashOperator(
        task_id="run_spark",
        bash_command="cd /opt/airflow && uv run python -m src.spark.transform",
    )

    extract_dag >> transform_dag
