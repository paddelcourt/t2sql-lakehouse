from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
        dag_id="simple_test_dag",
        schedule=None, # Set to None for manual triggering
        start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
        catchup=False,
        tags=["test", "simple"],
    ) as dag:
        run_bash_command = BashOperator(
            task_id="print_test_message",
            bash_command="echo 'Hello from Airflow worker!'",
        )
