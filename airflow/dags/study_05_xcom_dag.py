"""
STUDY DAG 5 — XCom (passing data between tasks)
==================================================
Task A computes a value; Task B reads that exact value and uses it.

Core concepts introduced here:

- Tasks run as separate processes (possibly on separate machines
  entirely) - they don't share Python memory. XCom ("cross-communication")
  is Airflow's built-in mechanism for passing small pieces of data between
  tasks anyway. It's stored in Airflow's own metadata database.

- The simplest way to push: just `return` a value from your function -
  Airflow automatically stores it as an XCom under the key "return_value".

- The simplest way to pull: `context["ti"].xcom_pull(task_ids="task_id")`
  inside a downstream task (`ti` = "task instance").

- The real DAG in this project (`packet_pipeline_dag.py`) uses this exact
  pattern: `create_capture` pushes `capture_id` and `filename`, and every
  later task (`start_capture`, `parse_and_store`, `archive_capture`) pulls
  them back out - that's how one capture run's ID flows through all 7
  tasks without being a global variable.

- XCom is for SMALL data (IDs, filenames, short strings/numbers) - not
  for passing entire files or large datasets between tasks. For that,
  tasks should read/write a shared location instead (like the pcap_data
  volume this project's tasks use).

Try this:
  1. Trigger this DAG, then open the "compute_value" task's XCom tab
     (in the task instance details) - you'll see the number it returned,
     stored and ready for the next task to read.
  2. Compare this to how `capture_id` moves through
     airflow/dags/packet_pipeline_dag.py.
"""

import random
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def compute_value():
    value = random.randint(1, 100)
    print(f"Computed value: {value}. Returning it so Airflow stores it as an XCom.")
    return value  # automatically pushed to XCom under key "return_value"


def use_value(**context):
    ti = context["ti"]
    value = ti.xcom_pull(task_ids="compute_value")
    print(f"Pulled value from XCom: {value}")
    print(f"Doubled: {value * 2}")


with DAG(
    dag_id="study_05_xcom",
    description="STUDY: passing a value from one task to another via XCom",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["study"],
) as dag:

    compute_task = PythonOperator(
        task_id="compute_value",
        python_callable=compute_value,
    )

    use_task = PythonOperator(
        task_id="use_value",
        python_callable=use_value,
    )

    compute_task >> use_task
