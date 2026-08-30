"""
STUDY DAG 1 — Hello World
==========================
The smallest possible DAG: one task, run manually.

Core concepts introduced here:

- A DAG (Directed Acyclic Graph) is just a Python object describing a
  workflow: which tasks exist, and what order they run in. It does NOT run
  any code itself - it's a blueprint. The scheduler reads this file and
  decides when to actually execute the tasks inside it.

- A "task" is one unit of work. Here we use a PythonOperator, which wraps
  a normal Python function.

- `schedule_interval=None` means this DAG never runs on its own - you
  trigger it manually from the UI (the "play" button) or CLI
  (`airflow dags trigger study_01_hello_world`). Good for DAGs you're
  actively testing/studying, as opposed to `packet_capture_pipeline`
  which runs automatically every 5 minutes.

Try this:
  1. Open http://localhost:8081, find `study_01_hello_world`.
  2. Toggle it on, click the play button to trigger a manual run.
  3. Click into the run, then click the task, then "Logs" - you'll see
     the print() output from `say_hello` right there.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def say_hello():
    print("Hello from Airflow! This function ran inside a PythonOperator.")


with DAG(
    dag_id="study_01_hello_world",
    description="STUDY: the smallest possible DAG - one task, manually triggered",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # manual trigger only, no automatic schedule
    catchup=False,
    tags=["study"],
) as dag:

    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )

    # A single-task DAG has no dependencies to wire up - there's just one
    # box in the graph.
