"""
STUDY DAG 4 — Parallel Tasks (Fan-out / Fan-in)
==================================================
One task splits into three independent parallel tasks, then all three
must finish before a final task runs.

Core concepts introduced here:

- Putting tasks in a Python list inside a `>>` chain makes them run in
  PARALLEL, not sequentially: `start >> [task_a, task_b, task_c] >> end`
  means task_a/task_b/task_c all become runnable as soon as `start`
  finishes, and Airflow can run them at the same time (subject to how
  many parallel slots your executor/worker has available).

- `end` won't start until ALL THREE of task_a/task_b/task_c succeed -
  that's "fan-in".

- This is exactly the pattern you'd use if, say, you wanted to parse
  three different pcap files at once instead of one at a time - though
  the main `packet_capture_pipeline` DAG intentionally stays sequential
  since there's only ever one capture per run.

- Whether tasks ACTUALLY run at the same time (vs. one after another)
  depends on the executor. This project uses `LocalExecutor`, which can
  run multiple tasks in parallel on the same machine (unlike
  `SequentialExecutor`, which never does).

Try this:
  1. Trigger this DAG and open the Graph or Gantt view for the run.
  2. In the Gantt view especially, you should see fetch_page_1,
     fetch_page_2, and fetch_page_3 overlapping in time - that's
     parallel execution, not just parallel-looking code.
"""

import time
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def fetch_page(page_number):
    print(f"Fetching page {page_number}...")
    time.sleep(3)  # pretend this is a slow network call
    print(f"Done fetching page {page_number}.")


with DAG(
    dag_id="study_04_parallel_fanout",
    description="STUDY: fan-out to parallel tasks, then fan-in to a join task",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["study"],
) as dag:

    start = EmptyOperator(task_id="start")

    fetch_1 = PythonOperator(
        task_id="fetch_page_1", python_callable=fetch_page, op_kwargs={"page_number": 1}
    )
    fetch_2 = PythonOperator(
        task_id="fetch_page_2", python_callable=fetch_page, op_kwargs={"page_number": 2}
    )
    fetch_3 = PythonOperator(
        task_id="fetch_page_3", python_callable=fetch_page, op_kwargs={"page_number": 3}
    )

    combine = EmptyOperator(task_id="combine_results")

    start >> [fetch_1, fetch_2, fetch_3] >> combine
